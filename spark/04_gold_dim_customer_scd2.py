"""
Gold layer - dim_customer, SCD Type 2.
Keyed on customer_unique_id (not customer_id, which changes every
order in Olist). Detects genuine address changes and builds valid_from/
valid_to/is_current history from the order timeline - this is an
initial historical load, not an incremental merge (that pattern comes
later, in Phase 6, using Delta MERGE INTO).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

SILVER_DIR = "lakehouse/silver"
GOLD_DIR = "lakehouse/gold"


def get_spark():
    builder = (
        SparkSession.builder
        .appName("RetailFlowGoldDimCustomerSCD2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Building dim_customer (SCD Type 2)...\n")

    customers = spark.read.format("delta").load(f"{SILVER_DIR}/customers")
    orders = spark.read.format("delta").load(f"{SILVER_DIR}/orders")

    # Each customer_id (order-level) maps to exactly one address snapshot.
    # Join to orders to get the purchase timestamp that address was used at -
    # this reconstructs the timeline needed to open/close SCD2 records.
    cust_with_order_date = (
        customers
        .join(
            orders.select("customer_id", "order_purchase_timestamp"),
            on="customer_id",
            how="inner",
        )
    )

    # row_hash: fingerprints the address fields. If this hash changes for
    # the same customer_unique_id across orders, that's a genuine address
    # change - the trigger for opening a new SCD2 row.
    df = cust_with_order_date.withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("customer_zip_code_prefix").cast("string"),
                F.col("customer_city"),
                F.col("customer_state"),
            ),
            256,
        )
    )

    # --- FIX: detect version changes CHRONOLOGICALLY, not by distinct hash.
    # Partitioning by (customer_unique_id, row_hash) alone (the old approach)
    # collapses ALL orders sharing an address into one version - including a
    # customer who moves away and later returns to a PREVIOUS address
    # (A -> B -> A). That's 2 distinct hashes but 3 real chronological
    # versions. This "gaps and islands" pattern - lag(row_hash) + a running
    # sum of change-flags, ordered by time - correctly opens a new version
    # every time the hash differs from the immediately PRECEDING order,
    # even if that hash was seen before.
    w_chrono = Window.partitionBy("customer_unique_id").orderBy("order_purchase_timestamp")
    df = df.withColumn("prev_hash", F.lag("row_hash").over(w_chrono))
    df = df.withColumn(
        "is_new_version",
        F.when(
            F.col("prev_hash").isNull() | (F.col("prev_hash") != F.col("row_hash")),
            1,
        ).otherwise(0)
    )
    df = df.withColumn("version_num", F.sum("is_new_version").over(w_chrono))

    # Collapse to one row per (customer_unique_id, row_hash, version_num) -
    # i.e. one row per chronological version segment - keeping the earliest
    # order date that segment was seen at (that's when this version became
    # valid). Adding version_num here is what preserves the A->B->A case.
    w_version = Window.partitionBy("customer_unique_id", "row_hash", "version_num")
    versions = (
        df
        .withColumn("_version_start", F.min("order_purchase_timestamp").over(w_version))
        .select(
            "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state", "row_hash", "_version_start",
        )
        .distinct()
    )

    # Order each customer's address versions chronologically, then compute
    # valid_from (this version's start) and valid_to (next version's start,
    # or NULL if this is the current/latest version).
    w_order = Window.partitionBy("customer_unique_id").orderBy("_version_start")
    versions = (
        versions
        .withColumn("valid_from", F.col("_version_start"))
        .withColumn("valid_to", F.lead("_version_start").over(w_order))
        .withColumn("is_current", F.col("valid_to").isNull())
        .drop("_version_start")
    )

    versions = versions.withColumn(
        "customer_key",
        F.row_number().over(Window.orderBy("customer_unique_id", "valid_from"))
    )

    final = versions.select(
        "customer_key", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state", "valid_from", "valid_to",
        "is_current", "row_hash",
    )

    total_rows = final.count()
    current_rows = final.filter(F.col("is_current")).count()
    distinct_customers = final.select("customer_unique_id").distinct().count()

    # Fixed: a customer "had an address change" if they have MORE THAN ONE
    # version row, not the old (current_rows - historical_rows) formula,
    # which didn't actually measure that.
    version_counts = final.groupBy("customer_unique_id").count()
    customers_with_change = version_counts.filter(F.col("count") > 1).count()

    (
        final.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD_DIR}/dim_customer")
    )

    print(f"[gold] dim_customer total_rows={total_rows}  current_rows={current_rows}  distinct_customers={distinct_customers}")
    print(f"[gold] customers with an address change: {customers_with_change} "
          f"(total_rows - current_rows = {total_rows - current_rows} historical/closed versions)")

    print("\ndim_customer SCD2 build complete.")
    spark.stop()


if __name__ == "__main__":
    main()
