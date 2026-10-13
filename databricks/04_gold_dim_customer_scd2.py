# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

storage_account = "retailflowstorage"
storage_key = dbutils.secrets.get(scope="retailflow", key="storage-key")
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)

SILVER_DIR = f"abfss://silver@{storage_account}.dfs.core.windows.net"
GOLD_DIR = f"abfss://gold@{storage_account}.dfs.core.windows.net"

# COMMAND ----------

def build_dim_customer():
    print("Building dim_customer (SCD Type 2)...\n")

    customers = spark.read.format("delta").load(f"{SILVER_DIR}/customers")
    orders = spark.read.format("delta").load(f"{SILVER_DIR}/orders")

    # Each customer_id (order-level) maps to one address snapshot.
    # Join to orders to get the purchase timestamp that address was used at.
    cust_with_order_date = (
        customers
        .join(
            orders.select("customer_id", "order_purchase_timestamp"),
            on="customer_id",
            how="inner",
        )
    )

    # row_hash: fingerprint of the address fields.
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

    # Detect version changes chronologically (gaps and islands):
    # lag(row_hash) + running sum of change flags, ordered by time.
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

    # One row per (customer_unique_id, row_hash, version_num), keeping the
    # earliest order date of that segment as the version start.
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

    # valid_from = this version's start; valid_to = next version's start
    # (NULL for the current version).
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

# COMMAND ----------

build_dim_customer()

# COMMAND ----------

dim = spark.read.format("delta").load(f"{GOLD_DIR}/dim_customer")

total = dim.count()
current = dim.filter(F.col("is_current")).count()
historical = total - current
changed = dim.groupBy("customer_unique_id").count().filter(F.col("count") > 1).count()

checks = [
    ("total rows",         total,      96355),
    ("current rows",       current,    96096),
    ("historical rows",    historical, 259),
    ("customers changed",  changed,    252),
]

all_ok = True
for name, actual, exp in checks:
    status = "OK" if actual == exp else "MISMATCH"
    if actual != exp:
        all_ok = False
    print(f"{name:<20} expected {exp:>7,}  actual {actual:>7,}  {status}")

print("\nSCD2 matches Phase 2 and Phase 3." if all_ok else "\nCheck the mismatches above.")