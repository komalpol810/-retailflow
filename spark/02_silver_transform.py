"""
Silver layer transformation.
Reads each Bronze Delta table, applies cleaning/typing/standardization,
quarantines invalid rows to a _rejects path (never drops them), and
writes clean rows to Silver as Delta (overwrite - Silver reflects current
best-known-clean state, unlike Bronze's append-only history).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

BRONZE_DIR = "lakehouse/bronze"
SILVER_DIR = "lakehouse/silver"
REJECTS_DIR = "lakehouse/silver/_rejects"


def get_spark():
    builder = (
        SparkSession.builder
        .appName("RetailFlowSilverTransform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_bronze(spark, table_name):
    return spark.read.format("delta").load(f"{BRONZE_DIR}/{table_name}")


def split_and_write(df: DataFrame, table_name: str, is_valid_col="_is_valid"):
    """
    Splits df into valid/invalid based on _is_valid boolean column,
    writes valid rows to Silver, invalid rows (with reason) to _rejects.
    Prints counts for both.
    """
    valid = df.filter(F.col(is_valid_col)).drop(is_valid_col, "_reject_reason")
    invalid = df.filter(~F.col(is_valid_col)).drop(is_valid_col)

    valid_count = valid.count()
    invalid_count = invalid.count()

    (
        valid.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{SILVER_DIR}/{table_name}")
    )

    if invalid_count > 0:
        (
            invalid.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{REJECTS_DIR}/{table_name}")
        )

    print(f"[silver] {table_name:<22} valid={valid_count:>8}  rejected={invalid_count:>6}")


# ------------------------------------------------------------------
# Per-table cleaning logic
# ------------------------------------------------------------------

def transform_customers(spark):
    df = read_bronze(spark, "customers")

    w = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    df = (
        df
        .withColumn("customer_city", F.trim(F.lower(F.col("customer_city"))))
        .withColumn("customer_state", F.trim(F.upper(F.col("customer_state"))))
    )

    df = df.withColumn(
        "_is_valid",
        F.col("customer_id").isNotNull() & F.col("customer_unique_id").isNotNull()
    ).withColumn(
        "_reject_reason",
        F.when(F.col("customer_id").isNull(), "null customer_id")
         .when(F.col("customer_unique_id").isNull(), "null customer_unique_id")
    )

    split_and_write(df, "customers")


def transform_orders(spark):
    df = read_bronze(spark, "orders")

    for ts_col in [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))

    valid_statuses = [
        "delivered", "shipped", "canceled", "unavailable",
        "invoiced", "processing", "created", "approved",
    ]

    df = df.withColumn(
        "_is_valid",
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("order_purchase_timestamp").isNotNull()
        & F.col("order_status").isin(valid_statuses)
    ).withColumn(
        "_reject_reason",
        F.when(F.col("order_id").isNull(), "null order_id")
         .when(F.col("customer_id").isNull(), "null customer_id")
         .when(F.col("order_purchase_timestamp").isNull(), "unparseable purchase timestamp")
         .when(~F.col("order_status").isin(valid_statuses), "unknown order_status")
    )

    split_and_write(df, "orders")


def transform_order_items(spark):
    df = read_bronze(spark, "order_items")
    df = df.withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))

    df = df.withColumn(
        "_is_valid",
        F.col("order_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.col("price") > 0)
        & (F.col("freight_value") >= 0)
    ).withColumn(
        "_reject_reason",
        F.when(F.col("order_id").isNull(), "null order_id")
         .when(F.col("product_id").isNull(), "null product_id")
         .when(F.col("price") <= 0, "non-positive price")
         .when(F.col("freight_value") < 0, "negative freight_value")
    )

    split_and_write(df, "order_items")


def transform_order_payments(spark):
    df = read_bronze(spark, "order_payments")

    df = df.withColumn(
        "_is_valid",
        F.col("order_id").isNotNull() & (F.col("payment_value") >= 0)
    ).withColumn(
        "_reject_reason",
        F.when(F.col("order_id").isNull(), "null order_id")
         .when(F.col("payment_value") < 0, "negative payment_value")
    )

    split_and_write(df, "order_payments")


def transform_order_reviews(spark):
    df = read_bronze(spark, "order_reviews")

    # Known Phase 1 finding: grain is (review_id, order_id), not review_id alone.
    w = Window.partitionBy("review_id", "order_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    for ts_col in ["review_creation_date", "review_answer_timestamp"]:
        df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))

    df = df.withColumn(
        "_is_valid",
        F.col("review_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("review_score").between(1, 5)
    ).withColumn(
        "_reject_reason",
        F.when(F.col("review_id").isNull(), "null review_id")
         .when(F.col("order_id").isNull(), "null order_id")
         .when(~F.col("review_score").between(1, 5), "review_score out of 1-5 range")
    )

    split_and_write(df, "order_reviews")


def transform_products(spark):
    df = read_bronze(spark, "products")
    translation = (
        read_bronze(spark, "category_translation")
        .select("product_category_name", "product_category_name_english")
    )

    df = df.join(
        translation,
        on="product_category_name",
        how="left",
    )

    df = df.withColumn(
        "_is_valid",
        F.col("product_id").isNotNull()
    ).withColumn(
        "_reject_reason",
        F.when(F.col("product_id").isNull(), "null product_id")
    )

    split_and_write(df, "products")


def transform_sellers(spark):
    df = read_bronze(spark, "sellers")

    w = Window.partitionBy("seller_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    df = df.withColumn(
        "_is_valid", F.col("seller_id").isNotNull()
    ).withColumn(
        "_reject_reason", F.when(F.col("seller_id").isNull(), "null seller_id")
    )

    split_and_write(df, "sellers")


def transform_geolocation(spark):
    df = read_bronze(spark, "geolocation")

    df = df.withColumn(
        "_is_valid",
        F.col("geolocation_zip_code_prefix").isNotNull()
        & F.col("geolocation_lat").isNotNull()
        & F.col("geolocation_lng").isNotNull()
    ).withColumn(
        "_reject_reason",
        F.when(F.col("geolocation_zip_code_prefix").isNull(), "null zip prefix")
         .when(F.col("geolocation_lat").isNull(), "null lat")
         .when(F.col("geolocation_lng").isNull(), "null lng")
    )

    split_and_write(df, "geolocation")


def transform_category_translation(spark):
    df = read_bronze(spark, "category_translation")

    df = df.withColumn(
        "_is_valid", F.col("product_category_name").isNotNull()
    ).withColumn(
        "_reject_reason",
        F.when(F.col("product_category_name").isNull(), "null category name")
    )

    split_and_write(df, "category_translation")


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Silver transformation...\n")

    transform_customers(spark)
    transform_orders(spark)
    transform_order_items(spark)
    transform_order_payments(spark)
    transform_order_reviews(spark)
    transform_products(spark)
    transform_sellers(spark)
    transform_geolocation(spark)
    transform_category_translation(spark)

    print("\nSilver transformation complete.")
    spark.stop()


if __name__ == "__main__":
    main()
