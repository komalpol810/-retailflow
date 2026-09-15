"""
Bronze layer ingestion.
Reads all 9 raw Olist CSVs as-is (schema-on-read, everything inferred/kept
loose), adds lineage columns, and writes each as an append-only Delta table
partitioned by ingestion date. Nothing is cleaned, deduped, or typed here —
that's Silver's job. Bronze exists to preserve original fidelity.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

DATA_DIR = "data"
BRONZE_DIR = "lakehouse/bronze"

# Maps output Delta table name -> source CSV filename
SOURCE_FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def get_spark():
    builder = (
        SparkSession.builder
        .appName("RetailFlowBronzeIngest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ingest_table(spark, table_name, source_filename):
    csv_path = f"{DATA_DIR}/{source_filename}"
    delta_path = f"{BRONZE_DIR}/{table_name}"

    # Schema-on-read: let Spark infer types loosely from the CSV itself.
    # This mirrors Phase 1's "everything as TEXT" philosophy in spirit —
    # we are not imposing a target schema here, just reading what's there.
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .csv(csv_path)
    )

    # Lineage columns — same idea as _ingested_at / _source_file in Phase 1
    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_filename))
        .withColumn("_ingestion_date", F.current_date())
    )

    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingestion_date")
        .save(delta_path)
    )

    print(f"[bronze] {table_name:<22} {row_count:>8} rows -> {delta_path}")


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Bronze ingestion...\n")
    for table_name, source_filename in SOURCE_FILES.items():
        ingest_table(spark, table_name, source_filename)

    print("\nBronze ingestion complete.")
    spark.stop()


if __name__ == "__main__":
    main()
