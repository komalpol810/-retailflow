# Databricks notebook source
storage_account = "retailflowstorage"
storage_key = dbutils.secrets.get(scope="retailflow", key="storage-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

RAW_DIR = f"abfss://raw@{storage_account}.dfs.core.windows.net"
BRONZE_DIR = f"abfss://bronze@{storage_account}.dfs.core.windows.net"


# COMMAND ----------

from pyspark.sql import functions as F

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


def ingest_table(table_name, source_filename):
    csv_path = f"{RAW_DIR}/{source_filename}"
    delta_path = f"{BRONZE_DIR}/{table_name}"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .csv(csv_path)
    )

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


print("Starting Bronze ingestion...\n")
for table_name, source_filename in SOURCE_FILES.items():
    ingest_table(table_name, source_filename)
print("\nBronze ingestion complete.")

# COMMAND ----------

for t in SOURCE_FILES:
    n = spark.read.format("delta").load(f"{BRONZE_DIR}/{t}").count()
    print(f"{t:<22} {n:>10}")
    