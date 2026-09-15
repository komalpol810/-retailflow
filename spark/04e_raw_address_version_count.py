"""
Diagnostic: directly count distinct (customer_unique_id, zip, city, state)
combinations in silver.customers - no join to orders, no window functions,
no hashing. This isolates the raw "how many address versions actually
exist" number so we can compare it directly against Phase 2's 259
historical-version count and our Spark build's 256, without any of the
timestamp/ordering logic in between.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

SILVER_DIR = "lakehouse/silver"

builder = (
    SparkSession.builder
    .appName("RawAddressVersionCount")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

customers = spark.read.format("delta").load(f"{SILVER_DIR}/customers")

addr_versions = customers.select(
    "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
).distinct()

total_versions = addr_versions.count()
distinct_customers = addr_versions.select("customer_unique_id").distinct().count()

print(f"Total distinct (customer_unique_id + address) rows: {total_versions}")
print(f"Total distinct customer_unique_id: {distinct_customers}")
print(f"Customers with more than one address version: {total_versions - distinct_customers}")

# Show the per-customer version counts, for customers with 2+ versions
version_counts = (
    addr_versions.groupBy("customer_unique_id")
    .agg(F.count("*").alias("version_count"))
    .filter(F.col("version_count") > 1)
)
print("\nCustomers with multiple address versions (count of such customers):")
print(version_counts.count())
version_counts.orderBy(F.col("version_count").desc()).show(20, truncate=False)

spark.stop()
