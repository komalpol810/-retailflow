"""
Diagnostic: find customers where the RAW (un-normalized) city/state
values differ across their orders, but the normalized (trim+lower city,
trim+upper state) values are identical - i.e. spurious "changes" that
were only formatting inconsistencies, not real address changes.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

BRONZE_DIR = "lakehouse/bronze"

builder = (
    SparkSession.builder
    .appName("CheckNormalizationEffect")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Use BRONZE (pre-normalization) customers to see raw values
customers = spark.read.format("delta").load(f"{BRONZE_DIR}/customers")

df = customers.select(
    "customer_unique_id", "customer_zip_code_prefix",
    "customer_city", "customer_state",
).withColumn(
    "norm_city", F.trim(F.lower(F.col("customer_city")))
).withColumn(
    "norm_state", F.trim(F.upper(F.col("customer_state")))
)

# Raw distinct address versions vs normalized distinct address versions, per customer
raw_versions = df.select(
    "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
).distinct().groupBy("customer_unique_id").agg(F.count("*").alias("raw_version_count"))

norm_versions = df.select(
    "customer_unique_id", "customer_zip_code_prefix", "norm_city", "norm_state"
).distinct().groupBy("customer_unique_id").agg(F.count("*").alias("norm_version_count"))

compare = raw_versions.join(norm_versions, "customer_unique_id").filter(
    F.col("raw_version_count") > F.col("norm_version_count")
)

print("Customers where normalization collapsed a spurious 'change':")
compare.show(20, truncate=False)
print(f"Total such customers: {compare.count()}")

spark.stop()
