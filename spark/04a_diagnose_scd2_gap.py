"""
Diagnostic: find customers where dim_customer SCD2 build might have
collapsed two real address versions into one, likely due to tied
order_purchase_timestamp values across different addresses.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

SILVER_DIR = "lakehouse/silver"

builder = (
    SparkSession.builder
    .appName("DiagnoseSCD2Gap")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

customers = spark.read.format("delta").load(f"{SILVER_DIR}/customers")
orders = spark.read.format("delta").load(f"{SILVER_DIR}/orders")

df = customers.join(
    orders.select("customer_id", "order_purchase_timestamp"),
    on="customer_id", how="inner",
).withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||",
        F.col("customer_zip_code_prefix").cast("string"),
        F.col("customer_city"),
        F.col("customer_state")), 256)
)

# Find customer_unique_ids with more than one distinct row_hash
# (genuine address changes) where two versions share the same
# order_purchase_timestamp (the tie condition that could cause collapse)
versions = df.select(
    "customer_unique_id", "row_hash", "order_purchase_timestamp"
).distinct()

dupe_ts = (
    versions.groupBy("customer_unique_id", "order_purchase_timestamp")
    .agg(F.countDistinct("row_hash").alias("distinct_hashes_at_same_ts"))
    .filter(F.col("distinct_hashes_at_same_ts") > 1)
)

print("Customers with 2+ different addresses at the exact same order_purchase_timestamp:")
dupe_ts.show(20, truncate=False)
print(f"Total such cases: {dupe_ts.count()}")

spark.stop()
