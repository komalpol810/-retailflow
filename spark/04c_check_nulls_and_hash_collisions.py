"""
Diagnostic: (1) check for NULLs in the address fields used to build
row_hash, since concat_ws silently drops NULLs rather than erroring,
which could cause two different addresses to hash the same.
(2) directly find customers where two DIFFERENT (zip, city, state)
combos produced the SAME row_hash.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

SILVER_DIR = "lakehouse/silver"

builder = (
    SparkSession.builder
    .appName("CheckNullsAndHashCollisions")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

customers = spark.read.format("delta").load(f"{SILVER_DIR}/customers")

print("Null counts in address fields:")
customers.select(
    F.sum(F.col("customer_zip_code_prefix").isNull().cast("int")).alias("null_zip"),
    F.sum(F.col("customer_city").isNull().cast("int")).alias("null_city"),
    F.sum(F.col("customer_state").isNull().cast("int")).alias("null_state"),
).show()

df = customers.select(
    "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
).distinct().withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||",
        F.col("customer_zip_code_prefix").cast("string"),
        F.col("customer_city"),
        F.col("customer_state")), 256)
)

# For each customer_unique_id, compare count of distinct full address tuples
# vs count of distinct row_hash values - a mismatch means a hash collision.
tuple_count = df.groupBy("customer_unique_id").agg(
    F.countDistinct("customer_zip_code_prefix", "customer_city", "customer_state").alias("distinct_tuples")
)
hash_count = df.groupBy("customer_unique_id").agg(
    F.countDistinct("row_hash").alias("distinct_hashes")
)

collisions = tuple_count.join(hash_count, "customer_unique_id").filter(
    F.col("distinct_tuples") > F.col("distinct_hashes")
)

print("Customers where two different addresses hashed to the SAME row_hash:")
collisions.show(20, truncate=False)
print(f"Total such customers: {collisions.count()}")

spark.stop()
