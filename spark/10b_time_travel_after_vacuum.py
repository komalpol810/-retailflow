from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PATH = "scratch/fct_files_demo"

builder = (
    SparkSession.builder.appName("time_travel_check")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("OFF")

for label, reader in [
    ("CURRENT version", spark.read.format("delta").load(PATH)),
    ("VERSION 0 (before OPTIMIZE and VACUUM)", spark.read.format("delta").option("versionAsOf", 0).load(PATH)),
]:
    try:
        total = reader.agg(F.sum("customer_key")).collect()[0][0]
        print(f"{label}: read OK, sum of customer_key = {total}")
    except Exception as e:
        print(f"{label}: FAILED to read data -> {type(e).__name__}")

spark.stop()
