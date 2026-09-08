from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("RetailFlowSmokeTest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.createDataFrame(
    [(1, "test_row_ok")],
    ["id", "message"]
)
df.show()

df.write.format("delta").mode("overwrite").save("/tmp/delta_smoke_test")
readback = spark.read.format("delta").load("/tmp/delta_smoke_test")
readback.show()

print("DELTA WRITE + READ SUCCESSFUL")
spark.stop()
