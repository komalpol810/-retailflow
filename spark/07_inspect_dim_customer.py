from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("inspect_dim_customer")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("lakehouse/gold/dim_customer")

print("\n=== SCHEMA ===")
df.printSchema()

print("\n=== ROW COUNT ===")
print(df.count())

print("\n=== SAMPLE: one customer with an address change ===")
multi = (
    df.groupBy("customer_unique_id").count().filter("count > 1").limit(1).collect()
)
cid = multi[0]["customer_unique_id"]
df.filter(df.customer_unique_id == cid).orderBy("valid_from").show(truncate=False)

spark.stop()
