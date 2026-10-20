from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SRC = "scratch/orders_cdf_demo"       # the plain copy we already made
PATH = "scratch/orders_cdf_demo_v2"   # new copy, with the change feed ON from the start

builder = (
    SparkSession.builder.appName("cdf_demo")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # every NEW delta table created in this session gets the change feed switched on
    .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---------- 1) create the table with the feed on (version 0) ----------
df0 = spark.read.format("delta").load(SRC)
df0.write.format("delta").mode("overwrite").save(PATH)

dt = DeltaTable.forPath(spark, PATH)
print("\n=== TABLE PROPERTIES (look for enableChangeDataFeed = true) ===")
spark.read.format("delta").load(PATH)  # make sure it loads
dt.detail().select("properties").show(truncate=False)

n0 = spark.read.format("delta").load(PATH).count()
print(f"rows at version 0: {n0}")

# ---------- 2) make three kinds of change, one commit each ----------
ids = [r["order_id"] for r in df0.orderBy("order_id").select("order_id").limit(4).collect()]
upd_ids, del_id = ids[:2], ids[2]

# version 1: UPDATE (2 rows)
dt.update(
    condition=F.col("order_id").isin(upd_ids),
    set={"order_status": F.lit("canceled")},
)

# version 2: DELETE (1 row)
dt.delete(F.col("order_id") == del_id)

# version 3: INSERT (1 row: a copy of an existing row with a new key)
new_row = df0.limit(1).withColumn("order_id", F.lit("cdf_demo_new_order"))
new_row.write.format("delta").mode("append").save(PATH)

print("\n=== TABLE HISTORY ===")
dt.history().select("version", "operation").orderBy("version").show(truncate=False)

# ---------- 3) read the change feed (from version 1: skip the initial load) ----------
changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .load(PATH)
)

print("=== CHANGE COUNTS BY TYPE ===")
changes.groupBy("_change_type").count().orderBy("_change_type").show()

print("=== THE CHANGES THEMSELVES ===")
(changes
    .select("order_id", "order_status", "_change_type", "_commit_version")
    .orderBy("_commit_version", "order_id", "_change_type")
    .show(truncate=False))

# ---------- 4) what a downstream job would actually use ----------
print("=== WHAT A DOWNSTREAM JOB APPLIES (no preimages) ===")
(changes
    .filter("_change_type != 'update_preimage'")
    .select("order_id", "order_status", "_change_type", "_commit_version")
    .orderBy("_commit_version", "order_id")
    .show(truncate=False))

spark.stop()
