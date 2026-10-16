import os, glob, json, shutil
from collections import Counter
from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

SRC = "lakehouse/gold/fct_order_items"
DEMO = "scratch/fct_demo"

builder = (
    SparkSession.builder.appName("delta_internals")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Work on a throwaway copy of the table
shutil.rmtree(DEMO, ignore_errors=True)
shutil.copytree(SRC, DEMO)

print("\n== 1. What is inside a Delta table folder ==")
print("Data files :", [f for f in sorted(os.listdir(DEMO)) if f.endswith(".parquet")][:3], "...")
print("Log files  :", sorted(os.listdir(DEMO + "/_delta_log")))

dt = DeltaTable.forPath(spark, DEMO)
base_version = dt.history(1).collect()[0]["version"]
base_count = spark.read.format("delta").load(DEMO).count()
print(f"\nStarting version = {base_version}, rows = {base_count}")

# 2. Make two changes so the log has something to show
print("\n== 2. Making two changes (an UPDATE and a DELETE) ==")
dt.update(condition="order_item_id = 1", set={"price": "price * 1.10"})
dt.delete("freight_value = 0")
now_count = spark.read.format("delta").load(DEMO).count()
print(f"Rows after the changes = {now_count}")

print("\n== 3. Table history (one row per commit) ==")
dt.history().select("version", "timestamp", "operation").show(truncate=False)

# 4. Read the receipt of the latest commit
print("== 4. Latest commit file: what did it add and remove? ==")
last_log = sorted(glob.glob(DEMO + "/_delta_log/*.json"))[-1]
print("File:", os.path.basename(last_log))
with open(last_log) as f:
    kinds = Counter(list(json.loads(line).keys())[0] for line in f)
print(dict(kinds))
print("add = new data files written, remove = old files logically deleted")

# 5. Time travel
print("\n== 5. Time travel ==")
old_count = spark.read.format("delta").option("versionAsOf", base_version).load(DEMO).count()
print(f"Rows at version {base_version} (before changes) = {old_count}")
print(f"Rows now                                       = {now_count}")

# 6. Undo everything with RESTORE
dt.restoreToVersion(base_version)
restored = spark.read.format("delta").load(DEMO).count()
print(f"\n== 6. After RESTORE to version {base_version}: rows = {restored} (should equal {base_count}) ==")
dt.history().select("version", "operation").show(truncate=False)

spark.stop()
