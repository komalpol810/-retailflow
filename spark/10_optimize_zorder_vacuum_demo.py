import glob
import shutil
import os
from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SRC = "lakehouse/gold/fct_order_items"
PATH = "scratch/fct_files_demo"

builder = (
    SparkSession.builder.appName("optimize_demo")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # allow VACUUM with 0 hours - FOR THIS DEMO ONLY (default safety check is 7 days)
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    # small max file size so OPTIMIZE produces several files and ZORDER has something to show
    .config("spark.databricks.delta.optimize.maxFileSize", "300000")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def live_files():
    d = DeltaTable.forPath(spark, PATH).detail().collect()[0]
    return d["numFiles"], d["sizeInBytes"]


def disk_files():
    return len(glob.glob(f"{PATH}/*.parquet"))


def report(label):
    n, size = live_files()
    print(f"[{label}] files in the current table: {n} | parquet files on disk: {disk_files()} | size: {size/1e6:.1f} MB")


def skipping(label, key):
    df = spark.read.format("delta").load(PATH).select(
        "customer_key", F.col("_metadata.file_path").alias("f"))
    per = df.groupBy("f").agg(F.min("customer_key").alias("lo"), F.max("customer_key").alias("hi"))
    total = per.count()
    hit = per.filter((F.col("lo") <= key) & (F.col("hi") >= key)).count()
    print(f"[{label}] a lookup of customer_key={key} would have to open {hit} of {total} files")


# ---------- setup: a fresh table made of 100 tiny files ----------
if os.path.exists(PATH):
    shutil.rmtree(PATH)
src = spark.read.format("delta").load(SRC)
src.repartition(100).write.format("delta").mode("overwrite").save(PATH)
rows = spark.read.format("delta").load(PATH).count()
print(f"\nrows: {rows} (expected 112650)")
report("after writing in 100 pieces")

key = int(spark.read.format("delta").load(PATH).approxQuantile("customer_key", [0.5], 0.01)[0])
skipping("before OPTIMIZE", key)

# ---------- 1) OPTIMIZE (compaction only) ----------
print("\n===== OPTIMIZE (compaction) =====")
DeltaTable.forPath(spark, PATH).optimize().executeCompaction()
report("after OPTIMIZE")
print("^ note: 'on disk' did NOT go down - the old small files are still there")
skipping("after OPTIMIZE", key)

# ---------- 2) OPTIMIZE ... ZORDER BY customer_key ----------
print("\n===== OPTIMIZE ZORDER BY customer_key =====")
DeltaTable.forPath(spark, PATH).optimize().executeZOrderBy("customer_key")
report("after ZORDER")
skipping("after ZORDER", key)

# ---------- 3) VACUUM ----------
print("\n===== VACUUM =====")
try:
    print("dry run - files VACUUM would delete (first rows):")
    spark.sql(f"VACUUM delta.`{PATH}` RETAIN 0 HOURS DRY RUN").show(3, truncate=False)
except Exception as e:
    print("dry run skipped:", str(e)[:150])

DeltaTable.forPath(spark, PATH).vacuum(0.0)
report("after VACUUM")

# ---------- 4) time travel after VACUUM ----------
print("\n===== TIME TRAVEL to version 0 after VACUUM =====")
try:
    n = spark.read.format("delta").option("versionAsOf", 0).load(PATH).count()
    print("version 0 still readable, rows:", n)
except Exception as e:
    print("version 0 can NO LONGER be read ->", type(e).__name__)

print(f"\ncurrent table still fine, rows: {spark.read.format('delta').load(PATH).count()}")

print("\n=== DELTA HISTORY ===")
(DeltaTable.forPath(spark, PATH).history()
    .select("version", "operation", "operationMetrics")
    .show(truncate=False))

spark.stop()
