import shutil
from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession, functions as F, Window

SRC = "lakehouse/silver/orders"
DEMO = "scratch/orders_demo"

builder = (
    SparkSession.builder.appName("merge_demo")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Throwaway copy of Silver orders
shutil.rmtree(DEMO, ignore_errors=True)
shutil.copytree(SRC, DEMO)
target = DeltaTable.forPath(spark, DEMO)
base = spark.read.format("delta").load(DEMO)
base_count = base.count()
print(f"\n== 1. Starting rows = {base_count} ==")

# 2. Build "today's batch"
ids = [r.order_id for r in base.filter("order_status = 'delivered'").limit(3).collect()]
upd_a, upd_b, dup_id = ids
template = base.limit(1)

# two normal updates: existing orders whose status changed to 'canceled'
updates = (base.filter(F.col("order_id").isin([upd_a, upd_b]))
           .withColumn("order_status", F.lit("canceled"))
           .withColumn("_ingested_at", F.current_timestamp()))

# one order that appears TWICE in the batch (older 'shipped', newer 'canceled')
dup_old = (base.filter(F.col("order_id") == dup_id)
           .withColumn("order_status", F.lit("shipped"))
           .withColumn("_ingested_at", F.current_timestamp()))
dup_new = (base.filter(F.col("order_id") == dup_id)
           .withColumn("order_status", F.lit("canceled"))
           .withColumn("_ingested_at", F.current_timestamp() + F.expr("INTERVAL 1 MINUTE")))

# two brand-new orders
new1 = template.withColumn("order_id", F.lit("new_order_001")).withColumn("order_status", F.lit("created"))
new2 = template.withColumn("order_id", F.lit("new_order_002")).withColumn("order_status", F.lit("created"))

batch = updates.unionByName(dup_old).unionByName(dup_new).unionByName(new1).unionByName(new2)
print(f"\n== 2. Batch has {batch.count()} rows: 2 updates, 1 order twice, 2 new orders ==")

# 3. Merge with the duplicate still in the batch -> should FAIL
print("\n== 3. MERGE with the duplicate still in the batch ==")
try:
    (target.alias("t")
        .merge(batch.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print("Unexpected: merge succeeded")
except Exception as e:
    print("MERGE FAILED as expected:", str(e).split("\n")[0][:200])
print("Rows after failed merge =", spark.read.format("delta").load(DEMO).count(), "(unchanged: nothing was committed)")

# 4. Keep only the latest row per order_id, then merge
w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
clean_batch = batch.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
print(f"\n== 4. After de-duplicating, batch has {clean_batch.count()} rows ==")

(target.alias("t")
    .merge(clean_batch.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# 5. Check the result
after = spark.read.format("delta").load(DEMO)
print(f"\n== 5. Rows after MERGE = {after.count()} (expected {base_count + 2}) ==")
print("Statuses of the touched orders:")
after.filter(F.col("order_id").isin([upd_a, upd_b, dup_id, "new_order_001", "new_order_002"])) \
     .select("order_id", "order_status").show(truncate=False)

print("== 6. What the MERGE commit recorded ==")
m = target.history(1).collect()[0]
print("operation =", m["operation"])
for k in ["numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted"]:
    print(f"{k} = {m['operationMetrics'].get(k)}")

spark.stop()
