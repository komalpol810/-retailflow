from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

PATH = "scratch/dim_customer_demo"

builder = (
    SparkSession.builder.appName("scd2_merge_demo")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def hash_expr():
    return F.sha2(
        F.concat_ws(
            "||",
            F.col("customer_zip_code_prefix").cast("string"),
            F.col("customer_city"),
            F.col("customer_state"),
        ),
        256,
    )


# ---------- CHECK 0: is my hash formula the same as the real pipeline's? ----------
dt = DeltaTable.forPath(spark, PATH)
current = dt.toDF().filter("is_current = true")
mismatch = current.filter(hash_expr() != F.col("row_hash")).count()
print(f"\n[CHECK 0] current rows whose recomputed hash != stored hash: {mismatch}")
if mismatch != 0:
    print("Hash formula does not match. Stop here and send me this output.")
    spark.stop()
    raise SystemExit(1)


# ---------- the SCD2 incremental load ----------
def apply_batch(batch, label):
    print(f"\n===== {label} =====")
    dt = DeltaTable.forPath(spark, PATH)

    # 1) one row per customer in the batch (latest wins) - same lesson as milestone 2
    w_latest = Window.partitionBy("customer_unique_id").orderBy(F.col("change_ts").desc())
    batch = (
        batch.withColumn("rn", F.row_number().over(w_latest))
        .filter("rn = 1")
        .drop("rn")
        .withColumn("row_hash", hash_expr())
    )

    # 2) compare against the CURRENT row of each customer
    cur = dt.toDF().filter("is_current = true").select(
        "customer_unique_id", F.col("row_hash").alias("cur_hash")
    )
    joined = batch.join(cur, "customer_unique_id", "left")
    changes = joined.filter(
        F.col("cur_hash").isNull() | (F.col("cur_hash") != F.col("row_hash"))
    )
    n_changes = changes.count()
    n_new = changes.filter(F.col("cur_hash").isNull()).count()
    print(f"batch customers: {batch.count()} | need action: {n_changes} "
          f"(brand new: {n_new}, moved: {n_changes - n_new})")
    if n_changes == 0:
        print("Nothing to do - batch already applied (idempotent).")
        return

    # 3) build the staged source: each moved customer appears twice
    max_key = dt.toDF().agg(F.max("customer_key")).collect()[0][0]
    w_key = Window.orderBy("customer_unique_id")

    inserts = (
        changes.withColumn("merge_key", F.lit(None).cast("string"))      # NULL never matches -> INSERT
        .withColumn("new_key", (F.lit(max_key) + F.row_number().over(w_key)).cast("int"))
    )
    closes = (
        changes.filter(F.col("cur_hash").isNotNull())
        .withColumn("merge_key", F.col("customer_unique_id"))            # matches old row -> CLOSE it
        .withColumn("new_key", F.lit(None).cast("int"))
    )
    source = inserts.unionByName(closes).drop("cur_hash")

    # 4) one atomic MERGE
    (
        dt.alias("t")
        .merge(source.alias("s"),
               "t.customer_unique_id = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(set={"valid_to": "s.change_ts", "is_current": "false"})
        .whenNotMatchedInsert(values={
            "customer_key": "s.new_key",
            "customer_unique_id": "s.customer_unique_id",
            "customer_zip_code_prefix": "s.customer_zip_code_prefix",
            "customer_city": "s.customer_city",
            "customer_state": "s.customer_state",
            "valid_from": "s.change_ts",
            "valid_to": "CAST(NULL AS TIMESTAMP)",
            "is_current": "true",
            "row_hash": "s.row_hash",
        })
        .execute()
    )
    print("MERGE committed.")


# ---------- build a small test batch from real data ----------
base = DeltaTable.forPath(spark, PATH).toDF().filter("is_current = true")
two = base.orderBy("customer_unique_id").limit(2).collect()
moved, unchanged = two[0], two[1]
print(f"\nmoved customer     : {moved['customer_unique_id']} (was {moved['customer_city']}, {moved['customer_state']})")
print(f"unchanged customer : {unchanged['customer_unique_id']}")

rows = [
    # moved: new address
    (moved["customer_unique_id"], 1310, "sao paulo", "SP", "2018-12-01 00:00:00"),
    # unchanged: exactly the same address as now
    (unchanged["customer_unique_id"], unchanged["customer_zip_code_prefix"],
     unchanged["customer_city"], unchanged["customer_state"], "2018-12-01 00:00:00"),
    # brand new customer
    ("new_customer_demo_0001", 20040, "rio de janeiro", "RJ", "2018-12-01 00:00:00"),
]
cols = ["customer_unique_id", "customer_zip_code_prefix", "customer_city",
        "customer_state", "change_ts"]
batch = spark.createDataFrame(rows, cols).withColumn("change_ts", F.col("change_ts").cast("timestamp"))

before = DeltaTable.forPath(spark, PATH).toDF().count()
print(f"\nrows before: {before}")

apply_batch(batch, "RUN 1 (should change 2 customers)")
after1 = DeltaTable.forPath(spark, PATH).toDF().count()
print(f"rows after run 1: {after1}   (expected {before + 2})")

apply_batch(batch, "RUN 2 - SAME batch again (should do nothing)")
after2 = DeltaTable.forPath(spark, PATH).toDF().count()
print(f"rows after run 2: {after2}   (expected {after1})")

# ---------- show the result ----------
ids = [moved["customer_unique_id"], unchanged["customer_unique_id"], "new_customer_demo_0001"]
print("\n=== HISTORY OF THE THREE TEST CUSTOMERS ===")
(DeltaTable.forPath(spark, PATH).toDF()
    .filter(F.col("customer_unique_id").isin(ids))
    .select("customer_key", "customer_unique_id", "customer_city", "customer_state",
            "valid_from", "valid_to", "is_current")
    .orderBy("customer_unique_id", "valid_from")
    .show(truncate=False))

print("=== INTEGRITY: customers with != 1 current row (expected 0) ===")
bad = (DeltaTable.forPath(spark, PATH).toDF()
       .filter("is_current = true")
       .groupBy("customer_unique_id").count()
       .filter("count != 1").count())
print(bad)

print("\n=== DELTA HISTORY ===")
(DeltaTable.forPath(spark, PATH).history()
    .select("version", "operation", "operationMetrics")
    .show(truncate=False))

spark.stop()
