from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

SRC = "scratch/dim_customer_demo"
NAIVE = "scratch/dim_customer_late_naive"
FIXED = "scratch/dim_customer_late_fixed"

builder = (
    SparkSession.builder.appName("late_arriving_demo")
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


def health(path, label):
    """Count history problems across the whole table."""
    df = DeltaTable.forPath(spark, path).toDF()
    w = Window.partitionBy("customer_unique_id").orderBy("valid_from")
    x = df.withColumn("prev_to", F.lag("valid_to").over(w)).withColumn("rn", F.row_number().over(w))
    inverted = df.filter(F.col("valid_to").isNotNull() & (F.col("valid_to") < F.col("valid_from"))).count()
    bad_link = x.filter(
        (F.col("rn") > 1) & (F.col("prev_to").isNull() | (F.col("prev_to") != F.col("valid_from")))
    ).count()
    not_one_current = (
        df.filter("is_current = true").groupBy("customer_unique_id").count().filter("count != 1").count()
    )
    print(f"[HEALTH {label}] valid_to before valid_from: {inverted} | "
          f"rows not joined to previous version: {bad_link} | customers with != 1 current row: {not_one_current}")


def show_history(path, cust_id, label):
    print(f"\n--- history of the test customer: {label} ---")
    (DeltaTable.forPath(spark, path).toDF()
        .filter(F.col("customer_unique_id") == cust_id)
        .select("customer_key", "customer_city", "customer_state", "valid_from", "valid_to", "is_current")
        .orderBy("valid_from")
        .show(truncate=False))


# ---------- the OLD logic (milestone 3): compare with the CURRENT row only ----------
def apply_naive(path, batch, label):
    print(f"\n===== {label} =====")
    dt = DeltaTable.forPath(spark, path)
    batch = batch.withColumn("row_hash", hash_expr())
    cur = dt.toDF().filter("is_current = true").select(
        "customer_unique_id", F.col("row_hash").alias("cur_hash"))
    changes = batch.join(cur, "customer_unique_id", "left").filter(
        F.col("cur_hash").isNull() | (F.col("cur_hash") != F.col("row_hash")))
    n = changes.count()
    print(f"need action: {n}")
    if n == 0:
        print("Nothing to do.")
        return
    max_key = dt.toDF().agg(F.max("customer_key")).collect()[0][0]
    w_key = Window.orderBy("customer_unique_id")
    inserts = (changes.withColumn("merge_key", F.lit(None).cast("string"))
               .withColumn("new_key", (F.lit(max_key) + F.row_number().over(w_key)).cast("int")))
    closes = (changes.filter(F.col("cur_hash").isNotNull())
              .withColumn("merge_key", F.col("customer_unique_id"))
              .withColumn("new_key", F.lit(None).cast("int")))
    source = inserts.unionByName(closes).drop("cur_hash")
    (dt.alias("t")
        .merge(source.alias("s"), "t.customer_unique_id = s.merge_key AND t.is_current = true")
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
        .execute())
    print("MERGE committed.")


# ---------- the NEW logic: find the version that was valid at change_ts and split it ----------
def apply_late_safe(path, batch, label):
    print(f"\n===== {label} =====")
    dt = DeltaTable.forPath(spark, path)
    tbl = dt.toDF()

    w_latest = Window.partitionBy("customer_unique_id").orderBy(F.col("change_ts").desc())
    batch = (batch.withColumn("rn", F.row_number().over(w_latest))
             .filter("rn = 1").drop("rn").withColumn("row_hash", hash_expr()))

    cover = tbl.select(
        F.col("customer_unique_id").alias("c_id"),
        F.col("customer_key").alias("cov_key"),
        F.col("row_hash").alias("cov_hash"),
        F.col("valid_from").alias("cov_from"),
        F.col("valid_to").alias("cov_to"),
        F.col("is_current").alias("cov_is_current"),
    )
    known = (tbl.select(F.col("customer_unique_id").alias("k_id")).distinct()
             .withColumn("is_known", F.lit(True)))

    # which existing version was valid at the moment of this change?
    joined = (
        batch.join(
            cover,
            (batch.customer_unique_id == cover.c_id)
            & (cover.cov_from <= batch.change_ts)
            & (cover.cov_to.isNull() | (batch.change_ts < cover.cov_to)),
            "left",
        )
        .join(known, batch.customer_unique_id == known.k_id, "left")
    )

    new_customer = F.col("cov_key").isNull() & F.col("is_known").isNull()
    before_history = F.col("cov_key").isNull() & F.col("is_known").isNotNull()
    needs_split = F.col("cov_key").isNotNull() & (F.col("cov_hash") != F.col("row_hash"))

    skipped = joined.filter(before_history).count()
    changes = (
        joined.filter(new_customer | needs_split)
        .withColumn("ins_valid_to", F.col("cov_to"))                                  # new piece ends where the old one ended
        .withColumn("ins_is_current", F.coalesce(F.col("cov_is_current"), F.lit(True)))  # ...and inherits "current" if it was
    )
    n = changes.count()
    print(f"need action: {n} | skipped (dated before the customer's first version): {skipped}")
    if n == 0:
        print("Nothing to do - already applied (idempotent).")
        return

    max_key = tbl.agg(F.max("customer_key")).collect()[0][0]
    w_key = Window.orderBy("customer_unique_id")
    inserts = (changes.withColumn("merge_key", F.lit(None).cast("int"))
               .withColumn("new_key", (F.lit(max_key) + F.row_number().over(w_key)).cast("int")))
    closes = (changes.filter(F.col("cov_key").isNotNull())
              .withColumn("merge_key", F.col("cov_key").cast("int"))
              .withColumn("new_key", F.lit(None).cast("int")))
    source = inserts.unionByName(closes)

    (dt.alias("t")
        .merge(source.alias("s"), "t.customer_key = s.merge_key")
        .whenMatchedUpdate(set={"valid_to": "s.change_ts", "is_current": "false"})
        .whenNotMatchedInsert(values={
            "customer_key": "s.new_key",
            "customer_unique_id": "s.customer_unique_id",
            "customer_zip_code_prefix": "s.customer_zip_code_prefix",
            "customer_city": "s.customer_city",
            "customer_state": "s.customer_state",
            "valid_from": "s.change_ts",
            "valid_to": "s.ins_valid_to",
            "is_current": "s.ins_is_current",
            "row_hash": "s.row_hash",
        })
        .execute())
    print("MERGE committed.")


# ---------- set up two identical scratch copies ----------
src = spark.read.format("delta").load(SRC)
src.write.format("delta").mode("overwrite").save(NAIVE)
src.write.format("delta").mode("overwrite").save(FIXED)
print(f"copied {src.count()} rows into two scratch tables")
health(FIXED, "baseline before any late data")

# ---------- pick a customer with exactly 2 versions, build a LATE change ----------
base = DeltaTable.forPath(spark, SRC).toDF()
victim_id = (base.groupBy("customer_unique_id").count().filter("count = 2")
             .orderBy("customer_unique_id").limit(1).collect()[0]["customer_unique_id"])
v = (base.filter(F.col("customer_unique_id") == victim_id).orderBy("valid_from").collect())
old_row, cur_row = v[0], v[1]
assert old_row["valid_to"] is not None and cur_row["is_current"], "unexpected shape for victim"
mid = old_row["valid_from"] + (old_row["valid_to"] - old_row["valid_from"]) / 2
late_ts = mid.strftime("%Y-%m-%d %H:%M:%S")
print(f"\ntest customer: {victim_id}")
print(f"old version : {old_row['customer_city']}, {old_row['customer_state']}  {old_row['valid_from']} -> {old_row['valid_to']}")
print(f"current ver : {cur_row['customer_city']}, {cur_row['customer_state']}  from {cur_row['valid_from']}")
print(f"LATE change dated {late_ts}  (older than the current version's start)")

cols = ["customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state", "change_ts"]
late_batch = spark.createDataFrame(
    [(victim_id, 40020, "salvador", "BA", late_ts)], cols
).withColumn("change_ts", F.col("change_ts").cast("timestamp"))

# ---------- A) the old logic ----------
show_history(NAIVE, victim_id, "BEFORE (naive copy)")
apply_naive(NAIVE, late_batch, "NAIVE logic")
show_history(NAIVE, victim_id, "AFTER naive logic  <-- look for the problems")
health(NAIVE, "naive")

# ---------- B) the fixed logic ----------
show_history(FIXED, victim_id, "BEFORE (fixed copy)")
apply_late_safe(FIXED, late_batch, "FIXED logic, run 1")
show_history(FIXED, victim_id, "AFTER fixed logic")
health(FIXED, "fixed, run 1")

apply_late_safe(FIXED, late_batch, "FIXED logic, run 2 - SAME batch again")
health(FIXED, "fixed, run 2")
rows_now = DeltaTable.forPath(spark, FIXED).toDF().count()
print(f"rows in fixed table: {rows_now}")

spark.stop()
