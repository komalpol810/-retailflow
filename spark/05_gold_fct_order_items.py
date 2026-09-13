"""
Phase 3 - Gold layer: fct_order_items
Mirrors the logic of sql/gold/09_fct_order_items_ddl.sql + 10_fct_order_items_load.sql,
but sources from the Silver Delta layer instead of Postgres staging, and joins
against the Gold dimension Delta tables already built in this phase.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta import configure_spark_with_delta_pip

LAKEHOUSE_PATH = "lakehouse"
SILVER = f"{LAKEHOUSE_PATH}/silver"
GOLD = f"{LAKEHOUSE_PATH}/gold"

builder = (
    SparkSession.builder.appName("RetailFlow-Gold-FctOrderItems")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Reading Silver tables...")
order_items = spark.read.format("delta").load(f"{SILVER}/order_items")
orders = spark.read.format("delta").load(f"{SILVER}/orders")
customers = spark.read.format("delta").load(f"{SILVER}/customers")
order_payments = spark.read.format("delta").load(f"{SILVER}/order_payments")

print("Reading Gold dimension tables...")
dim_customer = spark.read.format("delta").load(f"{GOLD}/dim_customer")
dim_product = spark.read.format("delta").load(f"{GOLD}/dim_product")
dim_seller = spark.read.format("delta").load(f"{GOLD}/dim_seller")
dim_date = spark.read.format("delta").load(f"{GOLD}/dim_date")
dim_geography = spark.read.format("delta").load(f"{GOLD}/dim_geography")
dim_order_status = spark.read.format("delta").load(f"{GOLD}/dim_order_status")

# --- Step 1: payment allocation ---
# SQL equivalent: SUM(price) OVER (PARTITION BY order_id), then allocate
# each order's total payment proportionally by each item's share of price.
order_payment_totals = (
    order_payments.groupBy("order_id")
    .agg(F.sum("payment_value").alias("total_payment"))
)

order_price_window = Window.partitionBy("order_id")

payment_alloc = (
    order_items
    .withColumn("order_total_price", F.sum("price").over(order_price_window))
    .join(order_payment_totals, on="order_id", how="left")
    .withColumn(
        "payment_value_allocated",
        F.round(
            (F.col("price") / F.when(F.col("order_total_price") == 0, None).otherwise(F.col("order_total_price")))
            * F.coalesce(F.col("total_payment"), F.lit(0.0)),
            2,
        ),
    )
    .select(
        "order_id",
        "order_item_id",
        "price",
        "freight_value",
        "product_id",
        "seller_id",
        "payment_value_allocated",
    )
)

# --- Step 2: bring in order + customer context ---
# INNER JOIN to orders and customers mirrors the SQL (every order_item must
# belong to a real order and customer for the fact row to make sense).
base = (
    payment_alloc
    .join(orders.select("order_id", "customer_id", "order_purchase_timestamp", "order_status"), on="order_id", how="inner")
    .join(customers.select("customer_id", "customer_unique_id", "customer_zip_code_prefix"), on="customer_id", how="inner")
)

# --- Step 3: SCD2 point-in-time join to dim_customer ---
# This is the part with no direct PySpark equivalent to a simple join key -
# it's a non-equi join (range condition), same as the SQL BETWEEN valid_from/valid_to.
base_with_customer = base.join(
    dim_customer.select(
        "customer_key", "customer_unique_id", "valid_from", "valid_to"
    ),
    on=(
        (base.customer_unique_id == dim_customer.customer_unique_id)
        & (base.order_purchase_timestamp >= dim_customer.valid_from)
        & (
            (base.order_purchase_timestamp < dim_customer.valid_to)
            | dim_customer.valid_to.isNull()
        )
    ),
    how="inner",
).drop(dim_customer.customer_unique_id)

# --- Step 4: LEFT JOIN remaining dimensions ---
fct = (
    base_with_customer
    .join(dim_product.select("product_key", "product_id"), on="product_id", how="left")
    .join(dim_seller.select("seller_key", "seller_id"), on="seller_id", how="left")
    .join(
        dim_date.select("date_key", "full_date"),
        F.col("order_purchase_timestamp").cast("date") == F.col("full_date"),
        how="left",
    )
    .join(
        dim_geography.select("geography_key", "zip_code_prefix"),
        F.col("customer_zip_code_prefix") == F.col("zip_code_prefix"),
        how="left",
    )
    .join(
        dim_order_status.select("order_status_key", "order_status"),
        on="order_status",
        how="left",
    )
)

# --- Step 5: select final columns + surrogate key ---
# No partitionBy on this row_number() - same single-partition-shuffle tradeoff
# flagged in 03_gold_dimensions.py; fine at this data volume, worth mentioning
# in interviews as a place you'd repartition or use monotonically_increasing_id()
# instead if this were a much larger dataset.
fct_final = (
    fct.select(
        "order_id",
        "order_item_id",
        "customer_key",
        "product_key",
        "seller_key",
        F.col("date_key").alias("order_date_key"),
        F.col("geography_key").alias("customer_geography_key"),
        "order_status_key",
        "price",
        "freight_value",
        "payment_value_allocated",
    )
    .withColumn("order_item_key", F.row_number().over(Window.orderBy("order_id", "order_item_id")))
    .withColumn("_loaded_at", F.current_timestamp())
)

# --- Step 6: write to Gold ---
print("Writing gold.fct_order_items...")
(
    fct_final.write.format("delta")
    .mode("overwrite")
    .save(f"{GOLD}/fct_order_items")
)

# --- Verification ---
total_rows = fct_final.count()
missing_product = fct_final.filter(F.col("product_key").isNull()).count()
missing_seller = fct_final.filter(F.col("seller_key").isNull()).count()
missing_date = fct_final.filter(F.col("order_date_key").isNull()).count()
missing_geo = fct_final.filter(F.col("customer_geography_key").isNull()).count()
missing_status = fct_final.filter(F.col("order_status_key").isNull()).count()

print(f"total_rows={total_rows}")
print(f"missing_product_key={missing_product}")
print(f"missing_seller_key={missing_seller}")
print(f"missing_date_key={missing_date}")
print(f"missing_geography_key={missing_geo}  (expect ~302, matches Phase 2's known zip-prefix gaps)")
print(f"missing_order_status_key={missing_status}")

spark.stop()
