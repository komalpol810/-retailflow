# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window

storage_account = "retailflowstorage"
storage_key = dbutils.secrets.get(scope="retailflow", key="storage-key")
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)

SILVER = f"abfss://silver@{storage_account}.dfs.core.windows.net"
GOLD = f"abfss://gold@{storage_account}.dfs.core.windows.net"

# COMMAND ----------

def build_fct_order_items():
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

    # --- Step 2: order + customer context ---
    base = (
        payment_alloc
        .join(orders.select("order_id", "customer_id", "order_purchase_timestamp", "order_status"), on="order_id", how="inner")
        .join(customers.select("customer_id", "customer_unique_id", "customer_zip_code_prefix"), on="customer_id", how="inner")
    )

    # --- Step 3: SCD2 point-in-time join to dim_customer ---
    dim_cust = dim_customer.select(
        "customer_key",
        F.col("customer_unique_id").alias("dim_customer_unique_id"),
        "valid_from",
        "valid_to",
    )

    base_with_customer = base.join(
        dim_cust,
        on=(
            (base["customer_unique_id"] == dim_cust["dim_customer_unique_id"])
            & (base["order_purchase_timestamp"] >= dim_cust["valid_from"])
            & (
                (base["order_purchase_timestamp"] < dim_cust["valid_to"])
                | dim_cust["valid_to"].isNull()
            )
        ),
        how="inner",
    ).drop("dim_customer_unique_id")

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

    # --- Step 5: final columns + surrogate key ---
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
        .option("overwriteSchema", "true")
        .save(f"{GOLD}/fct_order_items")
    )
    print("Write complete.")
    

# COMMAND ----------

build_fct_order_items()

# COMMAND ----------

fct = spark.read.format("delta").load(f"{GOLD}/fct_order_items")

total_rows = fct.count()
distinct_keys = fct.select("order_id", "order_item_id").distinct().count()

checks = [
    ("total_rows",                total_rows,                                                    112650),
    ("duplicate (order,item)",    total_rows - distinct_keys,                                    0),
    ("missing_customer_key",      fct.filter(F.col("customer_key").isNull()).count(),            0),
    ("missing_product_key",       fct.filter(F.col("product_key").isNull()).count(),             0),
    ("missing_seller_key",        fct.filter(F.col("seller_key").isNull()).count(),              0),
    ("missing_date_key",          fct.filter(F.col("order_date_key").isNull()).count(),          0),
    ("missing_geography_key",     fct.filter(F.col("customer_geography_key").isNull()).count(),  302),
    ("missing_order_status_key",  fct.filter(F.col("order_status_key").isNull()).count(),        0),
]

all_ok = True
for name, actual, exp in checks:
    status = "OK" if actual == exp else "MISMATCH"
    if actual != exp:
        all_ok = False
    print(f"{name:<26} expected {exp:>8,}  actual {actual:>8,}  {status}")

print("\nfct_order_items matches Phase 2 and Phase 3." if all_ok else "\nCheck the mismatches above.")