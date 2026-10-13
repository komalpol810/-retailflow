# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

storage_account = "retailflowstorage"
storage_key = dbutils.secrets.get(scope="retailflow", key="storage-key")
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)

SILVER_DIR = f"abfss://silver@{storage_account}.dfs.core.windows.net"
GOLD_DIR = f"abfss://gold@{storage_account}.dfs.core.windows.net"


# COMMAND ----------

def read_silver(table_name):
    return spark.read.format("delta").load(f"{SILVER_DIR}/{table_name}")


def add_surrogate_key(df, key_name, order_col=None):
    order_by = F.col(order_col) if order_col else F.lit(1)
    w = Window.orderBy(order_by)
    return df.withColumn(key_name, F.row_number().over(w))


def write_gold(df, table_name):
    (
        df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD_DIR}/{table_name}")
    )
    count = df.count()
    print(f"[gold] {table_name:<20} {count:>8} rows")


def build_dim_date():
    start_date = "2016-01-01"
    end_date = "2019-12-31"

    date_range_df = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{start_date}'), to_date('{end_date}'), interval 1 day
        )) AS full_date
    """)

    df = (
        date_range_df
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("month_num", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
        .withColumn("is_holiday", F.lit(False))
    )

    df = add_surrogate_key(df, "date_key", order_col="full_date")
    df = df.select(
        "date_key", "full_date", "day_of_month", "day_of_week", "day_name",
        "week_of_year", "month_num", "month_name", "quarter", "year",
        "is_weekend", "is_holiday",
    )
    write_gold(df, "dim_date")


def build_dim_geography():
    geo = read_silver("geolocation")

    city_counts = (
        geo.groupBy("geolocation_zip_code_prefix", "geolocation_city")
        .agg(F.count("*").alias("cnt"))
    )
    w_city = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.col("cnt").desc())
    mode_city = (
        city_counts
        .withColumn("rn", F.row_number().over(w_city))
        .filter(F.col("rn") == 1)
        .select("geolocation_zip_code_prefix", "geolocation_city")
    )

    state_counts = (
        geo.groupBy("geolocation_zip_code_prefix", "geolocation_state")
        .agg(F.count("*").alias("cnt"))
    )
    w_state = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.col("cnt").desc())
    mode_state = (
        state_counts
        .withColumn("rn", F.row_number().over(w_state))
        .filter(F.col("rn") == 1)
        .select("geolocation_zip_code_prefix", "geolocation_state")
    )

    avg_coords = (
        geo.groupBy("geolocation_zip_code_prefix")
        .agg(
            F.avg("geolocation_lat").alias("avg_lat"),
            F.avg("geolocation_lng").alias("avg_lng"),
        )
    )

    df = (
        avg_coords
        .join(mode_city, "geolocation_zip_code_prefix", "left")
        .join(mode_state, "geolocation_zip_code_prefix", "left")
        .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix")
        .withColumnRenamed("geolocation_city", "city")
        .withColumnRenamed("geolocation_state", "state")
    )

    df = add_surrogate_key(df, "geography_key", order_col="zip_code_prefix")
    df = df.select("geography_key", "zip_code_prefix", "city", "state", "avg_lat", "avg_lng")
    write_gold(df, "dim_geography")


def build_dim_product():
    products = read_silver("products")

    df = products.select(
        "product_id",
        F.col("product_category_name_english").alias("product_category_name"),
        "product_weight_g", "product_length_cm",
        "product_height_cm", "product_width_cm",
    )
    df = add_surrogate_key(df, "product_key", order_col="product_id")
    df = df.select(
        "product_key", "product_id", "product_category_name",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    )
    write_gold(df, "dim_product")


def build_dim_seller():
    sellers = read_silver("sellers")

    df = sellers.select(
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
    )
    df = add_surrogate_key(df, "seller_key", order_col="seller_id")
    df = df.select("seller_key", "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    write_gold(df, "dim_seller")


def build_dim_order_status():
    orders = read_silver("orders")

    df = orders.select("order_status").distinct()
    df = add_surrogate_key(df, "order_status_key", order_col="order_status")
    df = df.select("order_status_key", "order_status")
    write_gold(df, "dim_order_status")

# COMMAND ----------

print("Building Gold dimensions (Type 1)...\n")

build_dim_date()
build_dim_geography()
build_dim_product()
build_dim_seller()
build_dim_order_status()

print("\nGold Type-1 dimensions complete.")

# COMMAND ----------

expected = {
    "dim_date": 1461,
    "dim_geography": 19015,
    "dim_product": 32951,
    "dim_seller": 3095,
    "dim_order_status": 8,
}

all_ok = True
for table, exp in expected.items():
    actual = spark.read.format("delta").load(f"{GOLD_DIR}/{table}").count()
    status = "OK" if actual == exp else "MISMATCH"
    if actual != exp:
        all_ok = False
    print(f"{table:<20} expected {exp:>7,}  actual {actual:>7,}  {status}")

print("\nAll Gold dimensions match." if all_ok else "\nCheck the mismatches above.")