"""
Gold layer - simple dimensions (Type 1, no history tracking).
Rebuilds the Phase 2 star schema dims using PySpark DataFrame API
instead of SQL. Surrogate keys via monotonically_increasing_id()
mapped to dense row numbers for clean sequential integers.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

SILVER_DIR = "/home/komal/retailflow/lakehouse/silver"
GOLD_DIR = "/home/komal/retailflow/lakehouse/gold"


def get_spark():
    builder = (
        SparkSession.builder
        .appName("RetailFlowGoldDimensions")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_silver(spark, table_name):
    return spark.read.format("delta").load(f"{SILVER_DIR}/{table_name}")


def add_surrogate_key(df, key_name, order_col=None):
    """
    Adds a clean sequential integer surrogate key column.
    row_number() over a global window - fine at this data scale (not
    how you'd do it on a truly massive distributed table, but standard
    and correct here).
    """
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


# ------------------------------------------------------------------
def build_dim_date(spark):
    """
    Generated calendar table - same approach as Phase 2, just built
    with Spark's date functions instead of a SQL generate_series.
    Spans the order data's date range with headroom on both sides.
    """
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
        .withColumn("is_holiday", F.lit(False))  # placeholder, same as Phase 2
    )

    df = add_surrogate_key(df, "date_key", order_col="full_date")
    df = df.select(
        "date_key", "full_date", "day_of_month", "day_of_week", "day_name",
        "week_of_year", "month_num", "month_name", "quarter", "year",
        "is_weekend", "is_holiday",
    )
    write_gold(df, "dim_date")


def build_dim_geography(spark):
    """
    Aggregates raw geolocation (1M+ rows) down to one row per zip prefix,
    same aggregation logic as Phase 2: AVG lat/lng, MODE city/state.
    """
    geo = read_silver(spark, "geolocation")

    # MODE via count + rank: most frequent city/state per zip prefix
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


def build_dim_product(spark):
    products = read_silver(spark, "products")

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


def build_dim_seller(spark):
    sellers = read_silver(spark, "sellers")

    df = sellers.select(
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
    )
    df = add_surrogate_key(df, "seller_key", order_col="seller_id")
    df = df.select("seller_key", "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    write_gold(df, "dim_seller")


def build_dim_order_status(spark):
    orders = read_silver(spark, "orders")

    df = orders.select("order_status").distinct()
    df = add_surrogate_key(df, "order_status_key", order_col="order_status")
    df = df.select("order_status_key", "order_status")
    write_gold(df, "dim_order_status")


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Building Gold dimensions (Type 1)...\n")

    build_dim_date(spark)
    build_dim_geography(spark)
    build_dim_product(spark)
    build_dim_seller(spark)
    build_dim_order_status(spark)

    print("\nGold Type-1 dimensions complete.")
    spark.stop()


if __name__ == "__main__":
    main()
