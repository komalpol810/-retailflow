"""
Diagnostic: find customer_id rows in silver.customers that have NO
matching order in silver.orders. Since dim_customer's SCD2 build joins
customers to orders (inner join) to get a timestamp for each address
version, any customer_id with no matching order silently loses that
whole address version from the history - which could explain missing
historical rows without showing up as an error anywhere.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

SILVER_DIR = "lakehouse/silver"

builder = (
    SparkSession.builder
    .appName("CheckOrphanedCustomerIds")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

customers = spark.read.format("delta").load(f"{SILVER_DIR}/customers")
orders = spark.read.format("delta").load(f"{SILVER_DIR}/orders")

cust_ids = customers.select("customer_id").distinct()
order_cust_ids = orders.select("customer_id").distinct()

orphaned = cust_ids.join(order_cust_ids, "customer_id", "left_anti")

print(f"Total customer_id rows in customers: {cust_ids.count()}")
print(f"Total distinct customer_id in orders: {order_cust_ids.count()}")
print(f"customer_id rows in customers with NO matching order: {orphaned.count()}")

# For the orphaned ones, check which customer_unique_id they belong to,
# and whether that customer_unique_id has OTHER customer_id rows that DO
# have orders (meaning we'd lose exactly one version, not the whole customer)
orphaned_detail = orphaned.join(customers, "customer_id").select(
    "customer_id", "customer_unique_id", "customer_zip_code_prefix",
    "customer_city", "customer_state"
)
print("\nOrphaned customer_id rows detail:")
orphaned_detail.show(20, truncate=False)

spark.stop()
