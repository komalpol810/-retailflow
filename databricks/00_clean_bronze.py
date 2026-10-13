# Databricks notebook source
storage_account = "retailflowstorage"
storage_key = dbutils.secrets.get(scope="retailflow", key="storage-key")
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)

BRONZE_DIR = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"

# COMMAND ----------

items = dbutils.fs.ls(BRONZE_DIR)
for item in items:
    dbutils.fs.rm(item.path, recurse=True)
    print(f"Removed {item.path}")

print(f"Bronze now has {len(dbutils.fs.ls(BRONZE_DIR))} items")
