# Databricks notebook source
# MAGIC %md
# MAGIC # DBFS is gone from new workspaces. What changes on September 30, 2026 and how to prepare
# MAGIC
# MAGIC Code for the article [https://dataistheway.blog/en/dbfs-september-30-2026/](https://dataistheway.blog/en/dbfs-september-30-2026/).
# MAGIC
# MAGIC First we check what a workspace without legacy features does with old paths, then we use a Unity Catalog volume instead.
# MAGIC
# MAGIC **Input:** synthetic data generated in the notebook. The `stores.csv` file (5 stores, no personal data)
# MAGIC is written with `dbutils.fs.put` to the `news01_landing` volume instead of a manual upload from Catalog Explorer.
# MAGIC
# MAGIC **Name mapping** (article -> notebook, everything in `<catalog>.<schema>`):
# MAGIC - volume `landing` in the `default` schema of the reader's catalog -> volume `news01_landing`
# MAGIC - `/Volumes/<catalog>/default/landing` -> `/Volumes/<catalog>/<schema>/news01_landing`
# MAGIC - table `news01_stores` is created in `<catalog>.<schema>`

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dataistheway")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: old paths on a workspace without legacy features
# MAGIC On a workspace without legacy features (every new one since September 30, 2026) DBFS root and Hive Metastore are disabled, so both reads end with an error.
# MAGIC Tested on Free Edition (2026-10-17): the same two errors

# COMMAND ----------

try:
    spark.sql("SELECT * FROM read_files('dbfs:/FileStore/stores.csv', format => 'csv')").show()
except Exception as e:
    print(str(e).split("\n")[0])

try:
    spark.sql("SHOW SCHEMAS IN hive_metastore").show()
except Exception as e:
    print(str(e).split("\n")[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: create the volume
# MAGIC Tested on Free Edition (2026-10-17)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS news01_landing
# MAGIC COMMENT 'Manually uploaded files';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data preparation
# MAGIC In the article we upload the file through **Catalog** -> catalog -> `default` -> `landing` -> **Upload to this volume**.
# MAGIC Here we write a small, synthetic `stores.csv` from code instead.

# COMMAND ----------

stores_csv = """store_id,store_name,city,country,opened_date
STORE001,City Centre Store,Warszawa,Poland,2021-03-01
STORE002,Market Square Store,Krakow,Poland,2022-06-15
STORE003,Harbour Store,Gdansk,Poland,2023-01-10
STORE004,Downtown Store,Austin,USA,2023-09-01
STORE005,Riverside Store,Seattle,USA,2024-04-20
"""
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/news01_landing/stores.csv", stores_csv, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: list the volume and read the file with a plain path
# MAGIC Tested on Free Edition (2026-10-17)

# COMMAND ----------

LANDING_PATH = f"/Volumes/{catalog}/{schema}/news01_landing"

# What is in the volume? (same as LIST '/Volumes/...' in SQL)
for f in dbutils.fs.ls(LANDING_PATH):
    print(f"{f.name:<20} {f.size:>8,} bytes")

display(spark.sql(f"""
    SELECT *
    FROM read_files('{LANDING_PATH}/stores.csv', format => 'csv', header => true)
    LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: the volume as a Unity Catalog object
# MAGIC Tested on Free Edition (2026-10-17)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE VOLUME news01_landing;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: load the file into a Delta table
# MAGIC Tested on Free Edition (2026-10-17)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE news01_stores AS
    SELECT * FROM read_files('{LANDING_PATH}/stores.csv', format => 'csv', header => true)
""")
display(spark.sql("SELECT count(*) AS stores FROM news01_stores"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: pandas with /dbfs/... and with /Volumes/...
# MAGIC Tested on Free Edition (2026-10-17)

# COMMAND ----------

import pandas as pd

# Old path through /dbfs: there is no DBFS root on this workspace
try:
    pd.read_csv("/dbfs/FileStore/stores.csv")
except Exception as e:
    print(type(e).__name__, e)

# New path: the volume
pdf = pd.read_csv(f"{LANDING_PATH}/stores.csv")
print(len(pdf), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS news01_stores")
spark.sql("DROP VOLUME IF EXISTS news01_landing")
