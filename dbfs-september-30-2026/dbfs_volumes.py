# Databricks notebook source
# MAGIC %md
# MAGIC # DBFS is gone from new workspaces. What changes on September 30, 2026 and how to prepare
# MAGIC
# MAGIC Code for the article [https://dataistheway.blog/en/dbfs-september-30-2026/](https://dataistheway.blog/en/dbfs-september-30-2026/).
# MAGIC
# MAGIC **Input:** synthetic data generated in the notebook. The `stores.csv` file (5 stores, no personal data)
# MAGIC is written with `dbutils.fs.put` to the `landing` volume instead of a manual upload from Catalog Explorer.
# MAGIC
# MAGIC **Name mapping** (article -> notebook, everything in `<catalog>.<schema>`):
# MAGIC - volume `landing` in the `default` schema of the reader's catalog -> volume `landing`
# MAGIC - `/Volumes/<catalog>/default/landing` -> `/Volumes/<catalog>/<schema>/landing`

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
# MAGIC ## Step 1: create the volume
# MAGIC Works on Free Edition

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS landing
# MAGIC COMMENT 'Manually uploaded files';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data preparation
# MAGIC In the article we upload the file through **Catalog** -> catalog -> schema -> `landing` -> **Upload to this volume**.
# MAGIC Here we write a small, synthetic `stores.csv` from code instead.

# COMMAND ----------

stores_csv = """store_id,store_name,city,country,opened_date
STORE001,Sklep Centrum,Warszawa,Poland,2021-03-01
STORE002,Sklep Rynek,Krakow,Poland,2022-06-15
STORE003,Sklep Port,Gdansk,Poland,2023-01-10
STORE004,Downtown Store,Austin,USA,2023-09-01
STORE005,Riverside Store,Seattle,USA,2024-04-20
"""
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/landing/stores.csv", stores_csv, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: list the volume and read the file with a plain path
# MAGIC Works on Free Edition

# COMMAND ----------

LANDING_PATH = f"/Volumes/{catalog}/{schema}/landing"

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
# MAGIC ## Step 3: the volume as a Unity Catalog object
# MAGIC Works on Free Edition

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE VOLUME landing;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra: checking the pandas pitfall
# MAGIC This block is not in the article. It checks whether pandas reads `/Volumes/...` directly on this compute.
# MAGIC The cell catches the exception so "Run all" keeps going. It is worth noting the result together with the compute type.

# COMMAND ----------

try:
    import pandas as pd
    pdf = pd.read_csv(f"{LANDING_PATH}/stores.csv")
    print("pandas reads /Volumes/... directly, rows:", len(pdf))
except Exception as e:
    print("pandas could not read /Volumes/...:", type(e).__name__)
    print(str(e)[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP VOLUME IF EXISTS landing")
