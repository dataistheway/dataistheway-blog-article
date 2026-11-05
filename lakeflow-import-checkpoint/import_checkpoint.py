# Databricks notebook source
# MAGIC %md
# MAGIC # Moving a Structured Streaming job into a pipeline with import_checkpoint (Beta)
# MAGIC
# MAGIC A plain Structured Streaming query deduplicates orders by `id`. A pipeline takes over its checkpoint
# MAGIC and its table: does it keep the deduplication state, and what does a full refresh do afterwards?
# MAGIC **Requires:** a full (Premium) workspace with Unity Catalog and serverless. **Data:** synthetic, 9 rows.

# COMMAND ----------

import base64, time
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dataistheway")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
spark.sql("CREATE VOLUME IF NOT EXISTS news104_checkpoints")
CKPT = f"/Volumes/{catalog}/{schema}/news104_checkpoints/ckpt_orders"
w = WorkspaceClient()
api = w.api_client
SRC = f"/Workspace/Users/{w.current_user.me().user_name}/dataistheway/news104_src"
api.do("POST", "/api/2.0/workspace/mkdirs", body={"path": SRC})

spark.sql("CREATE OR REPLACE TABLE ckpt_source (id INT, country STRING, amount DECIMAL(10,2))")
spark.sql("""INSERT INTO ckpt_source VALUES
  (1, 'PL', 120.00), (2, 'PL', 80.50), (3, 'DE', 200.00), (4, 'CZ', 45.00), (5, 'DE', 99.90)""")
ORDERS = "SELECT id, country, amount FROM ckpt_orders ORDER BY id"

def wait(pid, uid):
    while (state := api.do("GET", f"/api/2.0/pipelines/{pid}/updates/{uid}")["update"]["state"]) not in (
            "COMPLETED", "FAILED", "CANCELED"):
        time.sleep(10)
    return state

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: the old job, a Structured Streaming query with deduplication
# MAGIC It writes `ckpt_orders` with a checkpoint in a volume and stops. Then 4 orders arrive: 3 new ones and id 3 again.
# MAGIC According to the docs this step works on Free Edition, I have not tested it.

# COMMAND ----------

(spark.readStream.table("ckpt_source")
    .dropDuplicates(["id"])
    .writeStream
    .option("checkpointLocation", CKPT)
    .trigger(availableNow=True)
    .toTable("ckpt_orders")
    .awaitTermination())
spark.sql("INSERT INTO ckpt_source VALUES (3, 'DE', 200.00), (6, 'PL', 10.00), (7, 'CZ', 20.00), (8, 'DE', 30.00)")
display(spark.sql(ORDERS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: a pipeline takes over the checkpoint and the table
# MAGIC `dp.create_table` points at the old job's table, `import_checkpoint` at its checkpoint, and the query stays the same.
# MAGIC If the state came along, id 3 is not added again. Requires a full (Premium) workspace, Beta.

# COMMAND ----------

migration_code = f'''
from pyspark import pipelines as dp

dp.create_table("ckpt_orders")

@dp.append_flow(target="ckpt_orders", import_checkpoint="{CKPT}")
def migrate_orders():
    return spark.readStream.table("ckpt_source").dropDuplicates(["id"])
'''
path = f"{SRC}/news104_ckpt.py"
api.do("POST", "/api/2.0/workspace/import", body={"path": path, "format": "AUTO", "overwrite": True,
                                                  "content": base64.b64encode(migration_code.encode()).decode()})
pid = api.do("POST", "/api/2.0/pipelines", body={"name": "news104_ckpt", "catalog": catalog, "schema": schema,
    "serverless": True, "development": True, "libraries": [{"file": {"path": path}}]})["pipeline_id"]
print("news104_ckpt:", wait(pid, api.do("POST", f"/api/2.0/pipelines/{pid}/updates", body={})["update_id"]))
display(spark.sql(ORDERS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: full refresh when the source keeps only recent data
# MAGIC Sources such as Kafka keep data only for a while. We simulate it by deleting the old orders, then run a full refresh.
# MAGIC Requires a full (Premium) workspace, Beta.

# COMMAND ----------

spark.sql("DELETE FROM ckpt_source WHERE id <= 5")
upd = api.do("POST", f"/api/2.0/pipelines/{pid}/updates", body={"full_refresh": True})["update_id"]
print("news104_ckpt full refresh:", wait(pid, upd))
display(spark.sql(ORDERS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC The pipeline with `cascade=true`, the tables, the checkpoint volume and the pipeline source file.

# COMMAND ----------

api.do("DELETE", f"/api/2.0/pipelines/{pid}", query={"cascade": "true"})
time.sleep(20)
for t in ("ckpt_orders", "ckpt_source"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
spark.sql("DROP VOLUME IF EXISTS news104_checkpoints")
_ = api.do("POST", "/api/2.0/workspace/delete", body={"path": path})
