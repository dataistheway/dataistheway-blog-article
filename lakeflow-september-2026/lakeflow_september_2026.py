# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow in September 2026: deleting a pipeline, maintenance windows and import_checkpoint
# MAGIC
# MAGIC Code for the article https://dataistheway.blog/en/lakeflow-september-2026-changes/. Three small serverless pipelines created through the REST API:
# MAGIC `news104_a` is deleted the default way (its tables stay), `news104_b` with `cascade=true` (its tables go) and `news104_c` takes over the checkpoint
# MAGIC of a plain Structured Streaming query with `import_checkpoint` (Beta). At the end a PAUSED continuous job gets a maintenance window, so nothing runs continuously.
# MAGIC **Requires:** a full (Premium) workspace with Unity Catalog and serverless. **Data:** synthetic, 8 rows.

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
CKPT = f"/Volumes/{catalog}/{schema}/news104_checkpoints"
w = WorkspaceClient()
api = w.api_client
SRC = f"/Workspace/Users/{w.current_user.me().user_name}/dataistheway/news104_src"
api.do("POST", "/api/2.0/workspace/mkdirs", body={"path": SRC})

# this notebook's tables, plus the pipelines' event logs and internal tables
TABLES = """SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()
  AND table_name RLIKE '^(source_orders|ss_orders|[abc]_)|^event_log_|^__materialization'
  ORDER BY table_name"""
spark.sql("CREATE OR REPLACE TABLE source_orders (id INT, country STRING, amount DECIMAL(10,2))")
spark.sql("""INSERT INTO source_orders VALUES
  (1, 'PL', 120.00), (2, 'PL', 80.50), (3, 'DE', 200.00), (4, 'CZ', 45.00), (5, 'DE', 99.90)""")

# COMMAND ----------

def create_pipeline(name, code):
    path = f"{SRC}/{name}.py"
    api.do("POST", "/api/2.0/workspace/import", body={"path": path, "format": "AUTO", "overwrite": True,
                                                      "content": base64.b64encode(code.encode()).decode()})
    spec = {"name": name, "catalog": catalog, "schema": schema, "serverless": True,
            "development": True, "continuous": False, "libraries": [{"file": {"path": path}}]}
    pid = api.do("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]
    return pid, api.do("POST", f"/api/2.0/pipelines/{pid}/updates", body={})["update_id"]

def wait(pid, uid):
    while True:
        state = api.do("GET", f"/api/2.0/pipelines/{pid}/updates/{uid}")["update"]["state"]
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            return state
        time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: two small serverless pipelines (a streaming table and a materialized view)
# MAGIC A and B run the same code, only the table prefix differs. Requires a full (Premium) workspace.

# COMMAND ----------

pipeline_code = '''
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="{p}_orders")
def orders():
    return spark.readStream.table("source_orders")

@dp.materialized_view(name="{p}_total_by_country")
def total_by_country():
    return (spark.read.table("{p}_orders")
            .groupBy("country").agg(F.sum("amount").alias("total")))
'''
pid_a, upd_a = create_pipeline("news104_a", pipeline_code.format(p="a"))
pid_b, upd_b = create_pipeline("news104_b", pipeline_code.format(p="b"))
t0 = time.time()
print("news104_a:", wait(pid_a, upd_a), f"after {time.time() - t0:.0f} s")
print("news104_b:", wait(pid_b, upd_b), f"after {time.time() - t0:.0f} s")
info = api.do("GET", f"/api/2.0/pipelines/{pid_a}")
print("effective_publishing_mode:", info.get("effective_publishing_mode") or info.get("spec", {}).get("effective_publishing_mode"))
display(spark.sql(TABLES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: delete pipeline A the default way (no `cascade`)
# MAGIC The same call as the Delete button in the UI. Requires a full (Premium) workspace.

# COMMAND ----------

api.do("DELETE", f"/api/2.0/pipelines/{pid_a}")
time.sleep(20)
left = api.do("GET", "/api/2.0/pipelines", query={"filter": "name LIKE 'news104_%'"}).get("statuses", [])
print("news104_* pipelines:", sorted(p["name"] for p in left))
display(spark.sql(TABLES))
# the tables of the deleted pipeline A can still be read
display(spark.sql("""SELECT (SELECT COUNT(*) FROM a_orders) AS a_orders_rows,
  (SELECT COUNT(*) FROM a_total_by_country) AS a_total_by_country_rows"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: can the orphaned tables be refreshed?
# MAGIC The cell prints the result or the error. Requires a full (Premium) workspace.

# COMMAND ----------

for stmt in ("REFRESH MATERIALIZED VIEW a_total_by_country", "REFRESH STREAMING TABLE a_orders"):
    try:
        spark.sql(stmt)
        print(stmt, "-> OK")
    except Exception as e:
        print(stmt, "->", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: delete pipeline B with `cascade=true`
# MAGIC The old behaviour: the pipeline takes its tables with it. Requires a full (Premium) workspace.

# COMMAND ----------

api.do("DELETE", f"/api/2.0/pipelines/{pid_b}", query={"cascade": "true"})
time.sleep(20)
display(spark.sql(TABLES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: a plain Structured Streaming query with a checkpoint in a volume
# MAGIC This is the "old" job we want to move into a pipeline. Works on Free Edition.

# COMMAND ----------

(spark.readStream.table("source_orders")
    .writeStream
    .option("checkpointLocation", f"{CKPT}/ss_orders")
    .trigger(availableNow=True)
    .toTable("ss_orders")
    .awaitTermination())
# the old job is stopped, then 3 new orders arrive in the source
spark.sql("INSERT INTO source_orders VALUES (6, 'PL', 10.00), (7, 'CZ', 20.00), (8, 'DE', 30.00)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: a pipeline with `create_table` and `import_checkpoint` (Beta)
# MAGIC The flow points at the checkpoint from step 5 and writes to a new managed table. Requires a full (Premium) workspace.

# COMMAND ----------

migration_code = f'''
from pyspark import pipelines as dp

dp.create_table("c_orders")

@dp.append_flow(
    target="c_orders",
    import_checkpoint="{CKPT}/ss_orders",
)
def migrate_orders():
    return spark.readStream.table("source_orders")
'''
pid_c, upd_c = create_pipeline("news104_c", migration_code)
t0 = time.time()
print("news104_c:", wait(pid_c, upd_c), f"after {time.time() - t0:.0f} s")
# if the checkpoint was taken over, c_orders has only the 3 new rows (ids 6-8)
display(spark.sql("""SELECT 'ss_orders' AS table_name, COUNT(*) AS row_count, MIN(id) AS min_id, MAX(id) AS max_id FROM ss_orders
  UNION ALL SELECT 'c_orders', COUNT(*), MIN(id), MAX(id) FROM c_orders"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: a maintenance window on a `continuous` job (PAUSED)
# MAGIC The job runs pipeline C, but it is paused, so nothing starts. Requires a full (Premium) workspace.

# COMMAND ----------

job_spec = {
    "name": "news104_continuous",
    "tasks": [{"task_key": "pipeline", "pipeline_task": {"pipeline_id": pid_c}}],
    "continuous": {"pause_status": "PAUSED",
                   "maintenance_window": {"start_hour": 2, "day_of_week": "SUNDAY", "timezone_id": "Europe/Warsaw"}},
}
job_id = api.do("POST", "/api/2.2/jobs/create", body=job_spec)["job_id"]
print("continuous:", api.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})["settings"]["continuous"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC The job, pipeline C with `cascade=true`, the orphaned A tables, the other tables, the checkpoint volume and the pipeline source files.

# COMMAND ----------

api.do("POST", "/api/2.2/jobs/delete", body={"job_id": job_id})
api.do("DELETE", f"/api/2.0/pipelines/{pid_c}", query={"cascade": "true"})
time.sleep(20)
spark.sql("DROP MATERIALIZED VIEW IF EXISTS a_total_by_country")
for t in ("a_orders", "ss_orders", "source_orders"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
spark.sql("DROP VOLUME IF EXISTS news104_checkpoints")
api.do("POST", "/api/2.0/workspace/delete", body={"path": SRC, "recursive": True})
display(spark.sql(TABLES))
