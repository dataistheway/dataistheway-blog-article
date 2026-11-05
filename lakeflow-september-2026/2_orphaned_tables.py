# Databricks notebook source
# MAGIC %md
# MAGIC # Orphaned pipeline tables: find them, redeploy, adopt (Lakeflow, September 2026)
# MAGIC
# MAGIC A pipeline deleted the default way leaves its tables behind. How do we find such tables,
# MAGIC what happens when we deploy the same pipeline again, and can a new pipeline take the tables over?
# MAGIC **Requires:** a full (Premium) workspace with Unity Catalog and serverless. **Data:** synthetic, 6 rows.

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
w = WorkspaceClient()
api = w.api_client
SRC = f"/Workspace/Users/{w.current_user.me().user_name}/dataistheway/news104_src"
api.do("POST", "/api/2.0/workspace/mkdirs", body={"path": SRC})

spark.sql("CREATE OR REPLACE TABLE orph_source (id INT, country STRING, amount DECIMAL(10,2))")
spark.sql("""INSERT INTO orph_source VALUES
  (1, 'PL', 120.00), (2, 'PL', 80.50), (3, 'DE', 200.00), (4, 'CZ', 45.00), (5, 'DE', 99.90)""")

def create_pipeline(name, code):
    path = f"{SRC}/{name}.py"
    api.do("POST", "/api/2.0/workspace/import", body={"path": path, "format": "AUTO", "overwrite": True,
                                                      "content": base64.b64encode(code.encode()).decode()})
    spec = {"name": name, "catalog": catalog, "schema": schema, "serverless": True,
            "development": True, "libraries": [{"file": {"path": path}}]}
    pid = api.do("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]
    return pid, api.do("POST", f"/api/2.0/pipelines/{pid}/updates", body={})["update_id"]

def wait(pid, uid):
    while (state := api.do("GET", f"/api/2.0/pipelines/{pid}/updates/{uid}")["update"]["state"]) not in (
            "COMPLETED", "FAILED", "CANCELED"):
        time.sleep(10)
    return state

pipeline_code = '''
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="orph_orders")
def orders():
    return spark.readStream.table("orph_source")

@dp.materialized_view(name="orph_total_by_country")
def total_by_country():
    return spark.read.table("orph_orders").groupBy("country").agg(F.sum("amount").alias("total"))
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: a pipeline, deleted the default way
# MAGIC Requires a full (Premium) workspace.

# COMMAND ----------

pid_old, upd = create_pipeline("news104_orph", pipeline_code)
print("news104_orph:", wait(pid_old, upd))
api.do("DELETE", f"/api/2.0/pipelines/{pid_old}")
time.sleep(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: find the orphaned tables
# MAGIC Each pipeline table remembers its pipeline id. A table whose pipeline no longer exists is orphaned.

# COMMAND ----------

alive = {p.pipeline_id for p in w.pipelines.list_pipelines()}
rows = [(t.name, t.table_type.value, t.owner, t.pipeline_id, t.pipeline_id in alive)
        for t in w.tables.list(catalog, schema) if t.pipeline_id]
display(spark.createDataFrame(rows, "table_name string, table_type string, owner string, pipeline_id string, pipeline_exists boolean"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: deploy the same pipeline again
# MAGIC A new pipeline with the same code, so the same table names. Requires a full (Premium) workspace.

# COMMAND ----------

pid_new, upd = create_pipeline("news104_orph_new", pipeline_code)
state = wait(pid_new, upd)
print("news104_orph_new:", state)
if state == "FAILED":
    events = api.do("GET", f"/api/2.0/pipelines/{pid_new}/events", query={"filter": "level='ERROR'", "max_results": 3})
    for e in events.get("events", []):
        print("-", e["message"][:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: move the orphaned tables to the new pipeline
# MAGIC `pipelines.pipelineId` points a table at another pipeline (the "Move tables between pipelines" docs page).
# MAGIC Then one new order and an update of the new pipeline. Requires a full (Premium) workspace.

# COMMAND ----------

for stmt in (f'ALTER STREAMING TABLE orph_orders SET TBLPROPERTIES("pipelines.pipelineId"="{pid_new}")',
             f'ALTER MATERIALIZED VIEW orph_total_by_country SET TBLPROPERTIES("pipelines.pipelineId"="{pid_new}")'):
    try:
        spark.sql(stmt)
        print(stmt.split(" SET")[0], "-> OK")
    except Exception as e:
        print(stmt.split(" SET")[0], "->", str(e).splitlines()[0][:300])
spark.sql("INSERT INTO orph_source VALUES (6, 'PL', 1000.00)")
print("news104_orph_new:", wait(pid_new, api.do("POST", f"/api/2.0/pipelines/{pid_new}/updates", body={})["update_id"]))
display(spark.sql("SELECT country, total FROM orph_total_by_country ORDER BY country"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC The new pipeline with `cascade=true`, leftover tables, the source table and the pipeline source files.

# COMMAND ----------

api.do("DELETE", f"/api/2.0/pipelines/{pid_new}", query={"cascade": "true"})
time.sleep(20)
spark.sql("DROP MATERIALIZED VIEW IF EXISTS orph_total_by_country")
for t in ("orph_orders", "orph_source"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
for name in ("news104_orph", "news104_orph_new"):
    api.do("POST", "/api/2.0/workspace/delete", body={"path": f"{SRC}/{name}.py"})
