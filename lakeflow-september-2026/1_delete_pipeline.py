# Databricks notebook source
# MAGIC %md
# MAGIC # Deleting a pipeline keeps its tables (Lakeflow, September 2026)
# MAGIC
# MAGIC Since 25.09.2026 deleting a Unity Catalog pipeline keeps its streaming tables and materialized views.
# MAGIC Two identical pipelines: `news104_del_a` is deleted the default way, `news104_del_b` with `cascade=true`.
# MAGIC **Requires:** a full (Premium) workspace with Unity Catalog and serverless. **Data:** synthetic, 5 rows.

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

spark.sql("CREATE OR REPLACE TABLE del_source (id INT, country STRING, amount DECIMAL(10,2))")
spark.sql("""INSERT INTO del_source VALUES
  (1, 'PL', 120.00), (2, 'PL', 80.50), (3, 'DE', 200.00), (4, 'CZ', 45.00), (5, 'DE', 99.90)""")
TABLES = """SELECT table_name, table_type FROM information_schema.tables
  WHERE table_schema = current_schema() AND table_name LIKE 'del\\_%' ORDER BY table_name"""

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: two identical pipelines, each with a streaming table and a materialized view
# MAGIC Requires a full (Premium) workspace.

# COMMAND ----------

pipeline_code = '''
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="del_{p}_orders")
def orders():
    return spark.readStream.table("del_source")

@dp.materialized_view(name="del_{p}_total_by_country")
def total_by_country():
    return spark.read.table("del_{p}_orders").groupBy("country").agg(F.sum("amount").alias("total"))
'''
pid_a, upd_a = create_pipeline("news104_del_a", pipeline_code.format(p="a"))
pid_b, upd_b = create_pipeline("news104_del_b", pipeline_code.format(p="b"))
print("news104_del_a:", wait(pid_a, upd_a), "| news104_del_b:", wait(pid_b, upd_b))
display(spark.sql(TABLES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: delete pipeline A the default way
# MAGIC The same call as the Delete button in the UI: no `cascade` parameter. Requires a full (Premium) workspace.

# COMMAND ----------

api.do("DELETE", f"/api/2.0/pipelines/{pid_a}")
time.sleep(20)
display(spark.sql(TABLES))
display(spark.sql("SELECT country, total FROM del_a_total_by_country ORDER BY country"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: new data does not reach the tables of the deleted pipeline
# MAGIC Pipeline B still runs, so its tables pick up the new order. Pipeline A is gone, so its tables stay as they were.

# COMMAND ----------

spark.sql("INSERT INTO del_source VALUES (6, 'PL', 1000.00)")
print("news104_del_b:", wait(pid_b, api.do("POST", f"/api/2.0/pipelines/{pid_b}/updates", body={})["update_id"]))
display(spark.sql("""SELECT 'del_a_orders' AS table_name, COUNT(*) AS row_count FROM del_a_orders
  UNION ALL SELECT 'del_b_orders', COUNT(*) FROM del_b_orders"""))

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
# MAGIC ## Cleanup
# MAGIC The tables left by pipeline A, the source table and the pipeline source files.

# COMMAND ----------

spark.sql("DROP MATERIALIZED VIEW IF EXISTS del_a_total_by_country")
for t in ("del_a_orders", "del_source"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
for name in ("news104_del_a", "news104_del_b"):
    api.do("POST", "/api/2.0/workspace/delete", body={"path": f"{SRC}/{name}.py"})
