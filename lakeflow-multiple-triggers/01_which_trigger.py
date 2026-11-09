# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Jobs: which trigger started the run
# MAGIC
# MAGIC Code for the article https://dataistheway.blog/en/lakeflow-multiple-triggers/.
# MAGIC
# MAGIC A job with two file arrival triggers on different folders. The task logs the trigger type and
# MAGIC `{{job.trigger.file_arrival.location}}`, which tells which folder started the run.
# MAGIC Then we add a table update trigger to the same job and see that this value is no longer allowed.
# MAGIC
# MAGIC **Requires:** the **Multiple Triggers** preview (workspace Previews page) and serverless.
# MAGIC **Data:** synthetic, a few rows. Import the whole folder: `task/` holds the notebook the job runs.

# COMMAND ----------

import os, time
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "news_134")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

api = WorkspaceClient().api_client
here = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
TASK_NB = os.path.dirname(here) + "/task/log_trigger"
LANDING = f"/Volumes/{catalog}/{schema}/landing/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: a source table, a log table and a volume with two folders

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE source_orders (id INT, country STRING, amount DECIMAL(10,2))")
spark.sql("CREATE OR REPLACE TABLE trigger_log (ts TIMESTAMP, trigger_type STRING, location STRING)")
spark.sql("CREATE VOLUME IF NOT EXISTS landing")
dbutils.fs.mkdirs(LANDING + "a/")
dbutils.fs.mkdirs(LANDING + "b/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: one job, two file arrival triggers
# MAGIC The task gets the trigger type and the folder of the trigger that started the run.

# COMMAND ----------

job = {
    "name": "news134_multiple_triggers",
    "tasks": [{
        "task_key": "log_trigger",
        "notebook_task": {
            "notebook_path": TASK_NB,
            "base_parameters": {"log_table": f"{catalog}.{schema}.trigger_log",
                                "trigger_type": "{{job.trigger.type}}",
                                "location": "{{job.trigger.file_arrival.location}}"},
        },
    }],
    "triggers": [
        {"file_arrival": {"url": LANDING + "a/"}},
        {"file_arrival": {"url": LANDING + "b/"}},
    ],
}
job_id = api.do("POST", "/api/2.2/jobs/create", body=job)["job_id"]
print("job created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: a file in folder `a/`, then a file in folder `b/`
# MAGIC A file written right after the job was created did not start a run in my lab, so we wait 90 s first.

# COMMAND ----------

time.sleep(90)
for folder in ["a/", "b/"]:
    start = int(time.time() * 1000)
    dbutils.fs.put(LANDING + folder + "orders.json", '{"id": 1, "country": "PL", "amount": 5.0}', True)
    for _ in range(40):
        time.sleep(15)
        runs = api.do("GET", "/api/2.2/jobs/runs/list", query={"job_id": job_id, "start_time_from": start}).get("runs", [])
        done = [r for r in runs if r["state"].get("result_state")]
        if done:
            print(f"{folder}: {done[0]['trigger']} after {(done[0]['start_time'] - start) / 1000:.0f} s, {done[0]['state']['result_state']}")
            break

# COMMAND ----------

# MAGIC %sql
# MAGIC -- what the task got from the dynamic values
# MAGIC SELECT trigger_type, location FROM trigger_log ORDER BY ts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: add a table update trigger to the same job
# MAGIC We send the whole array (the two file arrival triggers plus a table update). The cell prints the error.

# COMMAND ----------

triggers = api.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})["settings"]["triggers"]
triggers.append({"table_update": {"table_names": [f"{catalog}.{schema}.source_orders"]}})
try:
    api.do("POST", "/api/2.2/jobs/update", body={"job_id": job_id, "new_settings": {"triggers": triggers}})
    print("accepted")
except Exception as e:
    print(str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Delete the job, the tables and the volume (the schema stays).

# COMMAND ----------

api.do("POST", "/api/2.2/jobs/delete", body={"job_id": job_id})
spark.sql("DROP TABLE IF EXISTS source_orders")
spark.sql("DROP TABLE IF EXISTS trigger_log")
spark.sql("DROP VOLUME IF EXISTS landing")
print("cleaned up")
