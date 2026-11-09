# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Jobs: changing one trigger without losing the others
# MAGIC
# MAGIC Code for the article https://dataistheway.blog/en/lakeflow-multiple-triggers/.
# MAGIC
# MAGIC An update of the `triggers` field replaces the whole array. We resume one trigger the safe way (the full array),
# MAGIC then the wrong way (one element), and try the legacy `schedule` field on a job that uses `triggers`.
# MAGIC
# MAGIC **Requires:** the **Multiple Triggers** preview (workspace Previews page) and serverless. No job runs: every trigger stays paused or scheduled for 06:00.
# MAGIC **Data:** one synthetic table. Import the whole folder: `task/` holds the notebook the job would run.

# COMMAND ----------

import json, os
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: a job with two paused triggers
# MAGIC A table update and a backup schedule, both `PAUSED`.

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE source_orders (id INT, amount DECIMAL(10,2))")
job_id = api.do("POST", "/api/2.2/jobs/create", body={
    "name": "news134_multiple_triggers",
    "tasks": [{"task_key": "log_trigger", "notebook_task": {"notebook_path": TASK_NB}}],
    "triggers": [
        {"table_update": {"table_names": [f"{catalog}.{schema}.source_orders"]}, "pause_status": "PAUSED"},
        {"schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "Europe/Warsaw"},
         "pause_status": "PAUSED"},
    ],
})["job_id"]
print("job created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: resume the schedule the safe way
# MAGIC Read the array, change one element, send the whole array back.

# COMMAND ----------

triggers = api.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})["settings"]["triggers"]
for t in triggers:
    if "schedule" in t:
        t["pause_status"] = "UNPAUSED"
api.do("POST", "/api/2.2/jobs/update", body={"job_id": job_id, "new_settings": {"triggers": triggers}})

for t in api.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})["settings"]["triggers"]:
    print([k for k in t if k != "pause_status"][0], t["pause_status"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: "just resume the schedule" with one element
# MAGIC It looks like resuming one trigger, but `jobs/update` replaces the whole array.

# COMMAND ----------

api.do("POST", "/api/2.2/jobs/update", body={"job_id": job_id, "new_settings": {"triggers": [
    {"schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "Europe/Warsaw"},
     "pause_status": "UNPAUSED"},
]}})
triggers = api.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})["settings"]["triggers"]
print(len(triggers), "trigger(s) after the update:")
print(json.dumps(triggers, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: legacy `schedule` on a job with `triggers`
# MAGIC Can a job with a `triggers` array still take the old `schedule` field? The cell prints the error.

# COMMAND ----------

try:
    api.do("POST", "/api/2.2/jobs/update", body={"job_id": job_id, "new_settings": {
        "schedule": {"quartz_cron_expression": "0 0 7 * * ?", "timezone_id": "Europe/Warsaw"}}})
    print("accepted")
except Exception as e:
    print(str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Delete the job and the table (the schema stays).

# COMMAND ----------

api.do("POST", "/api/2.2/jobs/delete", body={"job_id": job_id})
spark.sql("DROP TABLE IF EXISTS source_orders")
print("cleaned up")
