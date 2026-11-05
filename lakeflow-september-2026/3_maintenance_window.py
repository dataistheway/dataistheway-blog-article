# Databricks notebook source
# MAGIC %md
# MAGIC # A maintenance window for a continuous pipeline job (Lakeflow, September 2026)
# MAGIC
# MAGIC A continuous job runs a pipeline all the time, so Databricks restarts it for maintenance.
# MAGIC Since September we can choose when: a day of the week, a start hour and a time zone.
# MAGIC The job is created PAUSED, so nothing runs. **Requires:** a full (Premium) workspace.

# COMMAND ----------

import base64
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dataistheway")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
w = WorkspaceClient()
api = w.api_client
SRC = f"/Workspace/Users/{w.current_user.me().user_name}/dataistheway/news104_src"
api.do("POST", "/api/2.0/workspace/mkdirs", body={"path": SRC})

# a pipeline for the job to run (created, not started)
path = f"{SRC}/news104_mw.py"
code = 'from pyspark import pipelines as dp\n\n@dp.materialized_view(name="mw_one")\ndef one():\n    return spark.range(1)\n'
api.do("POST", "/api/2.0/workspace/import", body={"path": path, "format": "AUTO", "overwrite": True,
                                                  "content": base64.b64encode(code.encode()).decode()})
pid = api.do("POST", "/api/2.0/pipelines", body={"name": "news104_mw", "catalog": catalog, "schema": schema,
    "serverless": True, "libraries": [{"file": {"path": path}}]})["pipeline_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: a continuous job with a maintenance window
# MAGIC Maintenance starts only within an hour of Sunday 02:00, Warsaw time. Requires a full (Premium) workspace.

# COMMAND ----------

window = {"start_hour": 2, "day_of_week": "SUNDAY", "timezone_id": "Europe/Warsaw"}
job_id = api.do("POST", "/api/2.2/jobs/create", body={
    "name": "news104_mw_job",
    "tasks": [{"task_key": "pipeline", "pipeline_task": {"pipeline_id": pid}}],
    "continuous": {"pause_status": "PAUSED", "maintenance_window": window},
})["job_id"]
print(api.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})["settings"]["continuous"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: what the API rejects
# MAGIC All three fields are required and the hour is 0-23. Requires a full (Premium) workspace.

# COMMAND ----------

for label, bad in (("no timezone_id", {"start_hour": 2, "day_of_week": "SUNDAY"}),
                   ("start_hour 24", {**window, "start_hour": 24})):
    try:
        api.do("POST", "/api/2.2/jobs/update", body={"job_id": job_id, "new_settings": {
            "continuous": {"pause_status": "PAUSED", "maintenance_window": bad}}})
        print(label, "-> accepted")
    except Exception as e:
        print(label, "->", str(e).splitlines()[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC The job, the pipeline (it never ran, so it has no tables) and its source file.

# COMMAND ----------

api.do("POST", "/api/2.2/jobs/delete", body={"job_id": job_id})
api.do("DELETE", f"/api/2.0/pipelines/{pid}", query={"cascade": "true"})
_ = api.do("POST", "/api/2.0/workspace/delete", body={"path": path})
