# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Jobs: a backup schedule that notices stale data
# MAGIC
# MAGIC Code for the article https://dataistheway.blog/en/lakeflow-multiple-triggers/.
# MAGIC
# MAGIC A backup schedule recomputes stale data and ends green, unless the task checks freshness.
# MAGIC The task fails a run started by the schedule (`{{job.trigger.type}}` = `periodic`) when the newest row is older than the SLA,
# MAGIC so the usual failed-run notification fires. A run started by a table change skips the check.
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
TASK_NB = os.path.dirname(here) + "/task/check_freshness"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: a source table whose newest row is two days old

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE source_orders (id INT, amount DECIMAL(10,2), ingested_at TIMESTAMP)")
spark.sql("INSERT INTO source_orders VALUES (1, 120.00, current_timestamp() - INTERVAL 2 DAYS)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: table update plus a backup schedule
# MAGIC The schedule runs every minute only for this demo. The task gets the trigger type and a 24-hour SLA.

# COMMAND ----------

job_id = api.do("POST", "/api/2.2/jobs/create", body={
    "name": "news134_multiple_triggers",
    "max_concurrent_runs": 1,
    "tasks": [{"task_key": "check_freshness", "notebook_task": {
        "notebook_path": TASK_NB,
        "base_parameters": {"source_table": f"{catalog}.{schema}.source_orders",
                            "trigger_type": "{{job.trigger.type}}", "sla_hours": "24"}}}],
    "triggers": [
        {"table_update": {"table_names": [f"{catalog}.{schema}.source_orders"]}},
        {"schedule": {"quartz_cron_expression": "0 * * * * ?", "timezone_id": "Europe/Warsaw"}},
    ],
})["job_id"]
print("job created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: the schedule fires and the freshness check fails the run
# MAGIC Then we pause the schedule the safe way: read the array, change one element, send the whole array back.

# COMMAND ----------

def wait_for_run(trigger, start_ms):
    for _ in range(40):
        time.sleep(15)
        runs = api.do("GET", "/api/2.2/jobs/runs/list", query={"job_id": job_id, "start_time_from": start_ms}).get("runs", [])
        done = [r for r in runs if r["trigger"] == trigger and r["state"].get("result_state")]
        if done:
            print(f"{trigger}: result {done[0]['state']['result_state']}")
            return done[0]

run = wait_for_run("PERIODIC", int(time.time() * 1000))
task_run_id = api.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run["run_id"]})["tasks"][0]["run_id"]
print(api.do("GET", "/api/2.2/jobs/runs/get-output", query={"run_id": task_run_id}).get("error"))

triggers = api.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})["settings"]["triggers"]
for t in triggers:
    if "schedule" in t:
        t["pause_status"] = "PAUSED"
api.do("POST", "/api/2.2/jobs/update", body={"job_id": job_id, "new_settings": {"triggers": triggers}})
print("schedule paused")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: fresh data, a run from the table update
# MAGIC A run started by a table change skips the check (`{{job.trigger.type}}` = `table`).
# MAGIC In my lab an `INSERT` right after the update of the `triggers` array did not start a run, so we wait 90 s first.

# COMMAND ----------

time.sleep(90)
start = int(time.time() * 1000)
spark.sql("INSERT INTO source_orders VALUES (2, 80.00, current_timestamp())")
run = wait_for_run("TABLE", start)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Delete the job and the table (the schema stays).

# COMMAND ----------

api.do("POST", "/api/2.2/jobs/delete", body={"job_id": job_id})
spark.sql("DROP TABLE IF EXISTS source_orders")
print("cleaned up")
