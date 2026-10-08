# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Jobs: multiple triggers on one job
# MAGIC
# MAGIC Code for the article https://dataistheway.blog/en/lakeflow-multiple-triggers/.
# MAGIC
# MAGIC A job with three triggers: table update, file arrival and a paused schedule as a backup.
# MAGIC We check which trigger started a run, what an update with one trigger does, and whether the legacy `schedule` can be added.
# MAGIC
# MAGIC **Requires:** a full (Premium) workspace, serverless and the **Multiple Triggers** preview enabled (Previews page).
# MAGIC **Data:** synthetic, a few rows. Import the whole folder: `zadanie/` holds the notebook the job runs.

# COMMAND ----------

import json, os, time
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dataistheway_triggers")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

api = WorkspaceClient().api_client
here = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
TASK_NB = os.path.dirname(here) + "/zadanie/zapisz_trigger"
LANDING = f"/Volumes/{catalog}/{schema}/landing/orders/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: source table, log table and a volume for files
# MAGIC Works on Free Edition.

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE zrodlo_zamowienia (id INT, kraj STRING, kwota DECIMAL(10,2))")
spark.sql("INSERT INTO zrodlo_zamowienia VALUES (1, 'PL', 120.00), (2, 'DE', 200.00), (3, 'CZ', 45.00)")
spark.sql("CREATE OR REPLACE TABLE log_uruchomien (ts TIMESTAMP, trigger_type STRING)")
spark.sql("CREATE VOLUME IF NOT EXISTS landing")
dbutils.fs.mkdirs(LANDING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: a job with a `triggers` array
# MAGIC Requires a full (Premium) workspace. The task writes the `{{job.trigger.type}}` value to `log_uruchomien`.

# COMMAND ----------

job = {
    "name": "news134_wiele_triggerow",
    "tasks": [{
        "task_key": "zapisz_trigger",
        "notebook_task": {
            "notebook_path": TASK_NB,
            "base_parameters": {"log_table": f"{catalog}.{schema}.log_uruchomien",
                                "trigger_type": "{{job.trigger.type}}"},
        },
    }],
    "triggers": [
        {"table_update": {"table_names": [f"{catalog}.{schema}.zrodlo_zamowienia"]}},
        {"file_arrival": {"url": LANDING}},
        {"schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "Europe/Warsaw"},
         "pause_status": "PAUSED"},
    ],
}
job_id = api.do("POST", "/api/2.2/jobs/create", body=job)["job_id"]
print("job created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: what the server stored
# MAGIC Requires a full (Premium) workspace.

# COMMAND ----------

settings = api.do("GET", "/api/2.2/jobs/get", query={"job_id": job_id})["settings"]
print(json.dumps(settings["triggers"], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: a table change starts the job
# MAGIC Requires a full (Premium) workspace. A fresh trigger first records the table state, so we wait 90 s before the `INSERT`.

# COMMAND ----------

def wait_for_run(trigger, od_ms):
    for _ in range(40):
        runs = api.do("GET", "/api/2.2/jobs/runs/list", query={"job_id": job_id, "start_time_from": od_ms})
        for r in runs.get("runs", []):
            if r["trigger"] == trigger and r["state"].get("result_state"):
                print(f"{trigger}: started after {(r['start_time'] - od_ms) / 1000:.0f} s, result {r['state']['result_state']}")
                return r
        time.sleep(15)
    print("no run with trigger =", trigger)

time.sleep(90)
od = int(time.time() * 1000)
spark.sql("INSERT INTO zrodlo_zamowienia VALUES (4, 'PL', 10.00), (5, 'CZ', 20.00)")
run = wait_for_run("TABLE", od)
print(json.dumps(run["trigger_info"], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: a new file in the volume starts the same job
# MAGIC Requires a full (Premium) workspace.

# COMMAND ----------

od = int(time.time() * 1000)
dbutils.fs.put(LANDING + "zamowienia_1.json", '{"id": 6, "kraj": "PL", "kwota": 5.0}', True)
run = wait_for_run("FILE_ARRIVAL", od)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- what the task got in {{job.trigger.type}}
# MAGIC SELECT * FROM log_uruchomien ORDER BY ts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: "just resume the schedule"
# MAGIC Requires a full (Premium) workspace. We send `triggers` with one element and count how many triggers are left.

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
# MAGIC ## Step 6: legacy `schedule` on a job with `triggers`
# MAGIC Requires a full (Premium) workspace. The cell prints the error.

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

# COMMAND ----------

api.do("POST", "/api/2.2/jobs/delete", body={"job_id": job_id})
spark.sql("DROP TABLE IF EXISTS zrodlo_zamowienia")
spark.sql("DROP TABLE IF EXISTS log_uruchomien")
spark.sql("DROP VOLUME IF EXISTS landing")
print("cleaned up")
