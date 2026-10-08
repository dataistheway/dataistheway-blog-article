# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow: a pipeline that doesn't belong to one person
# MAGIC
# MAGIC Code for the article https://dataistheway.blog/en/lakeflow-pipeline-run-as-group/.
# MAGIC
# MAGIC A pipeline with `run_as.group_name`: who owns the table, which groups are not allowed,
# MAGIC how to switch back to a user and whether `SET OWNER TO` changes anything.
# MAGIC
# MAGIC **Requires:** a full (Premium) workspace, Unity Catalog, serverless. Run it as a workspace admin, because it creates an account group.
# MAGIC **Data:** synthetic, 5 rows. Import the whole folder: `pipeline/` holds the pipeline code (one materialized view).

# COMMAND ----------

import os, time
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dataistheway_run_as")
dbutils.widgets.text("group_name", "dataistheway-etl-team")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
GROUP = dbutils.widgets.get("group_name")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

w = WorkspaceClient()
api = w.api_client
me = w.current_user.me()
here = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PIPE_DIR = os.path.dirname(here) + "/pipeline"

spark.sql("CREATE OR REPLACE TABLE zrodlo_zamowienia (id INT, kraj STRING, kwota DECIMAL(10,2))")
spark.sql("""INSERT INTO zrodlo_zamowienia VALUES
  (1, 'PL', 120.00), (2, 'PL', 80.50), (3, 'DE', 200.00), (4, 'CZ', 45.00), (5, 'DE', 99.90)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: an account group assigned to the workspace, with us as a member
# MAGIC Requires a full (Premium) workspace. Workspace-local groups can't be run-as.

# COMMAND ----------

gid = api.do("POST", "/api/2.0/account/scim/v2/Groups",
             body={"displayName": GROUP, "members": [{"value": me.id}]})["id"]
api.do("PUT", f"/api/2.0/preview/permissionassignments/principals/{gid}", body={"permissions": ["USER"]})
api.do("PATCH", f"/api/2.0/preview/scim/v2/Groups/{gid}", body={
    "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
    "Operations": [{"op": "add", "path": "entitlements", "value": [{"value": "workspace-access"}]}]})
print("group", GROUP, "created and assigned to the workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: permissions for the group
# MAGIC Requires a full (Premium) workspace. The pipeline runs as the group, so our own permissions don't count.

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{GROUP}`")
spark.sql(f"GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, SELECT, MODIFY ON SCHEMA {catalog}.{schema} TO `{GROUP}`")
dir_id = api.do("GET", "/api/2.0/workspace/get-status", query={"path": PIPE_DIR})["object_id"]
api.do("PATCH", f"/api/2.0/permissions/directories/{dir_id}",
       body={"access_control_list": [{"group_name": GROUP, "permission_level": "CAN_READ"}]})
print("grants and Can read on the pipeline code done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: a pipeline with `run_as.group_name`
# MAGIC Requires a full (Premium) workspace.

# COMMAND ----------

pipeline = {
    "name": "dataistheway_etl",
    "catalog": catalog,
    "schema": schema,
    "serverless": True,
    "libraries": [{"file": {"path": PIPE_DIR + "/mv_suma_kraj.sql"}}],
    "run_as": {"group_name": GROUP},
}
pid = api.do("POST", "/api/2.0/pipelines", body=pipeline)["pipeline_id"]
print("run_as_user_name:", api.do("GET", f"/api/2.0/pipelines/{pid}")["run_as_user_name"])

def run_update():
    t0 = time.time()
    uid = api.do("POST", f"/api/2.0/pipelines/{pid}/updates", body={})["update_id"]
    while True:
        st = api.do("GET", f"/api/2.0/pipelines/{pid}/updates/{uid}")["update"]["state"]
        if st in ("COMPLETED", "FAILED", "CANCELED"):
            return f"{st} ({time.time() - t0:.0f} s)"
        time.sleep(10)

print("update as the group:", run_update())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: who owns the tables
# MAGIC Requires a full (Premium) workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, table_type, table_owner
# MAGIC FROM information_schema.tables
# MAGIC WHERE table_schema = current_schema() AND table_name IN ('mv_suma_kraj', 'zrodlo_zamowienia')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: the built-in `users` group as run-as
# MAGIC Requires a full (Premium) workspace. The cell prints the error.

# COMMAND ----------

try:
    api.do("POST", "/api/2.0/pipelines", body={**pipeline, "name": "dataistheway_users", "run_as": {"group_name": "users"}})
    print("accepted")
except Exception as e:
    print(str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: back to a user: `updated_run_as` or `run_as`?
# MAGIC Requires a full (Premium) workspace. The docs say to use `updated_run_as` for edits.

# COMMAND ----------

spec = api.do("GET", f"/api/2.0/pipelines/{pid}")["spec"]
try:
    api.do("PUT", f"/api/2.0/pipelines/{pid}", body={**spec, "updated_run_as": {"user_name": me.user_name}})
    print("updated_run_as accepted")
except Exception as e:
    print("updated_run_as:", str(e).splitlines()[0])

api.do("PUT", f"/api/2.0/pipelines/{pid}", body={**spec, "run_as": {"user_name": me.user_name}})
print("run_as_user_name:", api.do("GET", f"/api/2.0/pipelines/{pid}")["run_as_user_name"])

# COMMAND ----------

# MAGIC %sql
# MAGIC -- owner right after the settings change, without a new update
# MAGIC SELECT table_name, table_owner
# MAGIC FROM information_schema.tables
# MAGIC WHERE table_schema = current_schema() AND table_name = 'mv_suma_kraj'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: `SET OWNER TO` the group on a pipeline table
# MAGIC Requires a full (Premium) workspace. The cell prints the error.

# COMMAND ----------

try:
    spark.sql(f"ALTER MATERIALIZED VIEW mv_suma_kraj SET OWNER TO `{GROUP}`")
    print("accepted")
except Exception as e:
    print(str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

api.do("DELETE", f"/api/2.0/pipelines/{pid}", query={"cascade": "true"})
spark.sql("DROP TABLE IF EXISTS zrodlo_zamowienia")
spark.sql(f"REVOKE ALL PRIVILEGES ON SCHEMA {catalog}.{schema} FROM `{GROUP}`")
spark.sql(f"REVOKE USE CATALOG ON CATALOG {catalog} FROM `{GROUP}`")
api.do("DELETE", f"/api/2.0/account/scim/v2/Groups/{gid}")
print("cleaned up")
