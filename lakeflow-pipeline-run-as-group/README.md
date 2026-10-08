# Lakeflow: a pipeline that doesn't belong to one person

Code for the article https://dataistheway.blog/en/lakeflow-pipeline-run-as-group/.

Creates an account group and a serverless pipeline with `run_as.group_name`, then checks who owns the published table and what happens when run-as changes. `pipeline/mv_total_by_country.sql` is the pipeline code (one materialized view).

**Runs on:** a full (Premium) workspace with Unity Catalog and serverless; run it as a workspace admin, because it creates an account group.

## How to run

1. Import the whole folder into your workspace (for example `databricks workspace import-dir <folder> /Workspace/Users/<your-login>/<folder>`), so `pipeline_run_as_group.py` and its subfolder sit next to each other.
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`, ...). The schema is created if it does not exist.
4. Run all cells. The last cell removes everything the notebook created.

## What each step does

- **Step 1:** create an account group with us as a member and assign it to the workspace.
- **Step 2:** grant the group everything the pipeline needs, including **Can read** on the pipeline code.
- **Step 3:** create the pipeline with `run_as.group_name` and run an update.
- **Step 4:** the materialized view is owned by the group.
- **Step 5:** the built-in `users` group is rejected as run-as.
- **Step 6:** switch back to a user: `updated_run_as` is rejected, `run_as` works, and the table owner changes right away.
- **Step 7:** `SET OWNER TO` on a pipeline table fails.
- **Cleanup:** deletes the pipeline, table, grants and the group.

## Notes

Import the whole folder (the notebook and `pipeline/`). The group name is a widget: if a group with that name is left over from an earlier run, change it or delete the old group in the account console.
