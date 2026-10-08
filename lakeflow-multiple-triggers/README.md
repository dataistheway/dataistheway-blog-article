# Lakeflow Jobs: multiple triggers on one job

Code for the article https://dataistheway.blog/en/lakeflow-multiple-triggers/.

Creates a job with three triggers through the Jobs REST API (table update, file arrival and a paused schedule as a backup) and shows which trigger started each run. `task/log_trigger.py` is the notebook the job runs: it logs `{{job.trigger.type}}` to a table.

**Runs on:** a full (Premium) workspace with serverless and the **Multiple Triggers** preview enabled (workspace Previews page).

## How to run

1. Import the whole folder into your workspace (for example `databricks workspace import-dir <folder> /Workspace/Users/<your-login>/<folder>`), so `lakeflow_multiple_triggers.py` and its subfolder sit next to each other.
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`, ...). The schema is created if it does not exist.
4. Run all cells. The last cell removes everything the notebook created.

## What each step does

- **Step 0:** a source table, a log table and a volume for incoming files.
- **Step 1:** create the job with a `triggers` array.
- **Step 2:** read the job back: the server adds `condition: ALL_UPDATED` and an explicit `pause_status`.
- **Step 3:** insert rows and wait for a run with `trigger = TABLE`.
- **Step 4:** drop a file into the volume and wait for a run with `trigger = FILE_ARRIVAL`, then show what the task logged.
- **Step 5:** send `jobs/update` with one trigger and see that it replaces the whole array.
- **Step 6:** the legacy `schedule` field is rejected on a job that uses `triggers`.
- **Cleanup:** deletes the job, tables and volume.

## Notes

Import the whole folder (the notebook and `task/`), because the job runs `task/log_trigger` from the path next to the notebook. Step 3 waits 90 s before the insert, so the run takes about 4 minutes.
