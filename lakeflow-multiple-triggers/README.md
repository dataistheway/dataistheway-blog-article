# Lakeflow Jobs: multiple triggers on one job

Code for the article https://dataistheway.blog/en/lakeflow-multiple-triggers/.

Creates a job with three triggers through the Jobs REST API (table update, file arrival and a paused schedule as a backup) and shows which trigger started each run. `task/log_trigger.py` is the notebook the job runs: it logs `{{job.trigger.type}}` to a table.

**Runs on:** a full (Premium) workspace with serverless and the **Multiple Triggers** preview enabled (workspace Previews page).

## How to run

1. Import the whole folder into your workspace (for example `databricks workspace import-dir <folder> /Workspace/Users/<your-login>/<folder>`), so `lakeflow_multiple_triggers.py` and its subfolder sit next to each other.
2. Attach it to Serverless compute.
3. Set the widgets `catalog` and `schema`. The schema is created if it does not exist.
4. Run all cells. The last cell removes everything the notebook created.

## What each step does

- **Step 0:** a source table, a log table and a volume for incoming files.
- **Step 1:** create the job with a `triggers` array.
- **Step 2:** read the job back: the server adds `condition: ALL_UPDATED` and an explicit `pause_status`; the cell also prints the job's `queue` setting.
- **Step 3:** insert rows and wait for a run with `trigger = TABLE`.
- **Step 4:** drop a file into the volume and wait for a run with `trigger = FILE_ARRIVAL`, then show what the task logged.
- **Step 5:** send `jobs/update` with one trigger and see that it replaces the whole array.
- **Step 6:** the legacy `schedule` field is rejected on a job that uses `triggers`.
- **Cleanup:** deletes the job, tables and volume.

## Notes

Import the whole folder (the notebook and `task/`), because the job runs `task/log_trigger` from the path next to the notebook. Step 3 waits 90 s before the insert, so the run takes about 4 minutes.

If the notebook stops before the last cell, delete the `news134_multiple_triggers` job by hand: it keeps active triggers (table update and file arrival until step 5, and after step 5 only an unpaused daily schedule).

## Bundle version

`bundle/` holds the layout from the article as a Declarative Automation Bundle: the base job has no trigger, the `dev` target (`mode: development`) adds the `triggers` array (table update and a paused backup schedule), and the `prod` target adds a plain `schedule`. Tested with Databricks CLI 1.20.0. The dev target needs the Multiple Triggers preview; the tables come from the notebook's step 0 (or create `source_orders` and `trigger_log` yourself).

```sh
cd bundle
databricks bundle validate --strict --var catalog=<catalog> --var schema=<schema>
databricks bundle deploy --var catalog=<catalog> --var schema=<schema>            # dev
databricks bundle deploy -t prod --var catalog=<catalog> --var schema=<schema>
databricks bundle destroy --var catalog=<catalog> --var schema=<schema>           # clean up dev
databricks bundle destroy -t prod --var catalog=<catalog> --var schema=<schema>   # clean up prod
```

The `prod` target deploys an unpaused daily schedule (06:00 Europe/Warsaw), so destroy it after the test.

Pin the CLI version you tested (I deployed this bundle with 1.20.0). CLI 1.12.1 only warns about the unknown `triggers` field and deploys the job without triggers, and per the CLI changelog only 1.15.0 fixed a job with a `table_update` trigger that never converged on the direct engine.
