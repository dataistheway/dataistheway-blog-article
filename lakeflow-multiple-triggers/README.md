# Lakeflow Jobs: multiple triggers on one job

Code for the article https://dataistheway.blog/en/lakeflow-multiple-triggers/.

Three small notebooks, each with its own setup and cleanup. They call the Jobs REST API from a notebook on serverless and create a job named `news134_multiple_triggers`. `task/` holds the notebooks the job runs.

**Runs on:** a workspace with serverless and the **Multiple Triggers** preview enabled (workspace Previews page). Recorded on a Premium workspace.

| Notebook | What it shows | Time |
|---|---|---|
| `01_which_trigger.py` | Two file arrival triggers: `{{job.trigger.file_arrival.location}}` tells which folder started the run. Adding a table update trigger to the same job is rejected while the task uses that value. | ~6 min |
| `02_update_triggers.py` | Resuming one trigger the safe way (read the array, change it, send it back) and the wrong way (one element replaces the whole array); the legacy `schedule` field is rejected. No job runs. | ~1 min |
| `03_freshness_check.py` | A table update trigger plus a backup schedule. The task (`task/check_freshness.py`) fails a run started by the schedule when the newest row is older than the SLA; a run started by a table change skips the check. | ~4 min |

## How to run

1. Import the whole folder into your workspace (for example `databricks workspace import-dir <folder> /Workspace/Users/<your-login>/<folder>`), so the notebooks and `task/` sit next to each other.
2. Attach a notebook to Serverless compute.
3. Set the widgets `catalog` and `schema` (default schema `news_134`). The schema is created if it does not exist.
4. Run all cells. The last cell deletes the job, tables and volume (the schema stays).

## Notes

- `01` and `03` wait 90 s before the first change: in my lab a change right after `jobs/create` or `jobs/update` did not start a run.
- `03` creates a schedule that fires every minute, only for the demo, and pauses it after the first run. If the notebook stops before the last cell, delete the `news134_multiple_triggers` job by hand.

## Bundle version

`bundle/` holds the layout from the article as a Declarative Automation Bundle: the base job has no trigger, the `dev` target (`mode: development`) adds the `triggers` array (table update and a paused backup schedule), and the `prod` target adds a plain `schedule`. Tested with Databricks CLI 1.20.0. The dev target needs the Multiple Triggers preview; create the two tables first (below).

```sql
CREATE TABLE <catalog>.<schema>.source_orders (id INT, country STRING, amount DECIMAL(10,2));
CREATE TABLE <catalog>.<schema>.trigger_log (ts TIMESTAMP, trigger_type STRING);
```

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
