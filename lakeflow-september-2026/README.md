# Lakeflow in September 2026: deleting a pipeline, maintenance windows and import_checkpoint

Code for the article https://dataistheway.blog/en/lakeflow-september-2026-changes/.

Creates three small serverless pipelines through the Pipelines REST API and deletes them in two ways: `news104_a` the default way (its tables stay) and `news104_b` with `cascade=true` (its tables go). Then `news104_c` takes over the checkpoint of a plain Structured Streaming query with `import_checkpoint` (Beta), and a PAUSED continuous job gets a maintenance window. The notebook writes the pipeline source files itself (to `/Workspace/Users/<your-login>/dataistheway/news104_src`), so the notebook is the only file to import.

**Runs on:** a full (Premium) workspace with Unity Catalog and serverless. Without admin rights you need `USE CATALOG`, permission to create a schema (or tables, materialized views and a volume in an existing one), and permission to create pipelines and jobs. Step 5 alone also works on Free Edition.

## How to run

1. Import `lakeflow_september_2026.py` into your workspace (**Workspace → Import → File**, or `databricks workspace import`).
2. Attach it to Serverless compute.
3. Set the widgets `catalog` and `schema`. The schema is created if it does not exist.
4. Run all cells. The last cell removes the job, the pipelines, the tables, the volume and the pipeline source files. The schema, the parent `dataistheway` folder and internal `__materialization_mat_*` tables may stay; drop them yourself if needed.

## What each step does

- **Setup:** a source table `source_orders` (5 rows), a volume for the checkpoint and two small helpers: create a pipeline and wait for its update.
- **Step 1:** pipelines `news104_a` and `news104_b`, each with a streaming table (`a_orders`, `b_orders`) and a materialized view (`a_total_by_country`, `b_total_by_country`), and the `effective_publishing_mode`.
- **Step 2:** delete `news104_a` without `cascade`: the pipeline is gone, its tables stay and can still be read.
- **Step 3:** try `REFRESH` on the orphaned tables and print the result or the error.
- **Step 4:** delete `news104_b` with `cascade=true`: its tables are gone.
- **Step 5:** a plain Structured Streaming query (`availableNow`) writes `ss_orders` with a checkpoint in the volume, then 3 new rows arrive in the source.
- **Step 6:** pipeline `news104_c` with `dp.create_table` and `@dp.append_flow(import_checkpoint=...)`: `c_orders` gets only the 3 new rows.
- **Step 7:** a job with a `continuous` trigger in `PAUSED` state and a `maintenance_window`, read back with `jobs/get`.
- **Cleanup:** deletes the job, pipeline C with `cascade=true`, the remaining tables, the volume and the pipeline source files.

## Notes

`REFRESH` in step 3 depends on the workspace preview "MV and ST in Serverless Notebooks and Jobs": without it serverless notebook compute rejects it with `MV_NOT_ENABLED_ON_SERVERLESS_GENERIC_COMPUTE`. The table list is filtered to this notebook's objects, plus the pipelines' `event_log_*` and internal `__materialization_*` tables, so other pipelines writing to the same schema may show up there too. Each pipeline update takes about a minute, so the whole run takes a few minutes.
