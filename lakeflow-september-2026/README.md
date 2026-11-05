# Lakeflow in September 2026: deleting a pipeline, orphaned tables and maintenance windows

Code for the article https://dataistheway.blog/en/lakeflow-september-2026-changes/.

Three small notebooks, one per change. Each one is independent: it creates what it needs, shows one thing and cleans up after itself. They create pipelines and jobs through the REST API and write the pipeline source files themselves (to `/Workspace/Users/<your-login>/dataistheway/news104_src`), so the notebook is the only file to import.

| Notebook | What it shows |
|---|---|
| `1_delete_pipeline.py` | Deleting a pipeline the default way keeps its streaming table and materialized view; they can be read, but new data no longer reaches them. `cascade=true` deletes them. |
| `2_orphaned_tables.py` | Finding orphaned tables (their `pipeline_id` points at a pipeline that no longer exists), what happens when the same pipeline is deployed again (it fails: the tables are still managed by the deleted pipeline) and moving the tables to the new pipeline with `pipelines.pipelineId`. |
| `3_maintenance_window.py` | A PAUSED continuous job that runs a pipeline, with a `maintenance_window`, and two settings the API rejects. Nothing runs continuously. |

**Runs on:** a full (Premium) workspace with Unity Catalog and serverless. I ran it as an admin, so I did not work out the minimum set of privileges. The docs require `USE CATALOG` on the catalog, `USE SCHEMA` + `CREATE TABLE` and `USE SCHEMA` + `CREATE MATERIALIZED VIEW` on the schema, and `CREATE SCHEMA` if the schema has to be created; on top of that the notebooks need permission to write workspace files and to create pipelines and jobs. Notebook 1 runs two pipelines at once and Free Edition allows one active pipeline per type (`free-edition-limitations`, checked 2026-11-03).

## How to run

1. Import a notebook into your workspace (**Workspace → Import → File**, or `databricks workspace import`).
2. Attach it to Serverless compute.
3. Set the widgets `catalog` and `schema`. The schema is created if it does not exist.
4. Run all cells. The last cell removes what the notebook created. The schema, the parent `dataistheway` folder and internal `__materialization_mat_*` tables may stay; drop them yourself if needed.
5. If a run stops halfway, delete the leftover `news104_*` pipelines (`databricks pipelines delete <id> --cascade`) and the `news104_mw_job` job by hand, then run the Cleanup cell, because the names must be free.

Each pipeline update takes about a minute, so notebooks 1 and 2 take 3-5 minutes each.
