# Lakeflow: a pipeline that doesn't belong to one person

Code for the article https://dataistheway.blog/en/lakeflow-pipeline-run-as-group/.

Creates an account group and a serverless pipeline with `run_as.group_name`, then checks who owns the published table and what happens when run-as changes. `pipeline/mv_total_by_country.sql` is the pipeline code (one materialized view).

**Runs on:** a workspace with Unity Catalog and serverless (tested on Azure Databricks Premium, 2026-10-19, and on Free Edition, 2026-10-17); run it as a workspace admin, because it creates an account group.

Run it only in a sandbox account or workspace. The notebook creates and deletes an account-level group, which is visible in every workspace of the account. Pick a unique `group_name`.

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

Import the whole folder (the notebook and `pipeline/`). On Free Edition step 3 failed once right after step 1 with "The user creating or updating the pipeline must be a member of the group", although we were a member; the same call went through a few minutes later, so rerun step 3 if you see it. The group name is a widget: if a group with that name is left over from an earlier run, change it or delete the old group in the account console.

## Bundle version (`bundle/`)

The same pipeline deployed with a Declarative Automation Bundle (formerly Databricks Asset Bundle). Tested on Databricks CLI v1.20.0 on Azure Databricks Premium and on Free Edition, 2026-10-19. `group_name` has been in the bundle schema since v1.18.0, but a deploy on v1.18 or v1.19 was not tested. CLI v1.16.1 drops `group_name` from `run_as` and the deploy fails with `run_as object must have user_name, service_principal_name or group_name fields filled`.

- `databricks.yml` sets `run_as.group_name` on the pipeline resource and gives the group `CAN_VIEW` through the top-level `permissions`. There is no `workspace.root_path`: the files stay in the home folder of whoever deploys, and the CLI grants the group Can read on that folder. Under `/Workspace/Shared/` the folder is writable by all workspace users.
- `src/mv_total_by_country.sql` is the pipeline code (the same file as `pipeline/`).

Before the deploy:

1. The account group exists, is assigned to the workspace and has the `workspace-access` entitlement.
2. The identity that deploys is a member of the group (or has Assume on it).
3. The group has `USE CATALOG` on the catalog and `USE SCHEMA`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW` and `SELECT` on the schema (`MODIFY` was not needed for the materialized view).
4. `source_orders` exists in the schema (the first cell of the notebook creates it).

The bundle variables go in as environment variables (`BUNDLE_VAR_<name>`):

```bash
cd bundle
export BUNDLE_VAR_catalog=<catalog> BUNDLE_VAR_schema=<schema> BUNDLE_VAR_group_name=<group>
databricks bundle validate
databricks bundle deploy
databricks bundle run etl
databricks bundle destroy
```

After the deploy, check that `run_as_user_name` of the pipeline is the group: a run-as changed through the API is not reverted by the next `bundle deploy` unless the pipeline definition changes. `bundle destroy` deletes the pipeline but leaves `mv_total_by_country` in the schema; drop it before deploying the bundle again, otherwise the first update fails with `is already managed by pipeline <id>`.
