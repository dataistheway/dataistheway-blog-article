# DBFS is gone from new workspaces

Code for the article https://dataistheway.blog/en/dbfs-september-30-2026/.

Shows what a workspace without legacy features does with old DBFS root and Hive Metastore paths, and the Unity Catalog volume as the replacement: create a volume, write a small CSV into it, read it back with a plain `/Volumes/...` path, load it into a Delta table and read it with pandas.

**Runs on:** Free Edition (serverless) or any Unity Catalog workspace.

Tested on Azure Databricks Premium (serverless, a workspace created without legacy features) and on Free Edition (2026-10-17, serverless job, `catalog=workspace`): the whole notebook passed with the same results on both.

## How to run

1. Import `dbfs_volumes.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`, ...). The schema is created if it does not exist.
4. Run all cells. The last cell drops the table and the volume (the schema stays).

## What each step does

- **Step 1:** reads `dbfs:/FileStore/...` and lists `hive_metastore`. On a workspace without legacy features both print an error (`DBFS_DISABLED`, `UC_HIVE_METASTORE_DISABLED_EXCEPTION`); the cell prints the error instead of stopping.
- **Step 2:** create the `news01_landing` volume.
- **Data preparation:** write a 5-row `stores.csv` into the volume with `dbutils.fs.put`.
- **Step 3:** list the volume and read the file with a plain path.
- **Step 4:** the volume as a Unity Catalog object (`SHOW VOLUMES`, `DESCRIBE VOLUME`).
- **Step 5:** load the file into the Delta table `news01_stores` (`CREATE OR REPLACE TABLE ... AS SELECT * FROM read_files(...)`).
- **Step 6:** pandas with `/dbfs/FileStore/...` (`FileNotFoundError` on a workspace without DBFS root) and with `/Volumes/...` (5 rows).
- **Cleanup:** drops the table and the volume (the schema stays).
