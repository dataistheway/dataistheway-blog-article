# DBFS is gone from new workspaces

Code for the article https://dataistheway.blog/en/dbfs-september-30-2026/.

Shows the Unity Catalog volume as the replacement for DBFS root and mounts: create a volume, write a small CSV into it and read it back with a plain `/Volumes/...` path.

**Runs on:** Free Edition (serverless) or any Unity Catalog workspace.

## How to run

1. Import `dbfs_volumes.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`, ...). The schema is created if it does not exist.
4. Run all cells. The last cell removes everything the notebook created.

## What each step does

- **Step 1:** create the `landing` volume.
- **Data preparation:** write a 5-row `stores.csv` into the volume with `dbutils.fs.put`.
- **Step 2:** list the volume and read the file with a plain path.
- **Step 3:** the volume as a Unity Catalog object (`SHOW VOLUMES`, `DESCRIBE VOLUME`).
- **Extra:** checks whether pandas can read `/Volumes/...` directly on your compute (not in the article; the cell prints the error instead of stopping).
- **Cleanup:** drops what the notebook created.
