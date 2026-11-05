# Moving a Structured Streaming job into a pipeline with import_checkpoint (Beta)

Code for the article https://dataistheway.blog/en/lakeflow-import-checkpoint-migration/.

One notebook, `import_checkpoint.py`. A plain Structured Streaming query deduplicates orders by `id` and writes `ckpt_orders` with a checkpoint in a volume. A Lakeflow pipeline then takes over the checkpoint and the same table with `dp.create_table` + `@dp.append_flow(import_checkpoint=...)`: a repeated `id` is not added again, so the deduplication state came along. The last step deletes the old rows from the source (like a Kafka topic with short retention) and runs a full refresh, which starts from an empty checkpoint, so the old rows disappear from the table.

**Runs on:** a full (Premium) workspace with Unity Catalog and serverless; `create_table` and `import_checkpoint` are in Beta. I ran it as an admin, so I did not work out the minimum set of privileges. It creates tables, a volume, workspace files and a pipeline, so it needs at least those rights; the pipeline's run-as identity has to read the old job's checkpoint, so that is the first permission to check. Step 1 uses no pipeline; according to the docs it works on Free Edition, I have not tested it.

## How to run

1. Import `import_checkpoint.py` into your workspace (**Workspace -> Import -> File**, or `databricks workspace import`).
2. Attach it to Serverless compute.
3. Set the widgets `catalog` and `schema`. The schema is created if it does not exist.
4. Run all cells. The last cell deletes the pipeline with `cascade=true`, the tables, the checkpoint volume and the pipeline source file.
5. If a run stops halfway, delete the `news104_ckpt` pipeline (`databricks pipelines delete <id> --cascade`), then run the Cleanup cell, because the names must be free.

Each pipeline update takes about a minute, so the notebook takes about 4 minutes.
