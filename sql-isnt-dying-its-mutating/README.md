# SQL isn't dying, it's mutating

Code for the article https://dataistheway.blog/en/sql-isnt-dying-its-mutating/.

The article is an opinion piece with two code blocks; the notebook runs both on a small synthetic `employees` table.

**Runs on:** Free Edition; step 2 needs an existing AI Search (formerly Vector Search) Delta Sync index with managed embeddings and `SELECT` on it. Step 2 was not run in the lab recording. To create an endpoint and an index, see https://learn.microsoft.com/azure/databricks/ai-search/create-ai-search (the notebook does not create them).

## How to run

1. Import `sql_mutating.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`, ...). The schema is created if it does not exist. Use a dedicated schema: the notebook replaces and drops `employees`.
4. Run all cells. The last cell drops the `employees` table. The schema stays.

## What each step does

- **Step 1:** a declarative `SELECT`: we say what, not how.
- **Step 2:** semantic search with `vector_search()` used as a function in `FROM`; set `RUN_VECTOR_SEARCH = true` and the `vs_index` widget, otherwise the step is skipped.
- **Cleanup:** drops the table.
