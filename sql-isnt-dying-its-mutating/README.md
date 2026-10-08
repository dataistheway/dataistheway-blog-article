# SQL isn't dying, it's mutating

Code for the article https://dataistheway.blog/en/sql-isnt-dying-its-mutating/.

The article is an opinion piece with two code blocks; the notebook runs both on a small synthetic `employees` table.

**Runs on:** Free Edition; step 2 needs an existing AI Search (formerly Vector Search) index with managed embeddings.

## How to run

1. Import `sql_mutating.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`, ...). The schema is created if it does not exist.
4. Run all cells. The last cell removes everything the notebook created.

## What each step does

- **Step 1:** a declarative `SELECT`: we say what, not how.
- **Step 2:** semantic search with `vector_search()` used as a function in `FROM`; set `RUN_VECTOR_SEARCH = true` and the `vs_index` widget, otherwise the step is skipped.
- **Cleanup:** drops the table.
