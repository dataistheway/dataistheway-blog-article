# SQL isn't dying, it's mutating

Code for the article https://dataistheway.blog/en/sql-isnt-dying-its-mutating/.

The article is an opinion piece with two code blocks; the notebook runs both on small synthetic tables.

**Tested on:** our Azure Databricks Premium (trial) workspace and on Free Edition (2026-10-17), same results.

**Creates:** tables `employees` and `coffee_tips`, an AI Search (formerly Vector Search) endpoint `sql_mutating_search` and a Delta Sync index `coffee_tips_index` with managed embeddings (`databricks-gte-large-en`). The last cell deletes all of them. The endpoint is billed for as long as it runs, so let the notebook finish or delete it yourself. Free Edition allows one AI Search endpoint. If you already have one, either delete it first, or set `ENDPOINT` to it, skip the `create_endpoint_and_wait` call and remove the endpoint `DELETE` line from the last cell, so the notebook does not delete your endpoint.

## How to run

1. Import `sql_mutating.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets `catalog` and `schema`. The schema is created if it does not exist. Use a dedicated schema: the notebook replaces and drops its tables.
4. Run all cells. Creating the endpoint and syncing the index took about 20 minutes in our runs (trial and Free Edition).

## Pitfalls we hit

- **Waiting for the index.** In our run the index waited about 20 minutes for the endpoint ("Delta sync index creation is pending endpoint provisioning"), even though the endpoint already reported `ONLINE`.
- **Running it again right away.** Deleting the endpoint takes a while (about 15 minutes in our run). Until it is gone, creating an endpoint with the same name fails with `AlreadyExists`. Wait, or change `ENDPOINT`.
- **Old SDK on serverless.** The `databricks-sdk` version on serverless did not accept `embedding_model_endpoint_name` in `EmbeddingSourceColumn` and had a different `delete_endpoint()` signature, so the notebook creates and deletes the index through the REST API (`w.api_client.do`), with the same JSON as in the docs.

## What each step does

- **Step 1:** a declarative `SELECT`: we say what, not how.
- **Data for step 2:** eight short coffee and tea tips, with Change Data Feed on (required by a Delta Sync index).
- **Endpoint and index:** the notebook creates the endpoint and the index and waits until the index is ready.
- **Step 2:** semantic search with `vector_search()` used as a function in `FROM`.
- **Cleanup:** deletes the index and the endpoint and drops both tables.
