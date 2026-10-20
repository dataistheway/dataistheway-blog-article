# Databricks notebook source
# MAGIC %md
# MAGIC # SQL isn't dying, it's mutating
# MAGIC
# MAGIC Code for the article https://dataistheway.blog/en/sql-isnt-dying-its-mutating/.
# MAGIC
# MAGIC Two examples from the article: a declarative `SELECT` and semantic search with `vector_search()`.
# MAGIC
# MAGIC **Input:** synthetic data generated in the notebook (tables `employees` and `coffee_tips`, no personal data).
# MAGIC The notebook creates an AI Search (formerly Vector Search) endpoint and an index, and deletes them at the end.
# MAGIC The endpoint is billed for as long as it runs.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dataistheway")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ENDPOINT = "sql_mutating_search"
INDEX = f"{catalog}.{schema}.coffee_tips_index"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data preparation
# MAGIC A synthetic `employees` table with columns `name` and `salary`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employees AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('Employee A', 4200),
# MAGIC   ('Employee B', 5100),
# MAGIC   ('Employee C', 7300),
# MAGIC   ('Employee D', 4999),
# MAGIC   ('Employee E', 6250)
# MAGIC AS t(name, salary);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: we say "what", not "how"
# MAGIC Only employees earning more than 5000, highest salary first. Plain SQL, no AI Search needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, salary
# MAGIC FROM employees
# MAGIC WHERE salary > 5000
# MAGIC ORDER BY salary DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data preparation for step 2
# MAGIC Eight short tips. Note that few of them contain the words "brew" or "properly".
# MAGIC Change Data Feed is required by a Delta Sync index.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE coffee_tips (id INT NOT NULL, tip STRING)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC INSERT INTO coffee_tips VALUES
# MAGIC   (1, 'Grind the beans just before making an espresso, not the day before.'),
# MAGIC   (2, 'For a pour-over, use water at 92-96 degrees, not straight off the boil.'),
# MAGIC   (3, 'A French press needs a coarse grind and about four minutes of steeping.'),
# MAGIC   (4, 'Green tea tastes bitter if the water is hotter than 80 degrees.'),
# MAGIC   (5, 'Descale the kettle once a month if you have hard water.'),
# MAGIC   (6, 'Keep coffee beans in an airtight container, away from light.'),
# MAGIC   (7, 'Froth milk for a cappuccino to about 60 degrees.'),
# MAGIC   (8, 'Weigh the coffee: about 60 grams per litre of water is a good start.');

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Search endpoint and index
# MAGIC A Delta Sync index with managed embeddings: Databricks computes the vectors with `databricks-gte-large-en`.
# MAGIC Creating the endpoint takes a few minutes.

# COMMAND ----------

import time
from datetime import timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType

w = WorkspaceClient()
w.vector_search_endpoints.create_endpoint_and_wait(
    name=ENDPOINT, endpoint_type=EndpointType.STANDARD, timeout=timedelta(minutes=30))

w.api_client.do("POST", "/api/2.0/vector-search/indexes", body={
    "name": INDEX,
    "endpoint_name": ENDPOINT,
    "primary_key": "id",
    "index_type": "DELTA_SYNC",
    "delta_sync_index_spec": {
        "source_table": f"{catalog}.{schema}.coffee_tips",
        "pipeline_type": "TRIGGERED",
        "embedding_source_columns": [
            {"name": "tip", "embedding_model_endpoint_name": "databricks-gte-large-en"}],
    },
})

while not w.vector_search_indexes.get_index(INDEX).status.ready:
    time.sleep(20)
print("Index ready:", INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: semantic search as a function in `FROM`
# MAGIC The question is plain text. The embedding endpoint turns it into a vector on every query.
# MAGIC `vector_search()` is in Public Preview, needs DBR 15.3+ and does not run on a classic SQL warehouse.
# MAGIC Free Edition allows one AI Search endpoint per workspace.

# COMMAND ----------

display(spark.sql(f"""
SELECT *
FROM vector_search(
  index => '{INDEX}',
  query_text => 'how to brew coffee properly',
  num_results => 3
)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC We delete the index and the endpoint (it is billed while it runs), then drop the tables.

# COMMAND ----------

w.api_client.do("DELETE", f"/api/2.0/vector-search/indexes/{INDEX}")
w.api_client.do("DELETE", f"/api/2.0/vector-search/endpoints/{ENDPOINT}")
spark.sql("DROP TABLE IF EXISTS coffee_tips")
spark.sql("DROP TABLE IF EXISTS employees")
