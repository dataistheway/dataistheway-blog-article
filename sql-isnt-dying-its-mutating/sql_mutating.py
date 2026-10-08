# Databricks notebook source
# MAGIC %md
# MAGIC # SQL isn't dying, it's mutating
# MAGIC
# MAGIC Code for the article https://dataistheway.blog/en/sql-isnt-dying-its-mutating/.
# MAGIC
# MAGIC The notebook follows the order of the article. The article is an opinion piece
# MAGIC with only two code blocks: a declarative `SELECT` and `vector_search()`.
# MAGIC
# MAGIC **Input:** synthetic data generated in the notebook (table `employees`, a few rows, no personal data).
# MAGIC Step 2 requires an existing AI Search index (formerly Vector Search) with managed embeddings.
# MAGIC
# MAGIC **Name mapping** (article -> notebook):
# MAGIC - `employees` -> `employees` in `<catalog>.<schema>`
# MAGIC - `<your_catalog>.<schema>.<index>` -> widget `vs_index` (empty = `<catalog>.<schema>.coffee_index`)
# MAGIC
# MAGIC **Switch:** `RUN_VECTOR_SEARCH` (default `false`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dataistheway")
dbutils.widgets.text("vs_index", "")
dbutils.widgets.dropdown("RUN_VECTOR_SEARCH", "false", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
vs_index = dbutils.widgets.get("vs_index") or f"{catalog}.{schema}.coffee_index"
RUN_VECTOR_SEARCH = dbutils.widgets.get("RUN_VECTOR_SEARCH") == "true"

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
# MAGIC Works on Free Edition (on any table with columns `name` and `salary`)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, salary
# MAGIC FROM employees
# MAGIC WHERE salary > 5000
# MAGIC ORDER BY salary DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: semantic search as a function in `FROM`
# MAGIC Works on Free Edition (requires an AI Search endpoint and index; Free Edition is limited to one endpoint)
# MAGIC
# MAGIC To prepare: an AI Search endpoint and an index with managed embeddings on a table with text
# MAGIC (for example, short coffee tips). Put the full index name in the `vs_index` widget and set `RUN_VECTOR_SEARCH = true`.
# MAGIC Check the argument names (`query_text`) in the documentation on the day you run it.

# COMMAND ----------

if RUN_VECTOR_SEARCH:
    display(spark.sql(f"""
    SELECT *
    FROM vector_search(
      index => '{vs_index}',
      query_text => 'how to brew coffee properly',
      num_results => 3
    )
    """))
else:
    print("Skipped: RUN_VECTOR_SEARCH = false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC The notebook does not create the index or the endpoint, so it does not delete them.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS employees")
