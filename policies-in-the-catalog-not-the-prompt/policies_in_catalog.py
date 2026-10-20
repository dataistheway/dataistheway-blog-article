# Databricks notebook source
# MAGIC %md
# MAGIC # Policies go to the catalog, not the prompt
# MAGIC
# MAGIC Code for the article https://dataistheway.blog/en/policies-in-the-catalog-not-the-prompt/.
# MAGIC
# MAGIC The code follows the order of the article: row filter, column mask, least privilege for the agent and removing the policies.
# MAGIC
# MAGIC **Input:** synthetic data generated in the notebook: `gold_customer_360` (400 customers in the states CA, NY, TX,
# MAGIC FL, WA, IL, some with a `tax_id`, 8 customers with a duplicated row), no personal data.
# MAGIC
# MAGIC **Name mapping** (article -> notebook, everything in `<catalog>.<schema>`):
# MAGIC - `<your_catalog>.<schema>.gold_customer_360` -> `gold_customer_360` (synthetic)
# MAGIC - `<your_catalog>.<schema>.retail_row_filter` / `mask_tax_id` -> the same names, unqualified
# MAGIC - `get_average_customer_value`, `get_customer_profile` -> simplified agent tool functions created in the notebook
# MAGIC - `<agent-sp>` -> widget `principal` (default `account users`, so the step runs on every workspace;
# MAGIC   in production put the application ID of the agent's service principal here)
# MAGIC
# MAGIC The groups `all_states_analysts` and `compliance_officers` stay as in the article. If they do not exist in your account,
# MAGIC the filter and the mask apply to you as well, which is the point of this exercise.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dataistheway")
dbutils.widgets.text("principal", "account users")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
principal = dbutils.widgets.get("principal")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
GOLD_TABLE = f"{catalog}.{schema}.gold_customer_360"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data preparation
# MAGIC Synthetic `gold_customer_360` and two agent tool functions (no `tax_id` in the result).

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_customer_360 AS
WITH base AS (
  SELECT
    CAST(100000 + id AS BIGINT) AS customer_id,
    CONCAT('Customer ', CAST(100000 + id AS STRING)) AS customer_name,
    CASE WHEN id % 5 = 0 THEN NULL
         ELSE CONCAT(CAST(10 + id % 89 AS STRING), '-', LPAD(CAST((id * 7919) % 10000000 AS STRING), 7, '0'))
    END AS tax_id,
    element_at(array('CA', 'NY', 'TX', 'FL', 'WA', 'IL', 'CA'), CAST(id % 7 + 1 AS INT)) AS state,
    CAST(id % 4 AS BIGINT) AS loyalty_segment,
    ROUND((id % 4) * 340.0 + (id % 13) * 17.5 + 25.0, 2) AS monetary,
    CAST((id % 4) * 3 + id % 3 + 1 AS BIGINT) AS num_orders
  FROM range(1, 401)
)
SELECT * FROM base
UNION ALL
SELECT * FROM base WHERE customer_id % 50 = 3   -- 8 customers with a duplicated row
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION get_average_customer_value(
  segment BIGINT COMMENT 'Loyalty segment ID (0=new, 1=occasional, 2=regular, 3=VIP). Pass -1 for all segments.'
)
RETURNS DOUBLE
COMMENT 'Returns the average monetary value (USD) of customers in the given loyalty segment.'
RETURN SELECT ROUND(AVG(monetary), 2)
FROM (SELECT DISTINCT customer_id, monetary, loyalty_segment FROM {GOLD_TABLE})
WHERE (segment = -1 OR loyalty_segment = segment)
""")

PROFILE_DDL = f"""
CREATE OR REPLACE FUNCTION get_customer_profile(
  requested_customer_id BIGINT COMMENT 'The numeric customer ID to retrieve.'
)
RETURNS STRING
COMMENT 'Returns a customer profile by customer ID: state, loyalty segment, spend and orders. Never returns PII.'
RETURN SELECT CONCAT('Customer ID: ', CAST(customer_id AS STRING), ' | State: ', state,
                     ' | Segment: ', CAST(loyalty_segment AS STRING), ' | Spend: $', CAST(monetary AS STRING),
                     ' | Orders: ', CAST(num_orders AS STRING))
FROM {GOLD_TABLE}
WHERE customer_id = requested_customer_id
LIMIT 1
"""
spark.sql(PROFILE_DDL)

display(spark.sql("SELECT state, COUNT(*) AS customers FROM gold_customer_360 GROUP BY state ORDER BY customers DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instructions in Genie (the prompt layer)
# MAGIC According to the docs this works on Free Edition, I have not tested it (Genie was tested on the Premium lab)
# MAGIC
# MAGIC This is not code to run. Paste it into the **General instructions** of a Genie Agent (formerly Genie space) on the table `gold_customer_360`:
# MAGIC
# MAGIC ```text
# MAGIC tax_id and customer_name are PII, do not show their values.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: row filter
# MAGIC Tested on Free Edition (2026-10-17); without the `all_states_analysts` group the filter applies to you as well

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION retail_row_filter(state_val STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'The all_states_analysts group sees all states, everyone else only CA.'
# MAGIC RETURN is_account_group_member('all_states_analysts') OR state_val = 'CA';
# MAGIC
# MAGIC ALTER TABLE gold_customer_360
# MAGIC SET ROW FILTER retail_row_filter ON (state);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test: only CA is left
# MAGIC SELECT state, COUNT(*) AS customers
# MAGIC FROM gold_customer_360
# MAGIC GROUP BY state
# MAGIC ORDER BY customers DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: column mask
# MAGIC Tested on Free Edition (2026-10-17)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION mask_tax_id(tax_id_val STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Only the compliance_officers group sees the real tax_id.'
# MAGIC RETURN CASE WHEN is_account_group_member('compliance_officers')
# MAGIC             THEN tax_id_val ELSE '***MASKED***' END;
# MAGIC
# MAGIC ALTER TABLE gold_customer_360
# MAGIC ALTER COLUMN tax_id SET MASK mask_tax_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test: the filter and the mask work together
# MAGIC SELECT customer_id, state, loyalty_segment, tax_id
# MAGIC FROM gold_customer_360
# MAGIC WHERE tax_id IS NOT NULL
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: checking in Genie
# MAGIC According to the docs Genie works on Free Edition, I have not tested it (tested on the Premium lab)
# MAGIC
# MAGIC No code. In a new Genie chat ask: "How many customers do we have in the state of New York (NY)?" and
# MAGIC "Show the tax_id and state of five VIP customers". Then check the table's **Sample Data** in Catalog Explorer
# MAGIC (`***MASKED***` in `tax_id`, only `CA` in `state`). The same policies are visible in the table metadata:

# COMMAND ----------

display(spark.sql("DESCRIBE TABLE EXTENDED gold_customer_360"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: least privilege for the agent
# MAGIC Tested on Free Edition (2026-10-19)
# MAGIC
# MAGIC The agent calls a tool function instead of reading the table. The row filter still applies inside the function:
# MAGIC customer 100006 is in CA, customer 100001 is in NY.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT get_customer_profile(100006) AS ca_customer,
# MAGIC        get_customer_profile(100001) AS ny_customer;

# COMMAND ----------

# MAGIC %md
# MAGIC Tested on Free Edition (2026-10-19)
# MAGIC
# MAGIC The agent's identity gets `EXECUTE` on the functions and `USE SCHEMA`, but no `SELECT` on the table.
# MAGIC `USE CATALOG` on the catalog is granted by the catalog owner.

# COMMAND ----------

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{principal}`")
spark.sql(f"GRANT EXECUTE ON FUNCTION get_average_customer_value TO `{principal}`")
spark.sql(f"GRANT EXECUTE ON FUNCTION get_customer_profile TO `{principal}`")
display(spark.sql(f"SHOW GRANTS `{principal}` ON FUNCTION get_customer_profile"))

# COMMAND ----------

# MAGIC %md
# MAGIC Pitfall: the same `CREATE OR REPLACE FUNCTION` once more, and the grant on the function is gone.

# COMMAND ----------

spark.sql(PROFILE_DDL)
display(spark.sql(f"SHOW GRANTS `{principal}` ON FUNCTION get_customer_profile"))

# COMMAND ----------

# MAGIC %md
# MAGIC The fix: `EXECUTE` on the schema with the agent's tools. It is inherited by the functions, also after they are recreated.
# MAGIC In this demo it is the same schema as the table, because there is only one; in production use a separate schema with tools only.

# COMMAND ----------

spark.sql(f"GRANT EXECUTE ON SCHEMA {catalog}.{schema} TO `{principal}`")
spark.sql(PROFILE_DDL)
display(spark.sql(f"SHOW GRANTS `{principal}` ON FUNCTION get_customer_profile"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: cleanup
# MAGIC Tested on Free Edition (2026-10-17)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gold_customer_360 DROP ROW FILTER;
# MAGIC ALTER TABLE gold_customer_360 ALTER COLUMN tax_id DROP MASK;
# MAGIC DROP FUNCTION IF EXISTS retail_row_filter;
# MAGIC DROP FUNCTION IF EXISTS mask_tax_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## By the way: define the measure first, then the result
# MAGIC Tested on Free Edition (2026-10-17)
# MAGIC
# MAGIC No code in the article. Two correct numbers of VIP customers: rows vs unique `customer_id`
# MAGIC (in the synthetic data the difference is the VIP customers with a duplicated row; in the article 9,541 vs 9,494).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS vip_rows,
# MAGIC        COUNT(DISTINCT customer_id) AS vip_customers
# MAGIC FROM gold_customer_360
# MAGIC WHERE loyalty_segment = 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC The table first (this also removes any policies), then the functions (this also removes their grants), then the grants on the schema.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gold_customer_360")
for f in ["retail_row_filter", "mask_tax_id", "get_average_customer_value", "get_customer_profile"]:
    spark.sql(f"DROP FUNCTION IF EXISTS {f}")
spark.sql(f"REVOKE EXECUTE ON SCHEMA {catalog}.{schema} FROM `{principal}`")
spark.sql(f"REVOKE USE SCHEMA ON SCHEMA {catalog}.{schema} FROM `{principal}`")
