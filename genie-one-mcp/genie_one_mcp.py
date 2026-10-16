# Databricks notebook source
# MAGIC %md
# MAGIC # Genie One MCP: any agent can ask about your data, and permissions stay in Unity Catalog
# MAGIC
# MAGIC Code for the article [https://dataistheway.blog/en/genie-one-mcp/](https://dataistheway.blog/en/genie-one-mcp/).
# MAGIC An MCP client in Python (like any agent) asks Genie One about a table protected by a row filter.
# MAGIC Genie writes SQL without any region condition, and Unity Catalog still returns PL rows only.
# MAGIC Creates table `sales_demo`, function `filter_region_pl` and Genie Agent `news_102 sales demo`.
# MAGIC The last cell removes them.

# COMMAND ----------

# MAGIC %pip install -U -q databricks-mcp "mcp>=1.24,<2" nest_asyncio

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "dataistheway")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
TABLE = f"{catalog}.{schema}.sales_demo"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: synthetic data with comments in the catalog
# MAGIC Genie reads the table and column comments as context. Works on Free Edition.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_demo (
# MAGIC   sale_date  DATE          COMMENT 'Sales day',
# MAGIC   region     STRING        COMMENT 'Country code of the sale: PL, DE, CZ, SK',
# MAGIC   category   STRING        COMMENT 'Product category',
# MAGIC   amount_pln DECIMAL(12,2) COMMENT 'Net sales amount in PLN'
# MAGIC ) COMMENT 'Daily demo sales (synthetic data) by region and category';
# MAGIC
# MAGIC INSERT INTO sales_demo
# MAGIC SELECT date_add(DATE'2026-09-01', CAST(id % 15 AS INT))                          AS sale_date,
# MAGIC        element_at(array('PL','DE','CZ','SK'), CAST(id % 4 AS INT) + 1)             AS region,
# MAGIC        element_at(array('bikes','helmets','accessories'), CAST(id % 3 AS INT) + 1) AS category,
# MAGIC        CAST(100 + (id * 37) % 900 AS DECIMAL(12,2))                                 AS amount_pln
# MAGIC FROM range(60);
# MAGIC
# MAGIC SELECT region, COUNT(*) AS row_count, SUM(amount_pln) AS total_pln
# MAGIC FROM sales_demo GROUP BY region ORDER BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: the policy goes to the catalog (row filter)
# MAGIC Only PL rows pass, unless the caller is in the `admins` group. Row filters work on Free Edition per the docs (I ran it on Premium).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION filter_region_pl(region STRING)
# MAGIC RETURN region = 'PL' OR is_account_group_member('admins');
# MAGIC
# MAGIC ALTER TABLE sales_demo SET ROW FILTER filter_region_pl ON (region);
# MAGIC
# MAGIC SELECT region, COUNT(*) AS row_count, SUM(amount_pln) AS total_pln
# MAGIC FROM sales_demo GROUP BY region ORDER BY region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: a Genie Agent (space) via the API
# MAGIC Genie One does not need it; it is here for the single-space server. Requires a full (Premium) workspace.

# COMMAND ----------

import json, re, time, uuid
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
wh = list(w.warehouses.list())[0]
me = w.current_user.me().user_name
space_def = {
    "version": 2,
    "config": {"sample_questions": [
        {"id": uuid.uuid4().hex, "question": ["What are the total sales by region?"]}]},
    "data_sources": {"tables": [{"identifier": TABLE}]},
    "instructions": {"text_instructions": [
        {"id": uuid.uuid4().hex, "content": ["Show amounts in PLN, rounded to 2 decimal places."]}]},
}
space = w.api_client.do("POST", "/api/2.0/genie/spaces", body={
    "title": "news_102 sales demo",
    "warehouse_id": wh.id,
    "parent_path": f"/Workspace/Users/{me}",
    "serialized_space": json.dumps(space_def),
})
space_id = space["space_id"]
print("Genie Agent created:", space["title"], "| SQL warehouse:", wh.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: an MCP client in Python lists the tools of three servers
# MAGIC Genie One, the old Beta endpoint and the single-space server. Requires a full (Premium) workspace.

# COMMAND ----------

import nest_asyncio
from databricks_mcp import DatabricksMCPClient
nest_asyncio.apply()  # the notebook already runs an asyncio loop, see Pitfalls
host = w.config.host.rstrip("/")
URLS = {
    "Genie One MCP (GA)": f"{host}/ai-gateway/mcp-services/system.ai.genie_one_mcp",
    "old Beta /api/2.0/mcp/genie": f"{host}/api/2.0/mcp/genie",
    "Genie Agent MCP (one space)": f"{host}/api/2.0/mcp/genie/{space_id}",
}
for label, url in URLS.items():
    tools = DatabricksMCPClient(server_url=url, workspace_client=w).list_tools()
    print(label, [t.name for t in tools])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: ask Genie One, get the SQL and the full result
# MAGIC `genie_ask` answers `in_progress`, so we poll `genie_poll_response`. The SQL and `item_id` for
# MAGIC `genie_get_query_result` come from the answer's markdown. Requires a full (Premium) workspace.

# COMMAND ----------

client = DatabricksMCPClient(server_url=URLS["Genie One MCP (GA)"], workspace_client=w)
t0 = time.time()
first = client.call_tool("genie_ask", {
    "question": f"What are the total sales (amount_pln) by region in table {TABLE}?"})
print("structuredContent from the client:", bool(first.structuredContent))
d = json.loads(first.content[0].text)
ids = {k: d[k] for k in ("conversation_id", "response_id")}
status, text = d["status"], first.content[0].text
while status == "in_progress":
    time.sleep(5)
    text = client.call_tool("genie_poll_response", ids).content[0].text
    status = (re.findall(r"\*\*Status:\*\*\s*(\w+)", text) or ["in_progress"])[0]
print("status:", status, "after", round(time.time() - t0), "s")
print(text.rsplit("</details>", 1)[-1].split("[Explore in Databricks]")[0])

# COMMAND ----------

sqls = re.findall(r"`{3}sql\n(.*?)`{3}", text, re.S)
item_ids = re.findall(r"`item_id`: `([^`]+)`", text)
for sql, item_id in zip(sqls, item_ids):
    res = client.call_tool("genie_get_query_result", {**ids, "item_id": item_id})
    full = json.loads(res.content[0].text)
    print(sql)
    print("rows:", full["rows"], "| truncated:", full["truncated"], "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: the same question through Genie Agent MCP (one space)
# MAGIC `query_space_<id>` may return before the answer is ready, so we also send the same call as raw JSON-RPC
# MAGIC through the SDK and read the final status. Requires a full (Premium) workspace.

# COMMAND ----------

agent = DatabricksMCPClient(server_url=URLS["Genie Agent MCP (one space)"], workspace_client=w)
query_tool = next(t.name for t in agent.list_tools() if t.name.startswith("query"))
args = {"query": "What are the total sales by region?"}
t0 = time.time()
try:
    print(agent.call_tool(query_tool, args).content[0].text[:1500])
except Exception as e:
    errors = [type(x).__name__ for x in getattr(e, "exceptions", [e])]
    print(f"DatabricksMCPClient after {round(time.time() - t0)} s:", errors)
t0 = time.time()
raw = w.api_client.do("POST", f"/api/2.0/mcp/genie/{space_id}", body={
    "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": query_tool, "arguments": args}})
answer = json.loads(raw["result"]["content"][0]["text"])
print(f"raw JSON-RPC after {round(time.time() - t0)} s:", answer["status"])
print(answer["content"]["queryAttachments"][0]["query"])
print(answer["content"]["textAttachments"][0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Removes the Genie Agent (to the trash), the table with its row filter and the function. The schema stays.

# COMMAND ----------

w.api_client.do("DELETE", f"/api/2.0/genie/spaces/{space_id}")
spark.sql("DROP TABLE IF EXISTS sales_demo")
spark.sql("DROP FUNCTION IF EXISTS filter_region_pl")
