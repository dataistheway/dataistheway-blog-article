# Genie One MCP: any agent can ask about your data, and permissions stay in Unity Catalog

Code for the article https://dataistheway.blog/en/genie-one-mcp/.

An MCP client in Python (`DatabricksMCPClient` from `databricks-mcp`) asks Genie One about a small synthetic sales table protected by a row filter. Genie writes SQL without any region condition, and Unity Catalog still returns PL rows only. The notebook also compares the new Genie One MCP service with the old Beta endpoint `/api/2.0/mcp/genie` (shut down on 2026-10-31) and with the single-space Genie Agent MCP server.

**Runs on:** steps 1–2 work on Free Edition (per the docs; I ran them on Premium, and on Free Edition there is no account group `admins`, so the filter applies to everyone); from step 3 on you need a full (Premium) workspace, a SQL warehouse you can use and access to Genie. Steps 4 and 6 also call the legacy single-space server `/api/2.0/mcp/genie/{space_id}`, which needs a workspace admin to enable **Managed MCP Servers** on the workspace Previews page. MCP calls run as whoever runs the notebook (a user or a service principal), and the row filter applies to that identity.

## How to run

1. Import `genie_one_mcp.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`). The schema is created if it does not exist.
4. Run all cells. The first two cells install `databricks-mcp`, `mcp` and `nest_asyncio` and restart Python. The last cell removes everything the notebook created.

## What each step does

- **Step 1:** the `sales_demo` table (60 synthetic rows, 15 per region) with table and column comments.
- **Step 2:** the `filter_region_pl` row filter: only PL rows, unless the caller is in the `admins` group.
- **Step 3:** a Genie Agent (space) `news_102 sales demo` created with `POST /api/2.0/genie/spaces`.
- **Step 4:** lists the tools of three MCP servers: Genie One (`/ai-gateway/mcp-services/system.ai.genie_one_mcp`), the old Beta endpoint and the single-space server.
- **Step 5:** asks Genie One (`genie_ask`, then `genie_poll_response`), prints the SQL Genie wrote and the rows from `genie_get_query_result`.
- **Step 6:** the same question through the single-space server, once through the MCP client and once as a JSON-RPC call through the Databricks SDK.
- **Cleanup:** deletes the Genie Agent (to the trash), the table and the function. The schema stays.

The article merges steps 1 and 2 into its step 1, so its steps 2–5 are notebook steps 3–6.

## Notes

- The filter uses `is_account_group_member('admins')`, which checks account-level groups. The workspace `admins` group is a workspace group, so a workspace admin still sees PL only; only members of an account group named `admins` see all regions.
- Genie answers come from an LLM, so the SQL and the wording can differ between runs.
- Keep the `"mcp>=1.24,<2"` pin (the same as in the Databricks docs). `databricks-mcp` has no upper bound on `mcp`, so without `<2` pip installs `mcp` 2.x, where the `CallToolResult` field is `structured_content`, and step 5 fails with `AttributeError: 'CallToolResult' object has no attribute 'structuredContent'`. The notebook reads the SQL and `item_id` from the markdown answer, which works whether or not the client returns `structuredContent`.
- After 2026-10-31 remove the `old Beta /api/2.0/mcp/genie` entry from `URLS` in step 4. The endpoint is shut down after that date, so the cell would fail and the notebook would never reach the cleanup.
- The polling loop in step 5 has no limit and treats a missing status as still running. In an agent or a test, cap the attempts or the time and treat an unknown status as an error.
- Step 3 takes the first SQL warehouse in the list. A Genie Agent requires a Pro or Serverless warehouse, so pick a specific one if you have several.
- If a run stops halfway, run the cleanup cell before the next run: `CREATE OR REPLACE TABLE` keeps an existing row filter.
