# Six layers of agent defence

Code for the article https://dataistheway.blog/en/six-layers-of-agent-defence/.

Builds a small agent on synthetic customer data and shows the layers that stop data leaks, none of which is "the model refused".

**Runs on:** recorded on a Premium workspace (Azure Databricks). Free Edition (checked on 2026-10-17): steps 0, 1, 3, 4 and 6 pass; steps 2 and 5 fail with `'system.ai.meta-llama-3-3-70b-instruct' does not exist.`, because Free Edition has no Unity Gateway model services. With `LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"` and `use_ai_gateway=False` the whole notebook passed there.

## Before you run it

- `agent_principal`: the application ID of the service principal the agent runs as. Step 4 grants it `USE CATALOG`, `USE SCHEMA` and `EXECUTE`, so you need to be the catalog owner, a metastore admin or have `MANAGE` on the catalog (`ALL PRIVILEGES` is not enough).
- Step 6 reads `system.access.audit`: you need `USE SCHEMA` and `SELECT` on `system.access`.

## How to run

1. Import `six_layers_of_defence.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets `catalog`, `schema` (default `news32_agent_defence`, created if it does not exist) and `agent_principal`.
4. Run all cells. The last cell drops the table, the functions and the schema and revokes `USE CATALOG` from the agent's service principal.

## What each step does

- **Step 0:** 20 synthetic customers with fictitious tax ids.
- **Step 1 (layer 4):** the function returns only the minimum columns.
- **Step 2:** the agent, four direct attacks and two assertions for layers 4 and 6.
- **Step 3 (layer 5):** a mask in the catalog on a group that does not exist.
- **Step 4 (layer 6):** grants for the agent's service principal, and how `CREATE OR REPLACE FUNCTION` wipes them.
- **Step 5:** an indirect attack through data (kept in memory only, never written to a table).
- **Step 6:** audit of who fetched the function.
- **Cleanup.**

The steps have the same numbers as in the article.

## Notes

The agent calls the model `system.ai.meta-llama-3-3-70b-instruct` as a Unity Gateway model service: `ChatDatabricks(model=LLM_MODEL, use_ai_gateway=True)` with `databricks-langchain==0.21.0`. Agent answers come from an LLM, so they can differ between runs.

This is the path for workspaces with **Enforce Unity Gateway** on (per the [docs](https://learn.microsoft.com/azure/databricks/ai-gateway/migrate-to-unity-gateway), the default on new workspaces), where the pay-per-token `databricks-*` endpoints return `PERMISSION_DENIED`. On our lab and on Free Edition enforcement is off (the old endpoint still answers), so we have not seen that error ourselves.

`langchain` and `langgraph` are pinned in the `%pip` cell: without the pins the install on serverless failed with `ResolutionTooDeep`. The pins downgrade a few core packages, so Databricks shows a "Core Python package version(s) changed" warning; it does not affect the run.
