# Six layers of agent defence

Code for the article https://dataistheway.blog/en/six-layers-of-agent-defence/.

Builds a small agent on synthetic customer data and shows the layers that stop data leaks, none of which is "the model refused".

**Runs on:** recorded on a Premium workspace (Azure Databricks). Steps 6–7 need a full (Premium) workspace (`run_premium = true` and the `agent_principal` widget). Free Edition: not checked. Steps 1 and 5 call the model through Unity Gateway, and the Free Edition limits page does not say whether Unity Gateway is available there.

## How to run

1. Import `six_layers_of_defence.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`, ...). The schema is created if it does not exist.
4. Run all cells. The last cell removes everything the notebook created.

## What each step does

- **Step 0:** 20 synthetic customers with fictitious tax ids.
- **Step 1:** the agent and four direct attacks.
- **Step 2:** the function returns only the minimum columns.
- **Step 3:** a mask in the catalog.
- **Step 5:** an indirect attack through data (kept in memory only, never written to a table).
- **Step 6 (Premium):** least privilege for the service principal.
- **Step 7 (Premium):** audit of who called the function.
- **Cleanup:** drops everything the notebook created.

The notebook numbers its steps differently from the article: article step 4 (agent identity) is Step 6 here, article step 6 (audit) is Step 7, and the function (Step 2) runs before the attacks (Step 1). There is no Step 4.

## Notes

The agent calls the model `system.ai.meta-llama-3-3-70b-instruct` as a Unity Gateway model service: `ChatDatabricks(model=LLM_MODEL, use_ai_gateway=True)` with `databricks-langchain==0.21.0` (the `llm_model` widget). Agent answers come from an LLM, so they can differ slightly between runs.

This is the path for workspaces with **Enforce Unity Gateway** on (per the [docs](https://learn.microsoft.com/azure/databricks/ai-gateway/migrate-to-unity-gateway), the default on new workspaces), where the pay-per-token `databricks-*` endpoints return `PERMISSION_DENIED`. The recording comes from a workspace with enforcement off, so the enforced case was not tested.

`langchain` and `langgraph` are pinned in the `%pip` cell: without the pins the install on serverless failed with `ResolutionTooDeep`.
