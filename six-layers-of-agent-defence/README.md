# Six layers of agent defence

Code for the article https://dataistheway.blog/en/six-layers-of-agent-defence/.

Builds a small agent on synthetic customer data and shows the layers that stop data leaks, none of which is "the model refused".

**Runs on:** Free Edition for steps 1–5; steps 6–7 need a full (Premium) workspace (`run_premium = true` and the `agent_principal` widget).

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

## Notes

Agent answers come from an LLM (`databricks-meta-llama-3-3-70b-instruct`), so they can differ slightly between runs.
