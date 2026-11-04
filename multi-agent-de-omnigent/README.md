# Multi-agent data engineering with Omnigent on Databricks

Code from the talk at the 4th Warsaw Databricks Community meetup (27 October 2026) and from the blog article.
Omnigent is alpha software and changes quickly. The recordings used Omnigent 0.20.0, a development build from commit `611429347` (with one local fix in `deploy.py`), self-hosted on Databricks Apps. Newer versions may differ.

## What is here

| Path | What it is |
|---|---|
| `demo/` | Declarative Automation Bundle: synthetic retail data generator and a bronze/silver Lakeflow pipeline. The gold layer is missing on purpose: building it is the agents' task. |
| `pr1/gold.sql` | The gold layer the agents built in PR #1 (Claude Code implemented it, Codex reviewed it twice). |
| `agents/lakehouse-reviewer/` | A custom Omnigent agent: a lead on `claude-sdk`, an implementer (Claude Code), a reviewer (Codex) and a local scanner (Pi + Ollama). |
| `agents/private-review/` | A cloud judge that grades a local model's review without file access, enforced by a server-side policy. |

## Steps

1. Deploy the Omnigent server to Databricks Apps (Lakebase for sessions, a Unity Catalog volume for artifacts).
   Follow the official guide: https://github.com/omnigent-ai/omnigent/tree/main/deploy/databricks
   What the lab run needed on top of the guide:
   - `--no-otel` when the workspace has no OTel tables;
   - after the first deploy, `grant_sp_perms.py` (gives the app's service principal access to Lakebase), then deploy again;
   - `--features grid_sessions_display_ui,custom_agents_settings_ui` for the session grid and Settings → Custom agents. The Databricks CLI reads a `--var` value with commas as a list, so in that build either patch how `deploy.py` quotes the value or pass a single feature.
2. Register your machine as a host:
   ```bash
   omnigent login <app_url>
   omnigent host <app_url>
   ```
   Sign in to Claude Code (`claude` → `/login`) and Codex (`codex login`) with the plans you already have.
3. Deploy the demo data and pipeline (from `demo/`):
   ```bash
   databricks bundle deploy --profile <your-profile>
   databricks bundle run generate_data --profile <your-profile>
   ```
4. Ask Polly (built-in orchestrator) for the gold layer:
   > Add a gold layer to the retailhub pipeline in demo/: daily revenue per store and product category, with expectations (non-null keys, revenue >= 0). Open a PR. Claude Code implements, Codex reviews. Work in English.
5. Add the custom agent and run a review:
   ```bash
   omnigent agent add agents/lakehouse-reviewer
   ```
   In the recordings `private-review` ran as a server agent, published by the server operator.
   Then pick `lakehouse-reviewer` in the agent list and ask: "Review PR #1: run reviewer and local_scanner in parallel, one pass each, then give me the merged findings table."
   The local scanner and the local reviewer use an Ollama model named `qwen3-coder-32k` (qwen3-coder 30B with a 32k context window). Create it with a Modelfile:
   ```
   FROM qwen3-coder:30b
   PARAMETER num_ctx 32768
   ```
   then `ollama create qwen3-coder-32k -f Modelfile`, and add `qwen3-coder-32k:latest` to Pi in `~/.pi/agent/models.json`.

## The trap in the data

Returns arrive as separate order events with a positive quantity. A naive `SUM(quantity * unit_price)` books them as revenue. On the demo data it gives 372,479.64: 23,944.09 (the value of returns) above gross sales of 348,535.55 and 47,888.18 above net revenue of 324,591.46.
