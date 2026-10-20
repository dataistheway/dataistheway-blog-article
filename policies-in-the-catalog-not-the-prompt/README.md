# Policies go to the catalog, not the prompt

Code for the article https://dataistheway.blog/en/policies-in-the-catalog-not-the-prompt/.

Builds a synthetic `gold_customer_360` table (400 customers) and moves access rules out of the prompt into Unity Catalog: a row filter, a column mask and least privilege for the agent's identity.

**Runs on:** recorded on a Premium workspace. Steps 1, 2 and 5 were also tested on Free Edition (2026-10-17); step 4 (the tool function call and the grants) on 2026-10-19; step 3 (Genie) was not tested there.

## How to run

1. Import `policies_in_catalog.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets `catalog`, `schema` and `principal` (default `account users`; in production the application ID of the agent's service principal). The schema is created if it does not exist. `account users` is only a demo default: until the cleanup cell runs, every user in the account can call the functions in this schema. Least privilege in production means a dedicated group or the agent's own service principal.
4. Run all cells. The last cell removes everything the notebook created, including the grants on the schema.

Run it in a dedicated schema: the notebook replaces, filters and drops `gold_customer_360`, so never point it at a schema with a real table of that name.

## What each step does

- **Data preparation:** the synthetic customer table.
- **Instructions in Genie:** what the prompt-only approach looks like.
- **Step 1:** row filter.
- **Step 2:** column mask on `tax_id` (only the `compliance_officers` group sees the real value).
- **Step 3:** no code: the questions to ask in Genie and what to check in Catalog Explorer.
- **Step 4:** the agent's tool function under the row filter, `EXECUTE` grants instead of `SELECT`, the `CREATE OR REPLACE FUNCTION` pitfall and the fix with `EXECUTE` on the schema.
- **Step 5:** removing the policies.
- **By the way:** define the measure first, then the result.
- **Cleanup:** drops everything the notebook created.
