# Policies go to the catalog, not the prompt

Code for the article https://dataistheway.blog/en/policies-in-the-catalog-not-the-prompt/.

Builds a synthetic `gold_customer_360` table (400 customers) and moves access rules out of the prompt into Unity Catalog: a row filter, a column mask and least privilege for the agent's identity.

**Runs on:** Free Edition; step 4 needs a full (Premium) workspace and a service principal.

## How to run

1. Import `policies_in_catalog.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`, ...). The schema is created if it does not exist.
4. Run all cells. The last cell removes everything the notebook created.

## What each step does

- **Data preparation:** the synthetic customer table.
- **Instructions in Genie:** what the prompt-only approach looks like.
- **Step 1:** row filter.
- **Step 2:** column mask on `tax_id` (only the `compliance_officers` group sees the real value).
- **Step 3:** no code: the questions to ask in Genie and what to check in Catalog Explorer.
- **Step 4 (Premium):** grants for a service principal; set the `principal` widget and `run_grants = true`, otherwise the step is skipped.
- **Step 5:** removing the policies.
- **By the way:** define the measure first, then the result.
- **Cleanup:** drops everything the notebook created.
