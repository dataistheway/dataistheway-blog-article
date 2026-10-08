# Data skew in Spark: a decision tree

Code for the article https://dataistheway.blog/en/data-skew-decision-tree/.

Generates 2 million synthetic fact rows where about 90% share one customer key and about 2% have a `NULL` key, plus a 10,000-row dimension. Then it walks the article's decision tree on that data.

**Runs on:** Free Edition (serverless) or any Databricks workspace.

## How to run

1. Import `data_skew.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`, ...). The schema is created if it does not exist.
4. Run all cells. The last cell removes everything the notebook created.

## What each step does

- **Step 0, 0a:** create the skewed data and check the key distribution (is it really skew?).
- **Step 2:** broadcast the small side of the join.
- **Step 3:** `NULL` as a hot key: filter it out of an inner join, split it off for a left join.
- **Steps 4–5:** a plain shuffle join vs salting only the hot keys, with a row-count check.
- **Step 6:** skewed aggregations: `count(DISTINCT)` vs a two-step version (including the `NULL` trap), a salted two-stage sum, and the latest row per key with `max_by` instead of a window.
- **Cleanup:** drops both tables.

## Notes

After each join cell, open the query profile (serverless) or Spark UI → Stages to see how the work is spread across tasks.
