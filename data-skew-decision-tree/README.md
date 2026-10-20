# Data skew in Spark: a decision tree

Code for the article https://dataistheway.blog/en/data-skew-decision-tree/.

Generates 2 million synthetic fact rows where about 90% share one customer key and about 2% have a `NULL` key, plus a 10,000-row dimension. Then it walks the article's decision tree on that data.

**Tested on:** serverless in our Premium lab workspace (2026-10-17) and on Free Edition (2026-10-19), with the same results. Not tested on a classic cluster.

## How to run

1. Import `data_skew.py` into your workspace (**Workspace → Import → File**).
2. Attach it to Serverless compute.
3. Set the widgets (`catalog`, `schema`, ...). The schema is created if it does not exist.
4. Run all cells. The last cell removes everything the notebook created.

## What each step does

- **Step 0, 0a:** create the skewed data, check the key distribution (is it really skew?) and try to read the AQE settings (on serverless they raise `AnalysisException`).
- **Step 2:** broadcast the small side of the join.
- **Step 3:** `NULL` as a hot key: filter it out of an inner join, split it off for a left join.
- **Steps 4–5:** a plain shuffle join vs salting only the hot keys, with a row-count check, then the same two joins with the `shuffle_hash` hint, timed three times with the `noop` sink.
- **Step 6:** the plan of `repartition()` before `groupBy`, skewed aggregations: `count(DISTINCT)` vs a two-step version (including the `NULL` trap), a salted two-stage sum, and the latest row per key with `max_by` instead of a window.
- **Cleanup:** drops both tables.

## Notes

On a classic cluster, the Spark UI → Stages tab shows how the work is spread across tasks (per the docs; we ran this notebook on serverless only). On serverless there is no Spark UI: the query profile shows per-operator metrics (and may flag a `DATA_SKEW` insight), so the notebook checks the key distribution with a query.
