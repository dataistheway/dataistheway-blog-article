---
name: review-dabs
description: Checklist for reviewing a Databricks Asset Bundle with Lakeflow Declarative Pipelines.
---

# Review checklist for Databricks bundles

1. `databricks bundle validate` passes.
2. Business logic: revenue excludes or nets out returns; no double counting from duplicate events; joins do not fan out rows.
3. Gold tables are materialized views and keep the dimensions analysts filter by (date, store, category).
4. Expectations exist for keys (NOT NULL) and business rules (amounts >= 0), with a deliberate action (warn / drop / fail).
5. No personal data in silver or gold without masking (emails, names).
6. Modern syntax: `CREATE OR REFRESH`, no `LIVE.` prefix, `STREAM read_files` for ingestion.
7. Cost: serverless pipeline, triggered (not continuous) unless latency requires it.
