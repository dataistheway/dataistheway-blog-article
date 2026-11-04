# RetailHub multi-agent demo

A Lakeflow Spark Declarative Pipelines pipeline (bronze → silver) over synthetic retail data. The gold layer is deliberately missing: building it is the task for the agents.

## Setup
Run from this folder:
```bash
databricks bundle deploy --profile <your-profile>
databricks bundle run generate_data --profile <your-profile>   # writes raw files, then refreshes the pipeline
```

## Layout
- `src/setup/generate_data.py`: synthetic customers, products, stores and order events (sales and returns).
- `src/pipeline/bronze.sql`, `silver.sql`: Auto Loader ingestion and cleaned order lines.
- `../agents/lakehouse-reviewer/`: a custom Omnigent agent (Claude implementer, Codex reviewer, local model scanner).
