# Data is the Way: code from the blog

Runnable code for the articles on [dataistheway.blog](https://dataistheway.blog/en/). Each folder is one article: a Databricks notebook (or SQL script) with the steps in the same order as in the text, plus the setup needed to run them on synthetic data. Every folder has its own README that explains what the code does, step by step.

[Wersja polska](README.pl.md)

## Articles

| Folder | Article | Where it runs |
|---|---|---|
| [dbfs-september-30-2026](dbfs-september-30-2026/) | [DBFS is gone from new workspaces](https://dataistheway.blog/en/dbfs-september-30-2026/) | Free Edition |
| [sql-isnt-dying-its-mutating](sql-isnt-dying-its-mutating/) | [SQL isn't dying, it's mutating](https://dataistheway.blog/en/sql-isnt-dying-its-mutating/) | Free Edition |
| [policies-in-the-catalog-not-the-prompt](policies-in-the-catalog-not-the-prompt/) | [Policies go to the catalog, not the prompt](https://dataistheway.blog/en/policies-in-the-catalog-not-the-prompt/) | Free Edition, step 4 needs Premium |
| [magnifying-glass-book-and-buddy](magnifying-glass-book-and-buddy/) | [Magnifying glass, book and buddy](https://dataistheway.blog/en/magnifying-glass-book-and-buddy/) | Free Edition + optional SQL Server 2025 script |
| [six-layers-of-agent-defence](six-layers-of-agent-defence/) | [Six layers of agent defence](https://dataistheway.blog/en/six-layers-of-agent-defence/) | Free Edition, step 6 needs Premium |
| [data-skew-decision-tree](data-skew-decision-tree/) | [Data skew in Spark: a decision tree](https://dataistheway.blog/en/data-skew-decision-tree/) | Free Edition |
| [lakeflow-multiple-triggers](lakeflow-multiple-triggers/) | [Lakeflow Jobs: multiple triggers on one job](https://dataistheway.blog/en/lakeflow-multiple-triggers/) | Premium, Multiple Triggers preview |
| [lakeflow-pipeline-run-as-group](lakeflow-pipeline-run-as-group/) | [Lakeflow: a pipeline that doesn't belong to one person](https://dataistheway.blog/en/lakeflow-pipeline-run-as-group/) | Premium, workspace admin |

## How to run a notebook

1. Import the file into your workspace: **Workspace → Import → File**, or with the CLI:
   ```bash
   databricks workspace import /Users/<your-login>/dataistheway/<notebook> \
     --file <folder>/<notebook>.py --format SOURCE --language PYTHON
   ```
2. Attach it to Serverless compute (or a SQL warehouse for `.sql` notebooks).
3. Set the `catalog` and `schema` widgets. The defaults are `workspace` and `dataistheway`, because the `workspace` catalog exists on Databricks Free Edition. The schema is created if it does not exist, but the catalog has to be there already and we need `CREATE SCHEMA` on it.
4. Run the cells in order. The last cell drops everything the notebook created.

All data is synthetic and generated inside the notebooks, so no files have to be uploaded first.

## Known limitations

- Steps marked "Requires a full (Premium) workspace" use service principals, account groups or grants that Free Edition does not have. The rest of the notebook still runs without them.
- Numbers in the articles come from our own runs. Results based on `rand()` or on LLM answers (`ai_similarity`, agents) can differ slightly between runs and model versions.
- Databricks changes fast. Each article has an "as of" date, and the code was run on that date.

## License

[MIT](LICENSE). You can copy and adapt the code, also in commercial projects.
