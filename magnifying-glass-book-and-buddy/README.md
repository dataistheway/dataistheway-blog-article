# Code for the article "Magnifying glass, book and buddy"

Code for the article https://dataistheway.blog/en/magnifying-glass-book-and-buddy/.

| File | Where to run | What it does |
|---|---|---|
| `magnifying_glass_book_buddy.sql` | Databricks (SQL notebook, Serverless or a SQL Warehouse), works on Free Edition | creates 12 synthetic articles about coffee, shows `ILIKE`, `RLIKE`, the empty-alternative trap and `ai_similarity`, and drops the table at the end |
| `sql_server_like_regex_fts.sql` | SQL Server 2025 (T-SQL, e.g. SSMS), database `EspressoSearch2025` | the same queries on 60 articles from my talk demo, plus full-text search (`FREETEXTTABLE`, `CONTAINSTABLE`) |

The sample articles and the search patterns are in Polish on purpose. The article shows how Polish inflection (kawa, kawy, kawę, kawowe) weakens `LIKE`, regex and full-text search, and the expected results depend on those Polish word forms. Comments and notebook text are in English.

## Databricks
1. Import `magnifying_glass_book_buddy.sql` into your workspace (Import, then File).
2. Set the `catalog` and `schema` widgets (defaults: `workspace` / `dataistheway`). The schema is created if it does not exist.
3. Run the cells in order. The last cell drops the `coffee_articles` table.

## SQL Server 2025
1. You need the `EspressoSearch2025` database with the `dbo.ArticlesAboutCoffee` table (60 rows), created by the script `01_create_and_insert_60.sql` from the materials of my talk "Nowa Era Przeszukiwania Danych" ("A New Era of Data Search"). That script is not part of this repository, so this file cannot be run on its own. The `embedding` column is not needed here.
2. Full-text search must be installed on the instance (the "Full-Text and Semantic Extractions for Search" component).
3. Run the script section by section. The expected counts are in the comments. They were computed by emulating the queries on the data, so compare them with the server output on the first run.
