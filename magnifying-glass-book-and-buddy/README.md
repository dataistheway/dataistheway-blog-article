# Code for the article "Magnifying glass, book and buddy"

Code for the article https://dataistheway.blog/en/magnifying-glass-book-and-buddy/.

| File | Where to run | What it does |
|---|---|---|
| `magnifying_glass_book_buddy.sql` | Databricks (SQL notebook, Serverless or a Pro/Serverless SQL warehouse; `ai_similarity` doesn't run on a Classic SQL warehouse), tested on Free Edition (2026-10-17) | creates 12 synthetic articles about coffee, shows `ILIKE`, `RLIKE`, the empty-alternative trap, `isearch` and `ai_similarity`, and drops the table at the end |

The sample articles and the search patterns are in Polish on purpose. The article shows how Polish inflection (kawa, kawy, kawę, kawowe) weakens `LIKE` and regex, and the expected results depend on those Polish word forms. Comments and notebook text are in English.

## Databricks
1. Import `magnifying_glass_book_buddy.sql` into your workspace (Import, then File).
2. Set the `catalog` and `schema` widgets (defaults: `workspace` / `dataistheway`). The schema is created if it does not exist.
3. Run the cells in order. The last cell drops the `coffee_articles` table.
