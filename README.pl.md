# Data is the Way: kod z bloga

Kod do artykułów z [dataistheway.blog](https://dataistheway.blog/). Każdy folder to jeden artykuł: notebook Databricks (albo skrypt SQL) z krokami w tej samej kolejności co w tekście oraz z przygotowaniem danych syntetycznych, żeby wszystko dało się uruchomić. Każdy folder ma własne README, które krok po kroku wyjaśnia, co robi kod.

Komentarze w kodzie są po angielsku, tak samo jak nazwy folderów. Artykuły czytamy w obu językach.

[English version](README.md)

## Artykuły

| Folder | Artykuł | Gdzie działa |
|---|---|---|
| [dbfs-september-30-2026](dbfs-september-30-2026/) | [DBFS znika z nowych workspace'ów](https://dataistheway.blog/dbfs-30-wrzesnia-2026/) | Free Edition |
| [sql-isnt-dying-its-mutating](sql-isnt-dying-its-mutating/) | [SQL nie umiera, tylko mutuje](https://dataistheway.blog/sql-nie-umiera-tylko-mutuje/) | Free Edition |
| [policies-in-the-catalog-not-the-prompt](policies-in-the-catalog-not-the-prompt/) | [Polityki w katalogu, nie w prompcie](https://dataistheway.blog/polityki-w-katalogu-nie-w-prompcie/) | Free Edition, krok 4 wymaga Premium |
| [magnifying-glass-book-and-buddy](magnifying-glass-book-and-buddy/) | [Lupa, książka i kumpel](https://dataistheway.blog/lupa-ksiazka-i-kumpel/) | Free Edition + opcjonalny skrypt na SQL Server 2025 |
| [six-layers-of-agent-defence](six-layers-of-agent-defence/) | [Sześć warstw obrony agenta](https://dataistheway.blog/szesc-warstw-obrony-agenta/) | Free Edition, krok 6 wymaga Premium |
| [data-skew-decision-tree](data-skew-decision-tree/) | [Data skew w Sparku: drzewko decyzyjne](https://dataistheway.blog/data-skew-drzewko-decyzyjne/) | Free Edition |
| [lakeflow-multiple-triggers](lakeflow-multiple-triggers/) | [Lakeflow Jobs: kilka triggerów na jednym jobie](https://dataistheway.blog/lakeflow-wiele-triggerow/) | Premium, podgląd Multiple Triggers |
| [lakeflow-pipeline-run-as-group](lakeflow-pipeline-run-as-group/) | [Lakeflow: pipeline, który nie należy do jednej osoby](https://dataistheway.blog/lakeflow-pipeline-run-as-grupa/) | Premium, admin workspace'u |

## Jak uruchomić notebook

1. Importujemy plik do workspace'u: **Workspace → Import → File** albo przez CLI:
   ```bash
   databricks workspace import /Users/<twoj-login>/dataistheway/<notebook> \
     --file <folder>/<notebook>.py --format SOURCE --language PYTHON
   ```
2. Podpinamy Serverless (albo SQL warehouse przy notebookach `.sql`).
3. Ustawiamy widgety `catalog` i `schema`. Domyślnie to `workspace` i `dataistheway`, bo katalog `workspace` jest na Databricks Free Edition. Schemat powstanie sam, ale katalog musi już istnieć i potrzebujemy na nim `CREATE SCHEMA`.
4. Uruchamiamy komórki po kolei. Ostatnia komórka usuwa wszystko, co notebook utworzył.

Wszystkie dane są syntetyczne i generowane w notebookach, więc nie trzeba niczego wcześniej wgrywać.

## Ograniczenia

- Kroki oznaczone „Requires a full (Premium) workspace” używają service principali, grup na koncie albo grantów, których na Free Edition nie ma. Reszta notebooka działa bez nich.
- Liczby w artykułach pochodzą z naszych uruchomień. Wyniki oparte na `rand()` albo na odpowiedziach modeli (`ai_similarity`, agenci) mogą się trochę różnić między uruchomieniami i wersjami modeli.
- Databricks zmienia się szybko. Każdy artykuł ma datę „Stan na” i kod był uruchamiany właśnie wtedy.

## Licencja

[MIT](LICENSE). Kod można kopiować i przerabiać, także w projektach komercyjnych.
