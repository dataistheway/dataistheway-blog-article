-- T-SQL, SQL Server 2025
-- Article: "Magnifying glass, book and buddy: LIKE, full-text and vectors in plain words"
-- https://dataistheway.blog/en/magnifying-glass-book-and-buddy/
-- Requires the database and the table dbo.ArticlesAboutCoffee (60 articles about coffee) from the demo
-- script of my talk "Nowa Era Przeszukiwania Danych" ("A New Era of Data Search"), file 01_create_and_insert_60.sql.
-- That script is not part of this repository. Only the queries from the article are below.
-- The data and the search patterns are in Polish on purpose: the article shows how Polish inflection
-- weakens LIKE, regex and full-text search, and the expected counts depend on Polish word forms.
-- Regex (REGEXP_LIKE, REGEXP_SUBSTR) requires COMPATIBILITY_LEVEL = 170.

USE EspressoSearch2025;
GO

---------------------------------------------------------------
-- 1. Magnifying glass: LIKE
---------------------------------------------------------------
SELECT id, title FROM dbo.ArticlesAboutCoffee
WHERE body COLLATE Latin1_General_CI_AS LIKE N'%jak%prawidłowo%parzyć%kawę%';   -- expected: 0 rows

SELECT id, title FROM dbo.ArticlesAboutCoffee
WHERE title COLLATE Latin1_General_CI_AS LIKE N'%jak%prawidłowo%parzyć%kawę%';  -- expected: 0 rows

SELECT id, title FROM dbo.ArticlesAboutCoffee
WHERE title LIKE N'%kawa%';                                                     -- expected: 18 rows

-- Titles with a different form of the word, which '%kawa%' does not find
SELECT id, title FROM dbo.ArticlesAboutCoffee
WHERE title LIKE N'%kaw%' AND title NOT LIKE N'%kawa%';
GO

---------------------------------------------------------------
-- 2. Regex
---------------------------------------------------------------
SELECT id, title,
       REGEXP_SUBSTR(body, N'(espresso|aeropress|french press|chemex|v60|moka)', 1, 1, 'i') AS first_method
FROM dbo.ArticlesAboutCoffee
WHERE REGEXP_LIKE(body, N'(espresso|aeropress|french press|chemex|v60|moka)', 'i');

-- Trap from the demo script: an empty alternative at the end matches every row (60/60)
SELECT COUNT(*) AS with_empty_alternative
FROM dbo.ArticlesAboutCoffee
WHERE REGEXP_LIKE(body, N'(jak|prawidlowo|parzyc|kawe|)', 'i');

-- Fixed pattern (no empty alternative)
SELECT COUNT(*) AS without_empty_alternative
FROM dbo.ArticlesAboutCoffee
WHERE REGEXP_LIKE(body, N'(jak|prawidlowo|parzyc|kawe)', 'i');
GO

---------------------------------------------------------------
-- 3. Book: full-text search (Polish = 1045)
---------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.fulltext_catalogs WHERE name = N'CoffeeCat')
    CREATE FULLTEXT CATALOG CoffeeCat;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_ArticlesAboutCoffee_Id')
    CREATE UNIQUE INDEX UX_ArticlesAboutCoffee_Id ON dbo.ArticlesAboutCoffee(id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.fulltext_indexes WHERE object_id = OBJECT_ID(N'dbo.ArticlesAboutCoffee'))
    CREATE FULLTEXT INDEX ON dbo.ArticlesAboutCoffee
        (title LANGUAGE 1045, body LANGUAGE 1045)
        KEY INDEX UX_ArticlesAboutCoffee_Id ON CoffeeCat
        WITH CHANGE_TRACKING AUTO;
GO

SELECT TOP 10 d.id, d.title, k.rank
FROM FREETEXTTABLE(dbo.ArticlesAboutCoffee, (title, body), N'jak prawidłowo parzyć kawę') AS k
JOIN dbo.ArticlesAboutCoffee d ON d.id = k.[KEY]
ORDER BY k.rank DESC;

SELECT d.id, d.title, k.rank
FROM CONTAINSTABLE(dbo.ArticlesAboutCoffee, (title, body), N'("espresso" OR "kawa")') AS k
JOIN dbo.ArticlesAboutCoffee d ON d.id = k.[KEY]
ORDER BY k.rank DESC;
GO

---------------------------------------------------------------
-- Cleanup (only the full-text objects created above)
---------------------------------------------------------------
-- DROP FULLTEXT INDEX ON dbo.ArticlesAboutCoffee;
-- DROP FULLTEXT CATALOG CoffeeCat;
