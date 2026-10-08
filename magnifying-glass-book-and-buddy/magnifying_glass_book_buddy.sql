-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Magnifying glass, book and buddy: LIKE, full-text and vectors in plain words
-- MAGIC
-- MAGIC Code for the article https://dataistheway.blog/en/magnifying-glass-book-and-buddy/.
-- MAGIC
-- MAGIC **Data:** synthetic, generated in the notebook (12 short articles about coffee: IDs 1-4 are texts for a professional barista, the rest is noise). No RetailHub files needed. The articles and the search patterns are in Polish on purpose: the article shows how Polish inflection weakens LIKE and regex, and the expected results depend on the Polish word forms.
-- MAGIC
-- MAGIC **Full-text:** Databricks SQL has no equivalent of `CONTAINSTABLE`/`FREETEXTTABLE`. The T-SQL version is in the file `sql_server_like_regex_fts.sql` next to this one.
-- MAGIC
-- MAGIC **Compute:** Serverless or a SQL Warehouse. Works on Free Edition.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'workspace';
CREATE WIDGET TEXT schema DEFAULT 'dataistheway';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema);
USE IDENTIFIER(:catalog || '.' || :schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 0: data

-- COMMAND ----------

-- The sample data below is Polish on purpose: the article shows how Polish
-- inflection (kawa, kawy, kawę, kawowe) breaks LIKE and regex, so the expected
-- results depend on these exact Polish texts. Do not translate them.
CREATE OR REPLACE TABLE coffee_articles (id INT, title STRING, type STRING, body STRING);

INSERT INTO coffee_articles VALUES
  (1,  'Zaawansowana ekstrakcja: profilowanie ciśnienia', 'science',
       'Dla profesjonalnego baristy kluczem do dobrego espresso jest kontrola ciśnienia w trakcie ekstrakcji: od niskiej preinfuzji do 9 barów i stopniowego obniżania. Zmieniamy w ten sposób kwasowość i body naparu.'),
  (2,  'Chemia wody w kawiarni: TDS i twardość', 'science',
       'Nawet najlepsze ziarna nie dadzą dobrego naparu bez odpowiedniej wody. Profesjonalne parzenie wymaga kontroli TDS oraz balansu między twardością ogólną a węglanową.'),
  (3,  'Dystrybucja i tampowanie w kolbie', 'howto',
       'Równomierna ekstrakcja zaczyna się w portafiltrze. Rozbijamy grudki mielonej kawy, równo ją rozprowadzamy i ubijamy, żeby woda przepływała przez całą objętość.'),
  (4,  'Refraktometr i procent ekstrakcji', 'science',
       'Refraktometr pozwala zmierzyć TDS i wyliczyć procent ekstrakcji. Dla espresso celujemy w 18-22 procent, co pomaga zdiagnozować kwaśny albo gorzki napar.'),
  (5,  'Historia kawy: legenda o pasterzu', 'history',
       'Legenda mówi, że pasterz zauważył pobudzenie swoich kóz po zjedzeniu czerwonych owoców pewnego krzewu. Tak miał narodzić się zwyczaj palenia i parzenia kawy.'),
  (6,  'French press krok po kroku', 'howto',
       'French press wymaga grubo zmielonych ziaren i wody o temperaturze około 94 stopni. Zalewamy kawę, czekamy 4 minuty i powoli wciskamy tłok.'),
  (7,  'Latte art: jak narysować serce', 'guide',
       'Kluczem jest mleko spienione na gładką mikropiankę. Wlewając je do espresso, najpierw trzymamy dzbanek wysoko, a potem obniżamy go i rysujemy wzór.'),
  (8,  'Arabica czy robusta', 'guide',
       'Arabica jest zwykle łagodniejsza i bardziej owocowa. Robusta jest bardziej gorzka, ma więcej kofeiny i daje grubszą cremę na espresso.'),
  (9,  'Regiony kawowe: Brazylia', 'geography',
       'Brazylia to największy producent kawy. Ziarna stąd mają niską kwasowość i nuty czekolady oraz orzechów.'),
  (10, 'Kawa a zdrowie: fakty i mity', 'lifestyle',
       'Umiarkowane picie kawy, 3-4 filiżanki dziennie, może mieć pozytywny wpływ na zdrowie, choć osoby z nadciśnieniem powinny uważać na kofeinę.'),
  (11, 'Cold brew na upały', 'howto',
       'Cold brew to kawa parzona zimną wodą przez 12-24 godziny. Wychodzi łagodny, orzeźwiający napój.'),
  (12, 'Przechowywanie kawy', 'guide',
       'Kawa traci aromat w kontakcie z powietrzem, wilgocią i światłem. Ziarna trzymamy w szczelnym opakowaniu z dala od słońca.');

SELECT COUNT(*) AS rows_loaded FROM coffee_articles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: the magnifying glass, or LIKE
-- MAGIC The question "jak prawidłowo parzyć kawę" ("how to brew coffee properly") as a LIKE pattern. None of the articles 1-4 contains those words.

-- COMMAND ----------

SELECT id, title
FROM coffee_articles
WHERE body ILIKE '%jak%prawidłowo%parzyć%kawę%'
   OR title ILIKE '%jak%prawidłowo%parzyć%kawę%';

-- COMMAND ----------

SELECT id, title
FROM coffee_articles
WHERE body ILIKE '%parzyć%' OR title ILIKE '%parzyć%';

-- COMMAND ----------

-- Inflection: '%kawa%' will not find 'kawy' or 'kawę' (other forms of "coffee")
SELECT id, title,
       title ILIKE '%kawa%'        AS matches_kawa,
       title RLIKE '(?i)kaw(a|y|ę|owe)' AS matches_forms
FROM coffee_articles
ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: a magnifying glass with better glass, or regex

-- COMMAND ----------

SELECT id, title,
       regexp_extract(body, '(?i)(espresso|aeropress|french press|chemex|v60|moka)', 1) AS first_method
FROM coffee_articles
WHERE body RLIKE '(?i)(espresso|aeropress|french press|chemex|v60|moka)';

-- COMMAND ----------

-- Trap: an empty alternative at the end of the pattern matches every row
SELECT
  count_if(body RLIKE '(?i)(jak|prawidlowo|parzyc|kawe|)') AS with_empty_alternative,
  count_if(body RLIKE '(?i)(jak|prawidlowo|parzyc|kawe)')  AS without_empty_alternative,
  COUNT(*) AS all_rows
FROM coffee_articles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: the buddy, or similarity of meaning
-- MAGIC `ai_similarity` compares two texts. No index and no model of your own. Check whether IDs 1-4 are at the top. The question stays in Polish because the data is Polish.

-- COMMAND ----------

SELECT id, title,
       ai_similarity(body, 'Jak prawidłowo parzyć kawę?') AS score
FROM coffee_articles
ORDER BY score DESC
LIMIT 5;

-- COMMAND ----------

-- The question below means: "Professional coffee brewing techniques for advanced baristas"
SELECT id, title,
       ai_similarity(body, 'Profesjonalne techniki parzenia kawy dla zaawansowanych baristów') AS score
FROM coffee_articles
ORDER BY score DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cleanup

-- COMMAND ----------

DROP TABLE IF EXISTS coffee_articles;
