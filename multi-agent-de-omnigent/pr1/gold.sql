-- Gold: daily revenue per store and product category.
--
-- REVENUE RULE: net revenue = sales minus returns.
-- Returns arrive as SEPARATE order events (event_type RETURN / status RETURNED) with a
-- POSITIVE quantity, so a naive SUM(quantity * unit_price) counts a return as revenue.
-- Each event is therefore signed: +1 for a sale, -1 if either marker says it is a return.
-- Returns are booked on the RETURN event's own date (1-10 days after the sale), not
-- back-dated to the original sale -- a deliberate accounting rule: the gold table reports
-- what happened on that day, not a restated sales ledger.
-- Consequence: net revenue CAN be legitimately negative for a (event_date, store_id,
-- category) bucket -- a day with a return and few or no sales in that bucket. On the demo
-- data that is 95 of 1683 rows (~5.6%): 49 are buckets with returns only (sales stop on
-- 2026-10-15, returns run to 2026-10-25) and 46 are buckets where a return outweighs thin
-- sales. So revenue_non_negative is a WARN expectation: it measures the effect instead of
-- dropping real money or failing the run. gross_revenue is the true invariant (quantities
-- and prices are positive), so that one is a hard FAIL UPDATE.
--
-- NULL KEYS: ~1% of order events have no store_id, and silver_orders joins products with a
-- LEFT JOIN, so category can be NULL. Those rows are deliberately bucketed into the unknown
-- members (store_id -1 / 'Unknown store' / 'Unknown') instead of being dropped: dropping
-- would silently lose ~1% of revenue.
-- Because that substitution happens BEFORE the aggregate, the non-null key expectations
-- below can NOT detect upstream NULLs -- by construction there are none left to find. They
-- guard the gold output's own integrity: they fail the run if this table ever emits a row
-- whose (event_date, store_id, category) key is incomplete, which would break the grain
-- contract its consumers rely on.
-- The upstream signal is therefore surfaced as DATA instead of being silently absorbed:
-- unknown_store_events / unknown_category_events count the events that arrived with a NULL
-- key, measured before substitution. They are plain measures, not expectations -- ~1% NULL
-- store_id is expected in this fixture and must never fail the pipeline.
--
-- DIMENSION SAFETY: silver_stores is a materialized view over raw files with no uniqueness
-- guarantee, so a duplicated store row would fan out a naive join and MULTIPLY revenue.
-- Two defences: (1) the dimension is deduplicated to at most one row per store_id, and
-- (2) the fact is aggregated on the three contractual keys FIRST and only then enriched
-- with store_name, so no join can widen the grain or inflate a measure.

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue (
  CONSTRAINT event_date_not_null EXPECT (event_date IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT store_id_not_null   EXPECT (store_id IS NOT NULL)   ON VIOLATION FAIL UPDATE,
  CONSTRAINT category_not_null   EXPECT (category IS NOT NULL)   ON VIOLATION FAIL UPDATE,
  CONSTRAINT gross_revenue_non_negative EXPECT (gross_revenue >= 0) ON VIOLATION FAIL UPDATE,
  CONSTRAINT revenue_non_negative EXPECT (revenue >= 0)
)
CLUSTER BY (event_date, store_id)
COMMENT "Daily net revenue (sales minus returns) per store and product category."
AS
WITH stores AS (
  -- One deterministic row per store_id: lowest store_name wins if a store were ever
  -- duplicated (NULLS LAST, so a named duplicate beats an unnamed one), making the output
  -- stable across refreshes rather than join-order dependent.
  SELECT store_id, store_name
  FROM silver_stores
  QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_name NULLS LAST) = 1
),
events AS (
  SELECT
    o.event_date,
    COALESCE(o.store_id, -1) AS store_id,
    COALESCE(o.category, 'Unknown') AS category,
    -- Qualified with o. so these read the source columns, never the aliases above.
    o.store_id AS raw_store_id,
    o.category AS raw_category,
    -- A return is marked by either field; everything else is a sale.
    CASE WHEN o.event_type = 'RETURN' OR o.status = 'RETURNED' THEN -1 ELSE 1 END AS sign,
    o.quantity,
    o.quantity * o.unit_price AS gross_amount
  FROM silver_orders o
),
daily AS (
  SELECT
    event_date,
    store_id,
    category,
    CAST(SUM(sign * gross_amount) AS DECIMAL(18, 2)) AS revenue,
    CAST(SUM(CASE WHEN sign > 0 THEN gross_amount ELSE 0 END) AS DECIMAL(18, 2)) AS gross_revenue,
    CAST(SUM(CASE WHEN sign < 0 THEN gross_amount ELSE 0 END) AS DECIMAL(18, 2)) AS returned_revenue,
    SUM(CASE WHEN sign > 0 THEN quantity ELSE 0 END) AS units_sold,
    SUM(CASE WHEN sign < 0 THEN quantity ELSE 0 END) AS units_returned,
    COUNT_IF(sign > 0) AS sale_events,
    COUNT_IF(sign < 0) AS return_events,
    -- Masked upstream data-quality signal, measured before substitution.
    COUNT_IF(raw_store_id IS NULL) AS unknown_store_events,
    COUNT_IF(raw_category IS NULL) AS unknown_category_events
  FROM events
  GROUP BY event_date, store_id, category
)
SELECT
  d.event_date,
  d.store_id,
  COALESCE(s.store_name, 'Unknown store') AS store_name,
  d.category,
  d.revenue,
  d.gross_revenue,
  d.returned_revenue,
  d.units_sold,
  d.units_returned,
  d.sale_events,
  d.return_events,
  d.unknown_store_events,
  d.unknown_category_events
FROM daily d
LEFT JOIN stores s ON d.store_id = s.store_id;
