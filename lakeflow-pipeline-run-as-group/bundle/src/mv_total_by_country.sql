CREATE MATERIALIZED VIEW mv_total_by_country AS
SELECT country, sum(amount) AS total
FROM source_orders
GROUP BY country;
