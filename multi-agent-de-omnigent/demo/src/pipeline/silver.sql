-- Silver: typed, cleaned, joined order lines.

CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_quantity EXPECT (quantity > 0)
)
COMMENT "Order events with product and customer attributes."
AS SELECT
  CAST(o.order_id AS BIGINT) AS order_id,
  o.event_type,
  o.status,
  CAST(o.event_ts AS TIMESTAMP) AS event_ts,
  CAST(o.event_ts AS DATE) AS event_date,
  CAST(o.store_id AS INT) AS store_id,
  CAST(o.customer_id AS INT) AS customer_id,
  c.full_name AS customer_name,
  c.email AS customer_email,
  CAST(o.product_id AS INT) AS product_id,
  p.product_name,
  p.category,
  CAST(o.quantity AS INT) AS quantity,
  CAST(o.unit_price AS DECIMAL(10, 2)) AS unit_price
FROM STREAM(bronze_orders) o
LEFT JOIN bronze_customers c ON o.customer_id = c.customer_id
LEFT JOIN bronze_products p ON o.product_id = p.product_id;

CREATE OR REFRESH MATERIALIZED VIEW silver_stores
COMMENT "Store dimension."
AS SELECT CAST(store_id AS INT) AS store_id, store_name, city FROM bronze_stores;
