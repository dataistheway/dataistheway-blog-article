-- Bronze: raw JSON files from the raw volume, ingested incrementally with Auto Loader.

CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT "Raw order events (sales and returns)."
AS SELECT *, _metadata.file_path AS source_file
FROM STREAM read_files('/Volumes/${catalog}/${schema}/raw/orders', format => 'json');

CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT "Raw customers."
AS SELECT * FROM STREAM read_files('/Volumes/${catalog}/${schema}/raw/customers', format => 'json');

CREATE OR REFRESH STREAMING TABLE bronze_products
COMMENT "Raw products."
AS SELECT * FROM STREAM read_files('/Volumes/${catalog}/${schema}/raw/products', format => 'json');

CREATE OR REFRESH STREAMING TABLE bronze_stores
COMMENT "Raw stores."
AS SELECT * FROM STREAM read_files('/Volumes/${catalog}/${schema}/raw/stores', format => 'json');
