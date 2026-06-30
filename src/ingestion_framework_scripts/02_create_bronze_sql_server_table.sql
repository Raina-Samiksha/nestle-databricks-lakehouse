CREATE TABLE IF NOT EXISTS nestle_dev_bronze.sql_server.sales_transactions (
  transaction_id STRING,
  product_id STRING,
  region STRING,
  channel STRING,
  customer_id STRING,
  quantity LONG,
  unit_price DECIMAL(10, 2),
  amount DECIMAL(15, 2),
  created_at TIMESTAMP,
  modified_at TIMESTAMP,
  ingestion_timestamp TIMESTAMP
) USING DELTA;