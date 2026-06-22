-- =============================================================
-- Metadata-Driven Ingestion Framework — control tables
-- Nestlé Lakehouse (DEV) — SCJ-aligned structure
--
-- Catalogs:
--   nestle_dev_bronze  → schemas: sharepoint, sql_server, data_lake
--   nestle_dev_silver  → schemas: control, core
--   nestle_dev_gold    → schemas: bi_core, lake_core, bi_secure_access
--
-- Control tables live in:  nestle_dev_silver.control
-- Raw Bronze tables live in: nestle_dev_bronze.{source_schema}
-- Files land in Volume:   /Volumes/nestle_dev_bronze/data_lake/landing/
--
-- Run top-to-bottom in SQL Editor.
-- =============================================================

-- ======== SCHEMAS (already created in UI, but CREATE IF NOT EXISTS is safe) ========
CREATE SCHEMA IF NOT EXISTS nestle_dev_silver.control;
CREATE SCHEMA IF NOT EXISTS nestle_dev_silver.core;
CREATE SCHEMA IF NOT EXISTS nestle_dev_bronze.sharepoint;
CREATE SCHEMA IF NOT EXISTS nestle_dev_bronze.sql_server;
CREATE SCHEMA IF NOT EXISTS nestle_dev_bronze.data_lake;

-- ======== LANDING VOLUME (for file sources to land before ingestion) ========
CREATE VOLUME IF NOT EXISTS nestle_dev_bronze.data_lake.landing;


-- =============================================================
-- 1. ingestion_source_config — master registry
--    One row per source. The engine reads this FIRST.
-- =============================================================
CREATE TABLE IF NOT EXISTS nestle_dev_silver.control.ingestion_source_config (
    source_id            STRING,
    source_system_name   STRING,
    source_dataset_name  STRING,
    ingestion_type       STRING,   -- 'SQL' or 'FILE'
    load_type            STRING,   -- 'full' | 'incremental' | 'hash'
    target_catalog       STRING,   -- 'nestle_dev_bronze'
    target_schema        STRING,   -- schema within that catalog
    target_table         STRING,   -- table name
    status_code          STRING,   -- 'Active' | 'Inactive'
    restart_flag         BOOLEAN,
    created_at           TIMESTAMP
) USING DELTA;


-- =============================================================
-- 2. sql_source_config — SQL-specific settings
-- =============================================================
CREATE TABLE IF NOT EXISTS nestle_dev_silver.control.sql_source_config (
    source_id              STRING,
    federated_catalog      STRING,   -- federation connection (set up later)
    source_query           STRING,   -- SQL query or table name
    watermark_column_name  STRING,
    last_watermark_value   STRING,
    write_mode             STRING,   -- 'overwrite' | 'append' | 'merge'
    key_column_names       STRING
) USING DELTA;


-- =============================================================
-- 3. file_source_config — FILE-specific settings
-- =============================================================
CREATE TABLE IF NOT EXISTS nestle_dev_silver.control.file_source_config (
    source_id              STRING,
    file_location          STRING,   -- /Volumes/... path
    file_type              STRING,   -- 'excel' | 'csv' | 'json'
    file_format_options    STRING,
    schema_definition_text STRING,
    key_column_names       STRING,
    hash_columns           STRING
) USING DELTA;


-- =============================================================
-- 4. ingestion_audit_log — one row per source per run
--    The FAILED -> SUCCESS pattern lives here.
-- =============================================================
CREATE TABLE IF NOT EXISTS nestle_dev_silver.control.ingestion_audit_log (
    source_id            STRING,
    job_id               STRING,
    run_id               STRING,
    source_system_name   STRING,
    source_dataset_name  STRING,
    ingestion_timestamp  TIMESTAMP,
    rows_read            BIGINT,
    rows_written         BIGINT,
    watermark_value      STRING,
    status               STRING,    -- 'FAILED' (pre-flight) -> 'SUCCESS'
    error_details        STRING,
    pipeline_name        STRING
) USING DELTA;


-- =============================================================
-- 5. dq_config — data quality rules per target table
-- =============================================================
CREATE TABLE IF NOT EXISTS nestle_dev_silver.control.dq_config (
    source_id          STRING,
    target_table       STRING,    -- fully qualified: 'nestle_dev_bronze.sharepoint.regional_sales_targets'
    pk_columns         STRING,
    not_null_columns   STRING,
    check_duplicates   BOOLEAN,
    check_row_count    BOOLEAN,
    active             BOOLEAN
) USING DELTA;


-- =============================================================
-- 6. dq_results — outcome of every DQ check on every run
-- =============================================================
CREATE TABLE IF NOT EXISTS nestle_dev_silver.control.dq_results (
    source_id         STRING,
    target_table      STRING,
    check_type        STRING,    -- 'row_count' | 'pk_not_null' | 'duplicate' | 'not_null'
    check_status      STRING,    -- 'PASS' | 'FAIL'
    check_details     STRING,
    run_id            STRING,
    result_timestamp  TIMESTAMP
) USING DELTA;


-- =============================================================
-- SEED DATA — your three Nestlé sources
-- (Re-runnable: TRUNCATE first)
-- =============================================================
TRUNCATE TABLE nestle_dev_silver.control.ingestion_source_config;
TRUNCATE TABLE nestle_dev_silver.control.sql_source_config;
TRUNCATE TABLE nestle_dev_silver.control.file_source_config;
TRUNCATE TABLE nestle_dev_silver.control.dq_config;

-- ---- Master registry: one row per source ----
INSERT INTO nestle_dev_silver.control.ingestion_source_config VALUES
  ('sharepoint_regional_targets', 'sharepoint', 'regional_sales_targets',
   'FILE', 'hash',        'nestle_dev_bronze', 'sharepoint', 'regional_sales_targets',
   'Active', false, current_timestamp()),

  ('sql_sales_transactions',      'sql_server', 'sales_transactions',
   'SQL',  'incremental', 'nestle_dev_bronze', 'sql_server', 'sales_transactions',
   'Active', false, current_timestamp()),

  ('datalake_web_events',         'data_lake',  'web_app_events',
   'FILE', 'full',        'nestle_dev_bronze', 'data_lake', 'web_app_events',
   'Active', false, current_timestamp());


-- ---- SQL source detail ----
INSERT INTO nestle_dev_silver.control.sql_source_config VALUES
  ('sql_sales_transactions', 'sqlserver_dev_catalog',
   'SELECT * FROM dbo.sales_transactions',
   'modified_at', '1900-01-01 00:00:00', 'merge', 'transaction_id');


-- ---- FILE source details — all paths point to /Volumes/nestle_dev_bronze/data_lake/landing/ ----
INSERT INTO nestle_dev_silver.control.file_source_config VALUES
  ('sharepoint_regional_targets',
   '/Volumes/nestle_dev_bronze/data_lake/landing/regional_sales_targets.xlsx',
   'excel', '{"header": true}',
   'product_id STRING, region STRING, target_amount DOUBLE, period STRING',
   'product_id,region',
   'product_id,region,target_amount,period'),

  ('datalake_web_events',
   '/Volumes/nestle_dev_bronze/data_lake/landing/web_app_events/',
   'json', '{"multiline": false}',
   'event_id STRING, product_id STRING, region STRING, event_type STRING, event_ts STRING',
   'event_id',
   NULL);


-- ---- DQ rules ----
INSERT INTO nestle_dev_silver.control.dq_config VALUES
  ('sharepoint_regional_targets', 'nestle_dev_bronze.sharepoint.regional_sales_targets',
   'product_id,region', 'product_id,region,target_amount', true, true, true),

  ('sql_sales_transactions', 'nestle_dev_bronze.sql_server.sales_transactions',
   'transaction_id', 'transaction_id,product_id,amount', true, true, true),

  ('datalake_web_events', 'nestle_dev_bronze.data_lake.web_app_events',
   'event_id', 'event_id,event_type', true, true, true);


-- =============================================================
-- VERIFY
-- =============================================================
-- SELECT * FROM nestle_dev_silver.control.ingestion_source_config;
-- SELECT * FROM nestle_dev_silver.control.file_source_config;
-- LIST '/Volumes/nestle_dev_bronze/data_lake/landing/';
