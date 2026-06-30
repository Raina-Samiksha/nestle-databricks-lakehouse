CREATE TABLE IF NOT EXISTS nestle_dev_silver.control.watermark_tracking (
  source_id STRING NOT NULL,
  watermark_column STRING NOT NULL,
  last_high_water_mark TIMESTAMP,
  updated_at TIMESTAMP NOT NULL,
  PRIMARY KEY (source_id, watermark_column)
) USING DELTA;

INSERT INTO nestle_dev_silver.control.watermark_tracking
SELECT 'sql_sales_transactions', 'modified_at',
  CAST('1900-01-01 00:00:00' AS TIMESTAMP), current_timestamp()
WHERE NOT EXISTS (
  SELECT 1 FROM nestle_dev_silver.control.watermark_tracking
  WHERE source_id = 'sql_sales_transactions'
);