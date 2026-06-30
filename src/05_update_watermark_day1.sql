UPDATE nestle_dev_silver.control.watermark_tracking
SET last_high_water_mark = CAST('2026-06-20 23:59:59' AS TIMESTAMP),
    updated_at = current_timestamp()
WHERE source_id = 'sql_sales_transactions'
  AND watermark_column = 'modified_at';

SELECT * FROM nestle_dev_silver.control.watermark_tracking
WHERE source_id = 'sql_sales_transactions';