SELECT 'DQ_CHECK_1_NO_NULL_PK' as check,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM nestle_dev_bronze.sql_server.sales_transactions
WHERE transaction_id IS NULL OR product_id IS NULL OR region IS NULL;

SELECT 'DQ_CHECK_2_NO_DUPLICATES' as check,
  CASE WHEN COUNT(*) = (SELECT COUNT(DISTINCT transaction_id) FROM nestle_dev_bronze.sql_server.sales_transactions) THEN 'PASS' ELSE 'FAIL' END as status
FROM nestle_dev_bronze.sql_server.sales_transactions;

SELECT 'DQ_CHECK_3_AMOUNT_POSITIVE' as check,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM nestle_dev_bronze.sql_server.sales_transactions WHERE amount <= 0;

SELECT 'DQ_CHECK_4_VALID_CHANNELS' as check,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM nestle_dev_bronze.sql_server.sales_transactions
WHERE channel NOT IN ('Online', 'Retail', 'B2B', 'Wholesale');