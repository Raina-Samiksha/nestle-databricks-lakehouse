# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,DQ Validation Header
# MAGIC %md
# MAGIC # DQ Validation – Day 1
# MAGIC Data quality checks for `nestle_dev_bronze.sql_server.sales_transactions`

# COMMAND ----------

# DBTITLE 1,Check 1: No Null Primary Keys
# MAGIC %sql
# MAGIC SELECT 'DQ_CHECK_1_NO_NULL_PK' AS check,
# MAGIC   CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM nestle_dev_bronze.sql_server.sales_transactions
# MAGIC WHERE transaction_id IS NULL OR product_id IS NULL OR region IS NULL;

# COMMAND ----------

# DBTITLE 1,Check 2: No Duplicate Transaction IDs
# MAGIC %sql
# MAGIC SELECT 'DQ_CHECK_2_NO_DUPLICATES' AS check,
# MAGIC   CASE WHEN COUNT(*) = (SELECT COUNT(DISTINCT transaction_id) FROM nestle_dev_bronze.sql_server.sales_transactions) THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM nestle_dev_bronze.sql_server.sales_transactions;

# COMMAND ----------

# DBTITLE 1,Check 3: Amount Must Be Positive
# MAGIC %sql
# MAGIC SELECT 'DQ_CHECK_3_AMOUNT_POSITIVE' AS check,
# MAGIC   CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM nestle_dev_bronze.sql_server.sales_transactions
# MAGIC WHERE amount <= 0;

# COMMAND ----------

# DBTITLE 1,Check 4: Valid Channel Values
# MAGIC %sql
# MAGIC SELECT 'DQ_CHECK_4_VALID_CHANNELS' AS check,
# MAGIC   CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM nestle_dev_bronze.sql_server.sales_transactions
# MAGIC WHERE channel NOT IN ('Online', 'Retail', 'B2B', 'Wholesale');
