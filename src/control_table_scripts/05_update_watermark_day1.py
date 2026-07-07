# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Update Watermark Header
# MAGIC %md
# MAGIC # Update Watermark – Day 1
# MAGIC Updates the high-water mark for `sql_sales_transactions` in `nestle_dev_silver.control.watermark_tracking`

# COMMAND ----------

# DBTITLE 1,Update Watermark
# MAGIC %sql
# MAGIC UPDATE nestle_dev_silver.control.watermark_tracking
# MAGIC SET last_high_water_mark = CAST('2026-06-20 23:59:59' AS TIMESTAMP),
# MAGIC     updated_at = current_timestamp()
# MAGIC WHERE source_id = 'sql_sales_transactions'
# MAGIC   AND watermark_column = 'modified_at';

# COMMAND ----------

# DBTITLE 1,Verify Watermark Update
# MAGIC %sql
# MAGIC SELECT * FROM nestle_dev_silver.control.watermark_tracking
# MAGIC WHERE source_id = 'sql_sales_transactions';
