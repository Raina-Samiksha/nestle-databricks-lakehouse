# Runbook — Operating the Lakehouse

## How to Run the Pipeline

### Automated (Scheduled)

The pipeline runs **daily at 6:00 AM UTC** automatically. No action needed.

**To check the schedule:**
- Databricks → Jobs & Pipelines → `nestle_regional_sales_targets_pipeline` → Schedule tab

---

### Manual Run (Ad-hoc)

1. **Databricks UI:**
   - Left sidebar → Jobs & Pipelines
   - Find `nestle_regional_sales_targets_pipeline`
   - Click **Run now**
   - Watch tasks execute sequentially

2. **Expected Runtime:** ~10-15 minutes (5 tasks)

3. **Monitor Progress:**
   - Click the run ID to see task details
   - Each task shows: start time, duration, status (SUCCEEDED/FAILED)
   - View logs by clicking task name

---

### What Each Task Does

| Task | Notebook | Duration | Input | Output |
|---|---|---|---|---|
| **1. Bronze SharePoint** | 03_nb_bronze_ingestion_scj | 3-5 min | Excel file | 50-53 rows in Bronze |
| **2. Silver SharePoint** | 05_nb_silver_scd2_regional_sales_targets_scj | 2-3 min | Bronze table | SCD2 table with history |
| **3. Bronze SQL Server** | 06_nb_bronze_watermark_sql_server_scj | 3-5 min | CSV file | 300-410 rows in Bronze |
| **4. Silver SQL Server** | 06_nb_silver_scd2_sql_sales_transactions_scj | 2-3 min | Bronze table | SCD2 table with history |
| **5. Gold Aggregation** | (Materialized view refresh) | 2-3 min | Silver tables | Fact + dims + agg views |

---

## Monitoring & Troubleshooting

### Check Data Freshness

```sql
-- Latest ingestion timestamp
SELECT source_id, MAX(load_timestamp) as last_load, status
FROM nestle_dev_silver.control.ingestion_audit_log
GROUP BY source_id
ORDER BY last_load DESC;

-- Should show timestamps from today's run (or yesterday if off-hours)
```

### Check Row Counts

```sql
SELECT 
  'SharePoint Bronze' as layer, COUNT(*) as rows
FROM nestle_dev_bronze.sharepoint.regional_sales_targets
UNION ALL
SELECT 'SQL Server Bronze', COUNT(*) 
FROM nestle_dev_bronze.sql_server.sales_transactions
UNION ALL
SELECT 'SharePoint Silver', COUNT() 
FROM nestle_dev_silver.core.regional_sales_targets_scd2
UNION ALL
SELECT 'SQL Server Silver', COUNT()
FROM nestle_dev_silver.core.sql_sales_transactions_scd2
UNION ALL
SELECT 'Gold Fact', COUNT()
FROM nestle_dev_gold.bi_core.f_sales_fact;
```

### Check Data Quality

```sql
-- Latest DQ results
SELECT source_id, check_type, check_status, result_timestamp
FROM nestle_dev_silver.control.dq_results
WHERE result_timestamp >= CURRENT_DATE() - INTERVAL 1 DAY
ORDER BY result_timestamp DESC;

-- Should show all PASS for latest run
```

---

## Common Tasks

### I need to re-load just one source

**Example: Re-load SharePoint only**

```sql
-- Delete recent ingestions for sharepoint
DELETE FROM nestle_dev_silver.control.ingestion_audit_log
WHERE source_id = 'sharepoint_regional_targets'
  AND DATE(load_timestamp) = CURRENT_DATE();

-- Delete Silver SCD2 rows loaded today
DELETE FROM nestle_dev_silver.core.regional_sales_targets_scd2
WHERE dbt_valid_from = CURRENT_DATE();

-- Reset watermark (SQL Server only, not needed for hash-based)
-- N/A for SharePoint
```

Then: Run just Task 1 & 2 manually from the notebook.

### I need to run just Gold aggregation

1. Open any notebook
2. Run this:
```sql
REFRESH MATERIALIZED VIEW nestle_dev_gold.bi_core.agg_daily_sales;
REFRESH MATERIALIZED VIEW nestle_dev_gold.bi_core.agg_customer_ltv;
REFRESH MATERIALIZED VIEW nestle_dev_gold.bi_core.agg_regional_performance;
```

### I need to check if the watermark is correct

```sql
-- Current watermark state
SELECT * FROM nestle_dev_silver.control.watermark_tracking
WHERE source_id = 'sql_sales_transactions';

-- Should show: last_high_water_mark = max(modified_at) from last run
```

---

## Escalation & Support

### Pipeline Failed

1. **Check the error:** Click task → View logs
2. **Is it a data issue?** Check DQ results table
3. **Is it a cluster issue?** Restart cluster
4. **Is it a job config issue?** Edit job definition, re-run

### Data Looks Wrong

1. **Sample the data:** `SELECT * FROM table LIMIT 100`
2. **Check lineage:** Catalog → Table → Lineage tab
3. **Revert & reload:** Delete bad data, re-run ingestion

### Performance Issues

1. **Check cluster:** Is it adequately sized?
2. **Check data size:** How many GB are we processing?
3. **Optimize queries:** Use EXPLAIN to identify slow joins
