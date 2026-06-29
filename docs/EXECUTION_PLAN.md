# 2-Day Intensive Execution Plan

## Overview

SQL Server Bronze (Day 1-4 data) → Silver SCD2 → Gold Layer → Semantic Layer.

---

## DAY 1

###Setup & Control Tables

**Do in SQL Editor:**

```sql
-- Watermark tracking table
CREATE TABLE IF NOT EXISTS nestle_dev_silver.control.watermark_tracking (
  source_id STRING,
  watermark_column STRING,
  last_high_water_mark TIMESTAMP,
  updated_at TIMESTAMP
);

INSERT INTO nestle_dev_silver.control.watermark_tracking
SELECT 'sql_sales_transactions','modified_at',CAST('1900-01-01' AS TIMESTAMP),current_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM nestle_dev_silver.control.watermark_tracking WHERE source_id='sql_sales_transactions');

-- Bronze table
CREATE TABLE IF NOT EXISTS nestle_dev_bronze.sql_server.sales_transactions (
  transaction_id STRING, product_id STRING, region STRING, channel STRING, customer_id STRING,
  quantity INTEGER, unit_price DECIMAL(18,2), amount DECIMAL(18,2),
  created_at TIMESTAMP, modified_at TIMESTAMP, run_id STRING, ingestion_timestamp TIMESTAMP
);

-- DQ config
INSERT INTO nestle_dev_silver.control.dq_config (source_id,target_table,pk_columns,check_configs)
SELECT 'sql_sales_transactions','nestle_dev_bronze.sql_server.sales_transactions',
       'transaction_id','row_count,pk_not_null,duplicate_pk,not_null'
WHERE NOT EXISTS (SELECT 1 FROM nestle_dev_silver.control.dq_config WHERE source_id='sql_sales_transactions');

-- Verify
SELECT * FROM nestle_dev_silver.control.watermark_tracking WHERE source_id='sql_sales_transactions';
```

**✓ Expected:** Watermark row created, tables exist.

---

### Create Bronze Notebook

**Create notebook:** `src/ingestion_framework_scripts/06_nb_bronze_watermark_sql_server_scj`

**Paste** the complete watermark-based Bronze notebook code (provided in ARCHITECTURE.md section on incremental loads).

**✓ Expected:** Notebook created and saved.

---

### Load (300 rows)

1. Download `sql_sales_txns_day1.csv`
2. Upload to `/Volumes/nestle_dev_bronze/sql_server/landing/` as `sales_transactions.csv`
3. Run notebook with `run_id = day_1_load`

**✓ Expected output:**
```
SUCCESS
total_rows: 300
max_modified_at: 2026-06-20 23:57:00
```

---

### Day 2 Load (350 rows, +70 incremental)

1. Download & upload `sql_sales_txns_day2.csv`
2. Run notebook with `run_id = day_2_load`

**✓ Expected:**
```
total_rows: 350
max_modified_at: 2026-06-21 23:38:00
```

---

### Day 3 Load (385 rows, +50 incremental)

1. Upload `sql_sales_txns_day3.csv`
2. Run notebook with `run_id = day_3_load`

**✓ Expected:**
```
total_rows: 385
max_modified_at: 2026-06-22 23:44:00
```

---

### Day 4 Load (410 rows, +35 incremental)

1. Upload `sql_sales_txns_day4.csv`
2. Run notebook with `run_id = day_4_load`

**✓ Expected:**
```
total_rows: 410
max_modified_at: 2026-06-23 23:05:00
```

---

### SQL Server Silver SCD2

**Create notebook:** `src/silver/06_nb_silver_scd2_sql_sales_transactions_scj`

**Paste the SCD2 logic** (expire old, insert new, same as SharePoint pattern).

**Run it. Expected:** SCD2 table created with 410+ history rows.

---

### Git Commit + DQ

1. Run DQ: `source_id = sql_sales_transactions` → 4/4 PASS
2. Git commit:
   ```
   feat(bronze,silver): SQL Server watermark incremental + SCD2
   - 4-day load tested (300→410 rows)
   - Watermark pattern proven (155 changes detected)
   ```
3. Push to Git

**✓ Status:** SQL Server complete!

---


### Dimension Tables (Gold)

```sql
CREATE TABLE nestle_dev_gold.bi_core.d_product AS
SELECT DISTINCT product_id,
  CASE WHEN product_id IN ('PROD0100','PROD0101','PROD0102') THEN 'Electronics'
       WHEN product_id IN ('PROD0103','PROD0104','PROD0105') THEN 'Apparel'
       ELSE 'Home & Garden' END AS category,
  CASE WHEN product_id IN ('PROD0100','PROD0101') THEN 'Premium' ELSE 'Standard' END AS price_tier
FROM nestle_dev_bronze.sql_server.sales_transactions;

-- Similar for d_region, d_customer
```

**✓ Expected:** 3 dimension tables created.

---

### Fact Table (Gold)

```sql
CREATE TABLE nestle_dev_gold.bi_core.f_sales_fact AS
SELECT txns.*, prod.category, prod.price_tier, reg.super_region, cust.customer_segment,
       targets.target_amount, targets.target_category
FROM nestle_dev_bronze.sql_server.sales_transactions txns
LEFT JOIN nestle_dev_gold.bi_core.d_product prod ON txns.product_id=prod.product_id
LEFT JOIN nestle_dev_gold.bi_core.d_region reg ON txns.region=reg.region
LEFT JOIN nestle_dev_gold.bi_core.d_customer cust ON txns.customer_id=cust.customer_id
LEFT JOIN nestle_dev_silver.core.regional_sales_targets_scd2 targets 
  ON txns.product_id=targets.product_id AND txns.region=targets.region AND targets.dbt_is_current=TRUE;
```

**✓ Expected:** 410 rows, all dimensions joined.

---

### Materialized Views (Gold)

Create 3 views: `agg_daily_sales`, `agg_customer_ltv`, `agg_regional_performance`

**✓ Expected:** Pre-computed aggregations ready.

---

### Semantic Layer YAML

Create 4 files in `semantic/`:
- `metrics/sales_metrics.yml`
- `dimensions/product_dimension.yml`
- `business_rules/sales_rules.yml`
- `templates/daily_sales_summary.sql`

**✓ Expected:** Semantic layer complete.

---

### Update README

Update main README with:
- Architecture diagram
- Data layers summary
- Incremental patterns explained
- Metrics defined

**✓ Expected:** Documentation complete.

---

### Git Commit All

```
feat(gold,semantic): Complete Gold layer + Semantic layer

Gold: fact + 3 dims + 3 materialized views
Semantic: metrics, dimensions, rules, templates
Docs: README updated

Production-ready lakehouse complete.
```

**Push to Git.**

---

### Update Orchestration Job

Edit `databricks.yml` to add 3 new tasks:
- Task 3: `bronze_ingestion_sql_server`
- Task 4: `silver_scd2_transform_sql_server`
- Task 5: `gold_aggregate_and_refresh`

All with task-level dependencies.

**✓ Expected:** Job updated with 5 tasks.

---

### Final Verification

1. Run full pipeline: **Jobs → Run now**
2. Verify all 5 tasks SUCCEEDED
3. Query each layer to confirm data
4. Final commit

**✓ Status: Complete Enterprise Lakehouse!**

---

## Files to Create in Git

- `docs/ARCHITECTURE.md` ← Already created
- `docs/EXECUTION_PLAN.md` ← This file
- `docs/SEMANTIC_LAYER.md` ← Next
- `docs/RUNBOOK.md` ← Next
- `docs/DATA_DICTIONARY.md` ← Next
- `docs/TROUBLESHOOTING.md` ← Next

All YAML files in `semantic/` folder (already planned).

---

## Success Criteria

**Day 1 End:** SQL Server Bronze + Silver (4 days tested & versioned)
**Day 2 End:** Gold + Semantic layers complete
**Final:** 5-task orchestration job running daily
**Git:** All code + docs versioned
**Production Ready:** Enterprise-grade lakehouse

