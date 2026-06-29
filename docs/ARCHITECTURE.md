# Architecture — Nestlé Databricks Lakehouse

## Overview

Complete Medallion Architecture with SCD2 history tracking, incremental loading (hash & watermark), and semantic layer for business metrics.

---

## Data Layers

### 🔴 BRONZE LAYER (Raw Data Ingestion)

**Purpose:** Exact copy of source systems, minimal transformation, immutable history.

#### Schemas & Tables

**SharePoint (Excel Files)**
- **Table:** `nestle_dev_bronze.sharepoint.regional_sales_targets`
- **Load Pattern:** Hash-based incremental (compare row fingerprints)
- **Grain:** Product-Region-Period
- **Row Count:** Day 1: 50 → Day 2: 53 → Day 3: 53
- **Columns:** product_id, region, period, target_amount, run_id, ingestion_timestamp
- **Update Logic:** MERGE (insert new, update changed via hash match)

**SQL Server (Transactional Database)**
- **Table:** `nestle_dev_bronze.sql_server.sales_transactions`
- **Load Pattern:** Watermark-based incremental (track modified_at timestamp)
- **Grain:** Transaction (one row = one sale event)
- **Row Count:** Day 1: 300 → Day 2: 350 → Day 3: 385 → Day 4: 410
- **Columns:** transaction_id, product_id, region, channel, customer_id, quantity, unit_price, amount, created_at, modified_at, run_id, ingestion_timestamp
- **Update Logic:** MERGE (insert new, update if modified_at > last_hwm)
- **Watermark Tracking:** `control.watermark_tracking` (last_high_water_mark = 2026-06-23 23:05)

**Data Lake (JSON Events)**
- **Table:** `nestle_dev_bronze.data_lake.web_app_events` (FUTURE)
- **Load Pattern:** Auto Loader (streaming-ready)
- **Grain:** Event (one row = one user action)

---

### 🟡 SILVER LAYER (Business Logic & History)

**Purpose:** Cleaned, deduplicated, business-ready data with SCD2 temporal tracking.

#### Slowly Changing Dimension Type 2 (SCD2)

**SharePoint Regional Targets**
- **Table:** `nestle_dev_silver.core.regional_sales_targets_scd2`
- **Type:** Dimension (with history)
- **Grain:** Product-Region-Period with temporal validity
- **SCD2 Columns:** 
  - `dbt_valid_from` — date row became active
  - `dbt_valid_to` — date row expired (2099-12-31 if current)
  - `dbt_is_current` — TRUE if this is the active version
- **Example History (P001/Asia/2026-Q2):**
  ```
  Day 1: 14065.89 | valid_from=2026-06-20 | valid_to=2099-12-31 | is_current=TRUE
  Day 2: 14065.89 | valid_from=2026-06-20 | valid_to=2026-06-23 | is_current=FALSE  (expired)
  Day 2: 18988.95 | valid_from=2026-06-23 | valid_to=2099-12-31 | is_current=TRUE   (new version)
  Day 3: 18988.95 | valid_from=2026-06-23 | valid_to=2026-06-23 | is_current=FALSE
  Day 3: 22000.00 | valid_from=2026-06-23 | valid_to=2099-12-31 | is_current=TRUE
  ```
- **Total Rows:** 50 original + 5 history rows (expired) + 3 new = 58 rows for day 3

**SQL Server Sales Transactions**
- **Table:** `nestle_dev_silver.core.sql_sales_transactions_scd2`
- **Type:** Fact with SCD2 (uncommon but useful for audit trail)
- **Grain:** Transaction with version tracking
- **Total Rows:** 410 current + ~60 historical versions = ~470 total

---

### 🟢 GOLD LAYER (Analytics & BI)

**Purpose:** Pre-aggregated, denormalized, performance-optimized for dashboards & reports.

#### Dimension Tables

**Product Dimension** (`d_product`)
- Columns: product_id, category, price_tier
- Rows: 15 unique products
- Lineage: bronze.sql_server.sales_transactions

**Region Dimension** (`d_region`)
- Columns: region, super_region (grouped)
- Rows: 5 regions (North America, Europe, Asia, Latin America, Middle East)
- Super regions: Americas (NA + LA), EMEA (EU + ME), APAC (Asia)

**Customer Dimension** (`d_customer`)
- Columns: customer_id, customer_segment (Enterprise, SMB, Startup)
- Rows: 156 unique customers

#### Fact Table

**Sales Fact** (`f_sales_fact`)
- **Grain:** Transaction (one row = one sale)
- **Row Count:** 410 rows (current snapshot)
- **Columns:**
  - Facts: transaction_id, quantity, unit_price, amount
  - Dimensions: product_id, region, channel, customer_id
  - Time: sales_date, sales_year, sales_month, sales_quarter
  - Attributes: product_category, price_tier, super_region, customer_segment, target_amount, target_category
  - Calculated: transaction_value_segment (High/Medium/Low)
- **Joins:**
  - ← d_product (via product_id)
  - ← d_region (via region)
  - ← d_customer (via customer_id)
  - ← silver.core.regional_sales_targets_scd2 (via product_id, region, where is_current=TRUE)

#### Materialized Views (Pre-computed Aggregations)

**Daily Sales Summary** (`agg_daily_sales`)
- **Grain:** Date, Super Region, Product Category, Channel
- **Metrics:** num_transactions, units_sold, revenue, avg_transaction_value, high_value_txns
- **Refresh:** Daily after gold.f_sales_fact updates
- **Use Case:** Dashboards, daily performance reports

**Customer Lifetime Value** (`agg_customer_ltv`)
- **Grain:** Customer, Customer Segment
- **Metrics:** lifetime_transactions, lifetime_units, lifetime_revenue, avg_value, customer_days_active
- **Use Case:** Segmentation, retention analysis, CLV reports

**Regional Performance** (`agg_regional_performance`)
- **Grain:** Super Region, Year, Quarter
- **Metrics:** quarterly_revenue, quarterly_units, unique_customers, above_target_revenue
- **Use Case:** Executive dashboards, regional KPI tracking

---

### 🟣 SEMANTIC LAYER (Business Metrics & Definitions)

**Purpose:** Single source of truth for business metrics, dimensions, rules, and query templates.

#### Files

**metrics/sales_metrics.yml**
- Revenue: SUM(amount)
- Units Sold: SUM(quantity)
- Avg Transaction Value: SUM(amount) / COUNT(txns)
- Customer Lifetime Revenue: SUM(amount) per customer
- Revenue vs Target %: Actual / Target * 100

**dimensions/product_dimension.yml**
- product_id, category, price_tier
- Attributes for filtering/grouping

**business_rules/sales_rules.yml**
- Fiscal calendar: Calendar year (Jan-Dec)
- Valid channels: Online, Retail, B2B, Wholesale
- Price thresholds: High ($5K), Medium ($1K), Low
- Data retention: 7 years transactional, 10 years archived

**templates/daily_sales_summary.sql**
- Pre-built query for analysts
- Filters: last 30 days, non-excluded regions
- Aggregations: by date, region, category, channel

---

## Data Flow (End-to-End)

```
Day 1:
SharePoint (50 rows) ──┐
SQL Server (300)  ─────┼──→ BRONZE ──→ SILVER (SCD2) ──┐
Data Lake (N/A) ───────┘                                 │
                                                         ├──→ GOLD (Fact + Dims) ──→ Materialized Views ──→ BI Tools
                                                         │
                                                    SEMANTIC (Metrics)
                                                         
Day 2:
SharePoint (53, +8 delta) ──────┐
SQL Server (350, +70 delta) ─────┼──→ BRONZE (incremental) ──→ SILVER (SCD2, expires old) ──┐
                                 │                                                             │
                                 └─────────────────────────────────────────────────────────────┴──→ GOLD ──→ Dashboards
```

---

## Incremental Load Patterns

### Pattern 1: Hash-Based (SharePoint)

**How it works:**
1. Compute SHA-256 hash of `(product_id, region, period, target_amount)`
2. Compare incoming hashes against hashes already in Bronze
3. If hash matches → row is identical, skip it
4. If hash differs → row is new or changed, insert/update it

**Efficiency:** Only processes changed rows

**Example:**
```
Day 1: 50 rows → All new → hash not in Bronze → INSERT all 50
Day 2: 53 rows (45 unchanged + 5 changed + 3 new)
  - 45 rows: hash matches Bronze → SKIP
  - 5 rows: hash differs → UPDATE
  - 3 rows: hash not in Bronze → INSERT
  Result: read 53, wrote 8 ✅
```

### Pattern 2: Watermark-Based (SQL Server)

**How it works:**
1. Track `modified_at` column (when each row was last changed)
2. Store max `modified_at` from previous load in `watermark_tracking` table
3. On next load, read only rows where `modified_at > last_high_water_mark`
4. MERGE: update if key exists, insert if key is new
5. Update watermark to new max `modified_at`

**Efficiency:** Database-level filtering (no full table scan)

**Example:**
```
Day 1: last_hwm = 1900-01-01 (first load)
  Read from SQL Server: modified_at > 1900-01-01 → 300 rows → INSERT all
  Update hwm to 2026-06-20 23:57

Day 2: last_hwm = 2026-06-20 23:57
  Read from SQL Server: modified_at > 2026-06-20 23:57 → 70 rows (20 modified, 50 new)
  MERGE: 20 UPDATE, 50 INSERT
  Update hwm to 2026-06-21 23:38
  
Result: read 350 total, wrote 70 ✅
```

---

## SCD2 Mechanics (How History is Tracked)

**Expire-and-Insert Pattern:**

```
INPUT: Bronze has P001/Asia/2026-Q2 with new amount 18988.95
       Silver currently has P001/Asia/2026-Q2 with old amount 14065.89 (is_current=TRUE)

Step 1 (Expire):
  UPDATE silver
  SET dbt_valid_to = today, dbt_is_current = FALSE
  WHERE is_current = TRUE AND key matches AND amount differs

Step 2 (Insert):
  INSERT into silver
  SELECT product_id, region, period, 18988.95, 
         today as dbt_valid_from, 2099-12-31 as dbt_valid_to, TRUE as is_current

RESULT:
  Old row: 14065.89 | valid_from=day1 | valid_to=day2 | is_current=FALSE
  New row: 18988.95 | valid_from=day2 | valid_to=2099-12-31 | is_current=TRUE
  
Query "what was the amount on day 1?" returns 14065.89 ✓
Query "what is current amount?" returns 18988.95 ✓
Full history preserved ✓
```

---

## Lineage & Dependencies

```
Bronze Tables
  ├── sharepoint.regional_sales_targets
  │   └─→ Silver: core.regional_sales_targets_scd2
  │        └─→ Gold: d_region (dimension), f_sales_fact (fact)
  │            └─→ Materialized Views: agg_daily_sales, agg_regional_performance
  │
  ├── sql_server.sales_transactions
  │   └─→ Silver: core.sql_sales_transactions_scd2
  │        └─→ Gold: d_product, d_customer, f_sales_fact
  │            └─→ Materialized Views: agg_daily_sales, agg_customer_ltv
  │
  └── data_lake.web_app_events (FUTURE)
      └─→ Silver: core.web_app_events
           └─→ Gold: f_events_fact
               └─→ BI Tools
```

---

## Control Tables (Tracking & Governance)

**ingestion_audit_log**
- Records every load: source_id, load_type, rows_read, rows_written, status, timestamp
- Used for: troubleshooting, SLA monitoring, data quality tracking

**watermark_tracking**
- Stores last_high_water_mark for each watermark-based source
- Used for: incremental load logic

**dq_config**
- Defines DQ rules: pk_columns, hash_columns, checks
- Used for: automating DQ validation

**dq_results**
- Records results of every DQ run: check_type, status, details
- Used for: data quality monitoring

---

## Next Steps

1. Bronze: SharePoint (hash) + SQL Server (watermark)
2. Silver: SCD2 for both
3. Gold: Fact + dimensions + materialized views
4. Semantic: Metrics, dimensions, rules, templates
5. Data Lake JSON: Auto Loader pattern (future)
6. Multi-environment: Deploy to TEST/PROD via DAB (future)
7. CI/CD: GitHub Actions (future)

---

## References

- [Databricks Medallion Architecture](https://www.databricks.com/blog/2022/06/24/prescriptive-guidance-on-the-medallion-lakehouse-architecture.html)
- [SCD Type 2 Implementation](https://docs.databricks.com/en/patterns/scd-type-2-sql.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/en/data-governance/unity-catalog/)
- [Delta Lake Optimization](https://docs.databricks.com/en/delta/index.html)
