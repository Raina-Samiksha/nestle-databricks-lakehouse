# Data Dictionary

## Bronze Layer Tables

### `nestle_dev_bronze.sharepoint.regional_sales_targets`

**Source:** SharePoint Excel file
**Load Type:** Hash-based incremental
**Update Frequency:** Daily
**Row Count:** ~50-53

| Column | Type | Description | Example |
|---|---|---|---|
| product_id | STRING | Product identifier | PROD0100 |
| region | STRING | Geographic region | North America |
| period | STRING | Time period | 2026-Q2 |
| target_amount | DECIMAL(18,2) | Sales target | 14065.89 |
| run_id | STRING | Ingestion run identifier | day_1_load |
| ingestion_timestamp | TIMESTAMP | When loaded | 2026-06-20 14:30:00 |

---

### `nestle_dev_bronze.sql_server.sales_transactions`

**Source:** SQL Server database
**Load Type:** Watermark-based incremental (modified_at)
**Update Frequency:** Daily
**Row Count:** ~300-410

| Column | Type | Description | Example |
|---|---|---|---|
| transaction_id | STRING | Unique transaction ID | TXN000001 |
| product_id | STRING | Product identifier | PROD0100 |
| region | STRING | Sale region | Europe |
| channel | STRING | Sales channel | Online, Retail, B2B, Wholesale |
| customer_id | STRING | Customer identifier | CUST000001 |
| quantity | INTEGER | Units sold | 5 |
| unit_price | DECIMAL(18,2) | Price per unit | 130.00 |
| amount | DECIMAL(18,2) | Total sale (qty × price) | 650.00 |
| created_at | TIMESTAMP | When transaction occurred | 2026-06-20 10:30:00 |
| modified_at | TIMESTAMP | Last modification (watermark) | 2026-06-21 15:45:00 |
| run_id | STRING | Ingestion run ID | day_2_load |
| ingestion_timestamp | TIMESTAMP | When loaded into Bronze | 2026-06-21 16:00:00 |

---

## Silver Layer Tables (SCD2)

### `nestle_dev_silver.core.regional_sales_targets_scd2`

**Type:** Dimension (with Slowly Changing Dimension Type 2 history)
**Source:** `bronze.sharepoint.regional_sales_targets`
**Grain:** Product-Region-Period with temporal validity
**Row Count:** ~58 (50 current + 8 history rows)

| Column | Type | Description | Example |
|---|---|---|---|
| product_id | STRING | Product | PROD0100 |
| region | STRING | Region | Asia |
| period | STRING | Period | 2026-Q2 |
| target_amount | DECIMAL(18,2) | Target (for this version) | 18988.95 |
| target_category | STRING | Category (Low/Medium/High) | High |
| dbt_valid_from | DATE | When this version became active | 2026-06-23 |
| dbt_valid_to | DATE | When this version expired | 2099-12-31 (if current) |
| dbt_is_current | BOOLEAN | Is this the active version? | TRUE |

**Usage Example:**
```sql
-- Get current targets
SELECT * FROM regional_sales_targets_scd2 WHERE dbt_is_current = TRUE;

-- Get historical values
SELECT * FROM regional_sales_targets_scd2 
WHERE product_id = 'PROD0100' AND region = 'Asia'
ORDER BY dbt_valid_from;
```

---

### `nestle_dev_silver.core.sql_sales_transactions_scd2`

**Type:** Fact with SCD2 (uncommon but useful for audit)
**Source:** `bronze.sql_server.sales_transactions`
**Grain:** Transaction with version tracking
**Row Count:** ~410 current + ~60 history = ~470 total

| Column | Type | Description |
|---|---|---|
| transaction_id | STRING | Unique transaction ID |
| product_id | STRING | Product |
| region | STRING | Region |
| channel | STRING | Sales channel |
| customer_id | STRING | Customer |
| quantity | INTEGER | Quantity (current version) |
| unit_price | DECIMAL(18,2) | Price (current version) |
| amount | DECIMAL(18,2) | Total (current version) |
| created_at | TIMESTAMP | Original transaction date |
| dbt_valid_from | DATE | When this version became active |
| dbt_valid_to | DATE | When this version expired |
| dbt_is_current | BOOLEAN | Is this the current version? |

---

## Gold Layer Tables

### `nestle_dev_gold.bi_core.f_sales_fact`

**Type:** Fact table (denormalized)
**Grain:** Transaction with all dimensions joined
**Row Count:** 410

| Column | Type | Source | Description |
|---|---|---|---|
| transaction_id | STRING | bronze | Transaction ID |
| product_id | STRING | bronze | Product |
| region | STRING | bronze | Region |
| channel | STRING | bronze | Sales channel |
| customer_id | STRING | bronze | Customer |
| quantity | INTEGER | bronze | Qty sold |
| unit_price | DECIMAL | bronze | Price |
| amount | DECIMAL | bronze | Total |
| sales_date | DATE | bronze (created_at) | Sale date |
| sales_year | INTEGER | bronze | Year |
| sales_month | INTEGER | bronze | Month |
| sales_quarter | INTEGER | bronze | Quarter |
| product_category | STRING | d_product | Electronics/Apparel/etc |
| price_tier | STRING | d_product | Premium/Standard/Budget |
| super_region | STRING | d_region | Americas/EMEA/APAC |
| customer_segment | STRING | d_customer | Enterprise/SMB/Startup |
| target_amount | DECIMAL | silver | SCD2 current target |
| target_category | STRING | silver | Low/Medium/High |
| transaction_value_segment | STRING | calculated | High/Medium/Low value |

---

### `nestle_dev_gold.bi_core.d_product`

**Type:** Dimension
**Row Count:** 15 unique products

| Column | Type | Example |
|---|---|---|
| product_id | STRING | PROD0100 |
| category | STRING | Electronics |
| price_tier | STRING | Premium |

---

### `nestle_dev_gold.bi_core.d_region`

**Type:** Dimension
**Row Count:** 5 regions

| Column | Type | Example |
|---|---|---|
| region | STRING | North America |
| super_region | STRING | Americas |

---

### `nestle_dev_gold.bi_core.d_customer`

**Type:** Dimension
**Row Count:** 156 customers

| Column | Type | Example |
|---|---|---|
| customer_id | STRING | CUST000001 |
| customer_segment | STRING | Enterprise |

---

## Control Tables

### `nestle_dev_silver.control.ingestion_audit_log`

**Purpose:** Track every ingestion run

| Column | Type | Description |
|---|---|---|
| source_id | STRING | sharepoint_regional_targets, sql_sales_transactions |
| load_type | STRING | hash, watermark, scd2 |
| rows_read | INTEGER | Rows from source |
| rows_written | INTEGER | Rows inserted/updated |
| status | STRING | SUCCESS, FAILED |
| load_timestamp | TIMESTAMP | When load ran |

---

### `nestle_dev_silver.control.watermark_tracking`

**Purpose:** Store last high-water mark for incremental loads

| Column | Type | Description |
|---|---|---|
| source_id | STRING | sql_sales_transactions |
| watermark_column | STRING | modified_at |
| last_high_water_mark | TIMESTAMP | Max modified_at from last load |
| updated_at | TIMESTAMP | When this record was updated |

---

### `nestle_dev_silver.control.dq_config`

**Purpose:** Define DQ rules for each source

| Column | Type | Description |
|---|---|---|
| source_id | STRING | sharepoint_regional_targets, sql_sales_transactions |
| target_table | STRING | Full table path |
| pk_columns | STRING | Primary key columns (comma-separated) |
| hash_columns | STRING | Columns to hash (for hash-based) |
| check_configs | STRING | DQ checks: row_count, pk_not_null, duplicate_pk, not_null |

---

### `nestle_dev_silver.control.dq_results`

**Purpose:** Store results of every DQ run

| Column | Type | Description |
|---|---|---|
| source_id | STRING | sharepoint_regional_targets |
| check_type | STRING | row_count, pk_not_null, duplicate_pk, not_null |
| check_status | STRING | PASS, FAIL |
| check_details | STRING | Details (e.g., "53 rows loaded") |
| result_timestamp | TIMESTAMP | When check ran |

---

## Materialized Views

### `nestle_dev_gold.bi_core.agg_daily_sales`

**Grain:** Date, Super Region, Product Category, Channel
**Metrics:** num_transactions, units_sold, revenue, avg_transaction_value, high_value_txns

---

### `nestle_dev_gold.bi_core.agg_customer_ltv`

**Grain:** Customer, Customer Segment
**Metrics:** lifetime_transactions, lifetime_units, lifetime_revenue, customer_days_active

---

### `nestle_dev_gold.bi_core.agg_regional_performance`

**Grain:** Super Region, Year, Quarter
**Metrics:** quarterly_revenue, quarterly_units, unique_customers, above_target_revenue

