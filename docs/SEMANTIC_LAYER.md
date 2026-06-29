# Semantic Layer Guide

## Overview

Semantic Layer = business-friendly translation layer. Analysts don't write complex SQL; they use predefined metrics, dimensions, and rules.

---

## Metrics (KPIs)

See: `semantic/metrics/sales_metrics.yml`

### Core Metrics

| Metric | Formula | Data Type | Use Case |
|---|---|---|---|
| **Revenue** | SUM(amount) | DECIMAL(18,2) | Total sales |
| **Units Sold** | SUM(quantity) | INTEGER | Volume tracking |
| **Avg Transaction Value** | SUM(amount) / COUNT(txns) | DECIMAL(18,2) | Average order size |
| **Customer LTV** | SUM(amount) per customer | DECIMAL(18,2) | Customer value |
| **Revenue vs Target** | (SUM(amount) / SUM(target)) * 100 | DECIMAL(5,2) | Goal achievement |

---

## Dimensions (Attributes for Filtering/Grouping)

See: `semantic/dimensions/product_dimension.yml`

### Product Dimension
- product_id, category, price_tier

### Region Dimension
- region, super_region

### Customer Dimension
- customer_id, customer_segment

### Time Dimension
- sales_date, sales_year, sales_month, sales_quarter

---

## Business Rules

See: `semantic/business_rules/sales_rules.yml`

- **Fiscal Calendar:** Jan-Dec (calendar year)
- **Valid Channels:** Online, Retail, B2B, Wholesale
- **High-Value Threshold:** $5,000+
- **Data Retention:** 7 years transactional, 10 years archived

---

## Query Templates

See: `semantic/templates/daily_sales_summary.sql`

Pre-built queries for common analyses:
- Daily sales by region & category
- Customer cohort analysis
- Regional performance trending

---

## How to Use

**For Analysts:**
1. Use templated queries from `semantic/templates/`
2. Filter by dimensions (region, product, customer)
3. Aggregate using metrics (revenue, units, LTV)
4. No need to know table joins or business logic

**For Engineers:**
1. Add new metrics to `semantic/metrics/sales_metrics.yml`
2. Update dimensions in `semantic/dimensions/`
3. Update rules in `semantic/business_rules/`
4. Update templates if business processes change
5. Commit to Git

---

## Example: "Show daily revenue by region for last 30 days"

Without Semantic Layer:
```sql
SELECT DATE(txns.created_at) as date, reg.super_region,
       SUM(txns.quantity * txns.unit_price) as revenue
FROM nestle_dev_bronze.sql_server.sales_transactions txns
LEFT JOIN nestle_dev_gold.bi_core.d_region reg ON txns.region = reg.region
WHERE DATE(txns.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND reg.super_region NOT IN (SELECT region FROM semantic.business_rules.excluded_regions)
GROUP BY DATE(txns.created_at), reg.super_region
ORDER BY date DESC;
```

With Semantic Layer:
```sql
-- Use the template
SELECT * FROM semantic.templates.daily_sales_summary
WHERE sales_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);
```

Much simpler! ✓
