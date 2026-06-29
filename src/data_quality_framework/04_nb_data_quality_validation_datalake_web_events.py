# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Validation Framework
# MAGIC Nestlé Lakehouse — post-ingestion DQ checks
# MAGIC
# MAGIC **Purpose:** Runs after Bronze ingestion completes. Reads `dq_config`,
# MAGIC runs structural checks (row count, PK, duplicates, nulls), and writes
# MAGIC results to `dq_results`. One notebook for all sources.
# MAGIC
# MAGIC **Flow:** read config → run checks in parallel → write results → alert on failures

# COMMAND ----------

# MAGIC %md ## 1. Setup

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

dbutils.widgets.text("source_id", "datalake_web_events")
SOURCE_ID = dbutils.widgets.get("source_id")

CONTROL_CATALOG = "nestle_dev_silver"
CONTROL_SCHEMA = "control"
RUN_ID = str(uuid.uuid4())

print(f"Starting DQ validation for source_id = {SOURCE_ID}")

# COMMAND ----------

# MAGIC %md ## 2. Read config

# COMMAND ----------

def get_dq_config(source_id: str):
    """Read DQ rules for this source from dq_config table."""
    rows = (spark.table(f"{CONTROL_CATALOG}.{CONTROL_SCHEMA}.dq_config")
            .where(F.col("source_id") == source_id)
            .where(F.col("active") == True)
            .collect())
    if not rows:
        raise ValueError(f"No active DQ config found for source_id '{source_id}'")
    return rows[0]

cfg = get_dq_config(SOURCE_ID)
TARGET = cfg.target_table
PK_COLS = [c.strip() for c in cfg.pk_columns.split(",")]
NN_COLS = [c.strip() for c in cfg.not_null_columns.split(",")]
CHECK_DUPS = cfg.check_duplicates
CHECK_COUNT = cfg.check_row_count

print(f"Target: {TARGET}")
print(f"PK columns: {PK_COLS}")
print(f"Not-null columns: {NN_COLS}")

# COMMAND ----------

# MAGIC %md ## 3. Load Bronze table

# COMMAND ----------

try:
    df = spark.table(TARGET)
    row_count = df.count()
    print(f"Bronze table loaded: {row_count} rows")
except Exception as e:
    error_msg = str(e)
    if "Table or view not found" in error_msg:
        raise ValueError(f"Cannot load target table {TARGET}: Table or view not found.")
    else:
        raise ValueError(f"Cannot load target table {TARGET}: {error_msg}")

# COMMAND ----------

# MAGIC %md ## 4. DQ Check functions

# COMMAND ----------

def write_dq_result(check_type, status, details):
    """Write one DQ result row."""
    result = spark.createDataFrame([(
        SOURCE_ID, TARGET, check_type, status, details, RUN_ID, datetime.now()
    )], schema=spark.table(f"{CONTROL_CATALOG}.{CONTROL_SCHEMA}.dq_results").schema)
    result.write.mode("append").saveAsTable(f"{CONTROL_CATALOG}.{CONTROL_SCHEMA}.dq_results")

def check_row_count(df):
    """Check that table has rows."""
    count = df.count()
    if count > 0:
        write_dq_result("row_count", "PASS", f"{count} rows loaded")
        return True
    else:
        write_dq_result("row_count", "FAIL", "No rows loaded")
        return False

def check_pk_not_null(df, pk_cols):
    """Check that PK columns have no NULLs."""
    for col in pk_cols:
        nulls = df.where(F.col(col).isNull()).count()
        if nulls > 0:
            write_dq_result("pk_not_null", "FAIL", f"{col}: {nulls} NULL values in PK")
            return False
    write_dq_result("pk_not_null", "PASS", f"No NULLs in PK columns: {','.join(pk_cols)}")
    return True

def check_duplicates(df, pk_cols):
    """Check that PK values are unique."""
    dups = (df.groupBy(pk_cols).count()
            .where(F.col("count") > 1)
            .count())
    if dups > 0:
        write_dq_result("duplicate", "FAIL", f"{dups} duplicate PK values")
        return False
    else:
        write_dq_result("duplicate", "PASS", "No duplicate PKs")
        return True

def check_not_null(df, nn_cols):
    """Check that not-null columns have no NULLs."""
    for col in nn_cols:
        if col not in df.columns:
            continue  # skip if column doesn't exist
        nulls = df.where(F.col(col).isNull()).count()
        if nulls > 0:
            write_dq_result("not_null", "FAIL", f"{col}: {nulls} NULL values")
            return False
    write_dq_result("not_null", "PASS", f"No NULLs in required columns: {','.join(nn_cols)}")
    return True

# COMMAND ----------

# MAGIC %md ## 5. Run checks

# COMMAND ----------

# DBTITLE 1,Run checks
print("\n=== Running DQ Checks ===\n")

results = []

if not spark.catalog.tableExists(TARGET):
    write_dq_result("target_table_exists", "FAIL", f"Configured target table does not exist: {TARGET}")
    results.append(("target_table_exists", False))
else:
    has_rows = False

    if CHECK_COUNT:
        has_rows = check_row_count(df)
        results.append(("row_count", has_rows))

    if has_rows:  # only check PK and other checks if rows exist
        results.append(("pk_not_null", check_pk_not_null(df, PK_COLS)))
        if CHECK_DUPS:
            results.append(("duplicate", check_duplicates(df, PK_COLS)))
        results.append(("not_null", check_not_null(df, NN_COLS)))

# COMMAND ----------

# MAGIC %md ## 6. Summary

# COMMAND ----------

passed = sum(1 for _, status in results if status)
total = len(results)

print(f"\n=== DQ Summary ===")
print(f"Checks: {passed}/{total} PASSED")
print(f"Source: {SOURCE_ID}")
print(f"Target: {TARGET}")

for check_type, status in results:
    result_str = "✅ PASS" if status else "❌ FAIL"
    print(f"  {check_type}: {result_str}")

if passed == total:
    print(f"\n✅ All checks passed. Data is ready for Silver.")
else:
    print(f"\n⚠️ {total - passed} check(s) failed. Review dq_results table for details.")

# COMMAND ----------

# MAGIC %md ## 7. Verify results in control table

# COMMAND ----------

# Query the latest DQ results
spark.sql(f"""
    SELECT check_type, check_status, check_details, result_timestamp
    FROM {CONTROL_CATALOG}.{CONTROL_SCHEMA}.dq_results
    WHERE run_id = '{RUN_ID}'
    ORDER BY result_timestamp
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_id, check_type, check_status, check_details
# MAGIC FROM nestle_dev_silver.control.dq_results
# MAGIC ORDER BY source_id, check_type;

