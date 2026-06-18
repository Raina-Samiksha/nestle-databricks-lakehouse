# Databricks notebook source
# MAGIC %md
# MAGIC # Generic Bronze Ingestion Engine
# MAGIC Nestlé Lakehouse — SCJ-aligned metadata-driven framework
# MAGIC
# MAGIC **One notebook for every source.** It reads its instructions from
# MAGIC `nestle_dev_silver.control` and loads Bronze in the appropriate schema.
# MAGIC Nothing about any specific table is hard-coded here — onboarding a new
# MAGIC source is a config insert, not a code change.
# MAGIC
# MAGIC Flow: read config → write FAILED audit row → route by load_type →
# MAGIC load Bronze with audit columns → flip audit row to SUCCESS.

# COMMAND ----------

# MAGIC %md ## 1. Parameters & setup

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
import uuid

# The ONLY input. A thin per-source job passes this in.
dbutils.widgets.text("source_id", "datalake_web_events")
SOURCE_ID = dbutils.widgets.get("source_id")

# Control tables live in nestle_dev_silver.control
CONTROL_CATALOG = "nestle_dev_silver"
CONTROL_SCHEMA = "control"
PIPELINE_NAME = "generic_bronze_ingestion"

# Databricks gives us job/run context; fall back to a generated id locally.
RUN_ID = str(uuid.uuid4())
JOB_ID = "manual"

print(f"Starting ingestion for source_id = {SOURCE_ID}")

# COMMAND ----------

# MAGIC %md ## 2. Read config

# COMMAND ----------

def get_source_config(source_id: str):
    """Read the master registry row for this source."""
    rows = (spark.table(f"{CONTROL_CATALOG}.{CONTROL_SCHEMA}.ingestion_source_config")
            .where(F.col("source_id") == source_id)
            .where(F.col("status_code") == "Active")
            .collect())
    if not rows:
        raise ValueError(f"No Active config found for source_id '{source_id}'")
    return rows[0]

cfg = get_source_config(SOURCE_ID)
TARGET = f"{cfg.target_catalog}.{cfg.target_schema}.{cfg.target_table}"

print(f"type={cfg.ingestion_type}  load_type={cfg.load_type}  target={TARGET}")

# COMMAND ----------

# MAGIC %md ## 3. Audit helpers (the FAILED → SUCCESS pattern)

# COMMAND ----------

def write_preflight_audit():
    """Write a FAILED row BEFORE reading the source. A crash leaves a
    discoverable failure instead of a silent gap."""
    audit = spark.createDataFrame([(
        SOURCE_ID, JOB_ID, RUN_ID, cfg.source_system_name, cfg.source_dataset_name,
        datetime.now(), 0, 0, None, "FAILED", None, PIPELINE_NAME
    )], schema=spark.table(f"{CONTROL_CATALOG}.{CONTROL_SCHEMA}.ingestion_audit_log").schema)
    audit.write.mode("append").saveAsTable(f"{CONTROL_CATALOG}.{CONTROL_SCHEMA}.ingestion_audit_log")

def update_audit_success(rows_read, rows_written, watermark_value):
    """Flip this run's row to SUCCESS with metrics."""
    spark.sql(f"""
        UPDATE {CONTROL_CATALOG}.{CONTROL_SCHEMA}.ingestion_audit_log
        SET status = 'SUCCESS',
            rows_read = {rows_read},
            rows_written = {rows_written},
            watermark_value = {f"'{watermark_value}'" if watermark_value else 'NULL'}
        WHERE run_id = '{RUN_ID}'
    """)

def update_audit_error(message):
    safe = message.replace("'", "''")[:4000]
    spark.sql(f"""
        UPDATE {CONTROL_CATALOG}.{CONTROL_SCHEMA}.ingestion_audit_log
        SET error_details = '{safe}'
        WHERE run_id = '{RUN_ID}'
    """)

# COMMAND ----------

# MAGIC %md ## 4. Standard audit columns
# MAGIC Every Bronze row carries full provenance.

# COMMAND ----------

def add_audit_columns(df):
    return (df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_system_name", F.lit(cfg.source_system_name))
        .withColumn("source_dataset_name", F.lit(cfg.source_dataset_name))
        .withColumn("run_id", F.lit(RUN_ID))
        .withColumn("pipeline_name", F.lit(PIPELINE_NAME)))

# COMMAND ----------

# MAGIC %md ## 5. Source readers

# COMMAND ----------

def read_sql_source(sql_cfg):
    """Read from SQL source. Incremental: only rows newer than the last watermark."""
    base = spark.table(f"{sql_cfg.federated_catalog}.{cfg.source_dataset_name}") \
        if sql_cfg.source_query is None else spark.sql(sql_cfg.source_query)
    wm_col, wm_val = sql_cfg.watermark_column_name, sql_cfg.last_watermark_value
    if wm_col and wm_val:
        base = base.where(F.col(wm_col) > F.lit(wm_val))
    return base

def read_file_source(file_cfg):
    """Read excel / csv / json based on file_type."""
    ft = file_cfg.file_type
    if ft == "csv":
        return spark.read.option("header", True).csv(file_cfg.file_location)
    if ft == "json":
        return spark.read.json(file_cfg.file_location)
    if ft == "excel":
        # requires the spark-excel library on the cluster
        return (spark.read.format("com.crealytics.spark.excel")
                .option("header", True).load(file_cfg.file_location))
    raise ValueError(f"Unsupported file_type '{ft}'")

# COMMAND ----------

# MAGIC %md ## 6. Load patterns

# COMMAND ----------

def full_load(df):
    """Overwrite the whole Bronze table. For small / reference data."""
    add_audit_columns(df).write \
        .option("overwriteSchema", "true") \
        .mode("overwrite").saveAsTable(TARGET)
    return df.count()

def merge_load(df, key_cols):
    """Upsert by business key. Creates the table on first run."""
    keys = [k.strip() for k in key_cols.split(",")]
    df = add_audit_columns(df)
    if not spark.catalog.tableExists(TARGET):
        df.write.saveAsTable(TARGET)
        return df.count()
    tgt = DeltaTable.forName(spark, TARGET)
    cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    tgt.alias("t").merge(df.alias("s"), cond) \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    return df.count()

def hash_load(df, key_cols, hash_cols):
    """Hash-based change detection: only changed rows are merged.
    Used when the source has no reliable change column (e.g. Excel)."""
    cols = [c.strip() for c in hash_cols.split(",")]
    df = df.withColumn("row_hash", F.sha2(F.concat_ws("||", *cols), 256))
    if spark.catalog.tableExists(TARGET):
        existing = spark.table(TARGET).select("row_hash")
        df = df.join(existing, on="row_hash", how="left_anti")  # keep only new/changed
    return merge_load(df, key_cols)

# COMMAND ----------

# MAGIC %md ## 7. Orchestration — route, load, audit

# COMMAND ----------

write_preflight_audit()
try:
    new_watermark = None

    if cfg.ingestion_type == "SQL":
        sql_cfg = spark.table(f"{CONTROL_CATALOG}.{CONTROL_SCHEMA}.sql_source_config") \
            .where(F.col("source_id") == SOURCE_ID).collect()[0]
        src = read_sql_source(sql_cfg)
        rows_read = src.count()

        if cfg.load_type == "full":
            rows_written = full_load(src)
        else:  # incremental
            rows_written = merge_load(src, sql_cfg.key_column_names)
            if sql_cfg.watermark_column_name and rows_read > 0:
                new_watermark = str(src.agg(
                    F.max(sql_cfg.watermark_column_name)).collect()[0][0])
                spark.sql(f"""UPDATE {CONTROL_CATALOG}.{CONTROL_SCHEMA}.sql_source_config
                    SET last_watermark_value = '{new_watermark}'
                    WHERE source_id = '{SOURCE_ID}'""")

    elif cfg.ingestion_type == "FILE":
        file_cfg = spark.table(f"{CONTROL_CATALOG}.{CONTROL_SCHEMA}.file_source_config") \
            .where(F.col("source_id") == SOURCE_ID).collect()[0]
        src = read_file_source(file_cfg)
        rows_read = src.count()

        if cfg.load_type == "full":
            rows_written = full_load(src)
        elif cfg.load_type == "hash":
            rows_written = hash_load(src, file_cfg.key_column_names,
                                     file_cfg.hash_columns)
        else:  # incremental on a file source
            rows_written = merge_load(src, file_cfg.key_column_names)

    else:
        raise ValueError(f"Unknown ingestion_type '{cfg.ingestion_type}'")

    update_audit_success(rows_read, rows_written, new_watermark)
    print(f"SUCCESS — read {rows_read}, wrote {rows_written}")

except Exception as e:
    update_audit_error(str(e))
    print(f"FAILED — {e}")
    raise   # re-raise so the Databricks task is marked failed
