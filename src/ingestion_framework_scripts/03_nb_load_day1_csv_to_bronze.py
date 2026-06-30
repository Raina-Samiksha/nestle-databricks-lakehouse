from pyspark.sql.functions import current_timestamp
import pandas as pd

pdf = pd.read_csv("/Volumes/nestle_dev_bronze/data_lake/landing/sql_sales_transactions_day1.csv")
df_day1 = spark.createDataFrame(pdf)
df_day1 = df_day1.withColumn("ingestion_timestamp", current_timestamp())
df_day1.write.mode("append").saveAsTable("nestle_dev_bronze.sql_server.sales_transactions")
print(f"✅ Day 1: {df_day1.count()} rows written")