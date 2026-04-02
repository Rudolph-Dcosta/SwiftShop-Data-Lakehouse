# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp,regexp_replace, when
from delta.tables import DeltaTable

df_bronze_orders = spark.read.table("project_lakehouse.bronze_layer.orders_raw")

df_orders_cleaned = df_bronze_orders.select(
    col("order_id").cast("long"),
    col("customer_id").cast("long"),
    col("amount").cast("decimal(10,2)"),
    regexp_replace(col("order_date"), '"', "").alias("clean_date"),
    col("status"),
    col("store_id").cast("int")
).select(
    "*",
    col("clean_date").cast("timestamp").alias("order_date_final")
).drop("clean_date", "order_date")


df_silver_orders = df_orders_cleaned.filter(
        (col("amount") > 0) &
        (col("customer_id").isNotNull())
    ).withColumn("processed_at", current_timestamp())\
    .withColumn("source_file", col("_metadata.file_path"))

target_table = "project_lakehouse.silver_layer.fact_orders"

if not spark.catalog.tableExists(target_table):
    df_silver_orders.write.format("delta").saveAsTable(target_table)
else:
    dt = DeltaTable.forName(spark, target_table)
    dt.alias("t").merge(
        df_silver_orders.alias("s"),
        "t.order_id = s.order_id"
    ).whenNotMatchedInsertAll().execute()

print("Silver Orders updated with data quality filters")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_lakehouse.silver_layer.fact_orders

# COMMAND ----------

