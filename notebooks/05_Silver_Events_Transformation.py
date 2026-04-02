# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, from_json

df_bronze_events = spark.read.table("project_lakehouse.bronze_layer.events_raw")

df_events_cleaned = df_bronze_events.select(
    col("user_id").cast("long"),
    col("event").alias("event_type"),
    col("ts").cast("timestamp").alias("event_timestamp"),
    col("platform"),
    col("url"),
    col("marketing_campaign_id"),
    col("_rescued_data")
).filter(col("user_id").isNotNull())
 
df_silver_events = df_events_cleaned.withColumn("processed_at", current_timestamp())

df_silver_events.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("project_lakehouse.silver_layer.fact_web_events")

print("Silver Events flattened and cleaned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM project_lakehouse.silver_layer.fact_web_events 
# MAGIC --where platform is  NULL
# MAGIC where marketing_campaign_id is not NULL
# MAGIC