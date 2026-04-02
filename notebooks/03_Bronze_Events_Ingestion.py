# Databricks notebook source
events_checkpoint = "/Volumes/project_lakehouse/bronze_layer/credentials/checkpoints/web_events"
events_schema_hint = "/Volumes/project_lakehouse/bronze_layer/credentials/schemas/web_events"

BUCKET_NAME = "swiftshop-datalake-bronze" 

events_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", events_schema_hint)
    .option("coudFiles.inferColumnTypes", "true")
    .load(f"gs://{BUCKET_NAME}/raw/web_platform/events/"))

(events_stream.writeStream
    .format("delta")
    .option("checkpointLocation", events_checkpoint)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("project_lakehouse.bronze_layer.events_raw"))

print("Web Events ingested")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from project_lakehouse.bronze_layer.events_raw;