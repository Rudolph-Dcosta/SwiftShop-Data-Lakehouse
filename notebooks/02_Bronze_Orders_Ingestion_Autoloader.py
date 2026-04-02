# Databricks notebook source
checkout_path = "/Volumes/project_lakehouse/bronze_layer/credentials/checkpoints/orders"
schema_path = "/Volumes/project_lakehouse/bronze_layer/credentials/schemas/orders"

BUCKET_NAME = "swiftshop-datalake-bronze" 

orders_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("header", True)
    .load(f"gs://{BUCKET_NAME}/raw/pos_system/orders/")) 

(orders_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkout_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("project_lakehouse.bronze_Layer.orders_raw"))

print("Auto Loader has finished the initial ingestion of orders")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from project_lakehouse.bronze_layer.orders_raw;