# Databricks notebook source
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable

df_source = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("gs://swiftshop-datalake-bronze/raw/crm/customers/customers_master.csv"))

target_table = "project_lakehouse.silver_layer.dim_customers"

if not spark.catalog.tableExists(target_table):
    (df_source.withColumn("is_current", lit(True))
                .withColumn("start_date", current_timestamp())
                .withColumn("end_date", lit(None).cast("timestamp"))
                .write.format("delta")
                .saveAsTable(target_table))
    print("Initilized SCD Type 2 table.")

else:
    print("SCD Type 2 Merge")
    dt = DeltaTable.forName(spark, target_table)

    df_updates = df_source.alias("s").join(
        spark.read.table(target_table).alias("t"),
        (col("s.customer_id") == col("t.customer_id")) & (col("t.is_current") == True)).where("s.segment<> t.segment or s.country <> t.country")\
            .select("s.*")

        
    dt.alias("t").merge(
            df_updates.alias("s"),
            "t.customer_id = s.customer_id and t.is_current = True"
        ).whenMatchedUpdate(set = {
            "is_current": "false",
            "end_date": "current_timestamp()"
        }).execute()


    df_to_insert = df_source.alias("s").join(spark.read.table(target_table).where("is_current = true").alias("t"),
        "customer_id",
        "left_anti"
    )
        
    (df_to_insert.withColumn("is_current", lit(True))
                .withColumn("start_date", current_timestamp())
                .write.format("delta")
                .mode("append")
                .saveAsTable(target_table))

print("SCD Type 2 Transformation complete")



# df_cust_silver = df_cust_raw.select(
#     col("customer_id").cast("long"),
#     "first_name",
#     "email",
#     "country",
#     "segment"
#     ).dropDuplicates(["customer_id"])

# df_cust_silver.write.format("delta")\
#     .mode("overwrite")\
#     .saveAsTable("project_lakehouse.silver_layer.dim_customers")

# print("Silver Dimention 'dim_customers' created via Batch Read")

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history project_lakehouse.silver_layer.dim_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select "previous_version" as version, count(*) as row_count from project_lakehouse.silver_layer.dim_customers version as of 1
# MAGIC union all
# MAGIC select "current_version" as version, count(*) as row_count from project_lakehouse.silver_layer.dim_customers version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     customer_id, 
# MAGIC     first_name, 
# MAGIC     segment, 
# MAGIC     country, 
# MAGIC     is_current, 
# MAGIC     start_date, 
# MAGIC     end_date
# MAGIC FROM project_lakehouse.silver_layer.dim_customers
# MAGIC WHERE customer_id = 1
# MAGIC ORDER BY start_date ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     is_current, 
# MAGIC     COUNT(*) as record_count 
# MAGIC FROM project_lakehouse.silver_layer.dim_customers 
# MAGIC GROUP BY is_current;