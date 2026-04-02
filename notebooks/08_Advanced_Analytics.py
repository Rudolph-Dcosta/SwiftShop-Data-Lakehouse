# Databricks notebook source
# MAGIC %sql
# MAGIC --Conversion Rate Query
# MAGIC select
# MAGIC   platform,
# MAGIC   count(distinct user_id) as total_users,
# MAGIC   count(distinct customer_id) as buyers,
# MAGIC   round((count(distinct customer_id) / count(distinct user_id)) * 100, 2) as conversion_rate_pct
# MAGIC from project_lakehouse.silver_layer.fact_web_events e
# MAGIC left join project_lakehouse.silver_layer.fact_orders o on e.user_id = o.customer_id
# MAGIC group by platform
# MAGIC order by conversion_rate_pct desc;

# COMMAND ----------

#Dynamic Tiering
from pyspark.sql.functions import col, when

df_gold = spark.read.table("project_lakehouse.gold_layer.customer_360_analytics")
df_segmented = df_gold.withColumn("customer_tier",
    when(col("lifetime_value") > 400, "Platinum")
    .when(col("lifetime_value") > 200, "Gold")
    .otherwise("Silver")
)

df_segmented.groupBy("customer_tier").count().show()
df_segmented.write.format("delta").mode("overwrite").saveAsTable("project_lakehouse.gold_layer.customer_Count_by_Tier")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Data Quality (DQ) Audit
# MAGIC select 
# MAGIC   'Null Customer ID' as error_type,
# MAGIC   count(*) as issue_count from project_lakehouse.silver_layer.fact_orders
# MAGIC where customer_id is null
# MAGIC union all
# MAGIC select 
# MAGIC   'Negative Amount' as error_type,
# MAGIC   count(*) from project_lakehouse.silver_layer.fact_orders
# MAGIC   where amount <= 0
# MAGIC union all
# MAGIC select 
# MAGIC   'Orphaned Events' as error_type,
# MAGIC   count(*) from project_lakehouse.silver_layer.fact_web_events where user_id not in (select customer_id from project_lakehouse.silver_layer.dim_customers);

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI_Executive_Summary
# MAGIC select
# MAGIC   round(sum(lifetime_value), 2) as total_revenue,
# MAGIC   count(distinct customer_id) as total_customers,
# MAGIC   round(avg(lifetime_value), 2) as avg_customer_value,
# MAGIC   sum(order_count) as total_transactions
# MAGIC from project_lakehouse.gold_layer.customer_360_analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC --Revenue_by_Segment
# MAGIC select
# MAGIC   segment,
# MAGIC   round(sum(lifetime_value), 2) as revenue
# MAGIC   from project_lakehouse.gold_layer.customer_360_analytics
# MAGIC   group by segment
# MAGIC   order by revenue desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --Customer_Tier_Distribution
# MAGIC select
# MAGIC   customer_tier,
# MAGIC   count(*) as customer_count
# MAGIC from project_lakehouse.gold_layer.customer_Count_by_Tier
# MAGIC group by customer_tier
# MAGIC order by
# MAGIC   case when customer_tier = 'Platinum' then 1
# MAGIC        when customer_tier = 'Gold' then 2
# MAGIC        else 3 end

# COMMAND ----------

