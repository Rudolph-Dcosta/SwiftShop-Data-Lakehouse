# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table project_lakehouse.gold_layer.customer_360_analytics
# MAGIC as 
# MAGIC with active_customers as (
# MAGIC   select * from project_lakehouse.silver_layer.dim_customers
# MAGIC   where is_current = true
# MAGIC ),
# MAGIC order_status as (
# MAGIC   select
# MAGIC     customer_id,
# MAGIC     sum(amount) as lifetime_value,
# MAGIC     count(order_id) as total_orders,
# MAGIC     max(order_date_final) as last_purchase
# MAGIC   from project_lakehouse.silver_layer.fact_orders
# MAGIC   group by customer_id
# MAGIC ),
# MAGIC web_stats as (
# MAGIC   select
# MAGIC     user_id,
# MAGIC     count(*) as website_sessions,
# MAGIC     max(marketing_campaign_id) as attribution_campaign,
# MAGIC     max(event_timestamp) as last_web_visit
# MAGIC   from project_lakehouse.silver_layer.fact_web_events
# MAGIC   group by user_id
# MAGIC )
# MAGIC select 
# MAGIC   c.customer_id,
# MAGIC   c.email,
# MAGIC   c.segment,
# MAGIC   c.country,
# MAGIC   coalesce(o.lifetime_value, 0) as lifetime_value,
# MAGIC   coalesce(o.total_orders, 0) as order_count,
# MAGIC   coalesce(w.website_sessions, 0) as web_engagement,
# MAGIC   w.attribution_campaign,
# MAGIC   greatest(o.last_purchase, w.last_web_visit) as last_interaction
# MAGIC from active_customers c
# MAGIC left join order_status o on c.customer_id = o.customer_id
# MAGIC left join web_stats w on c.customer_id = w.user_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_lakehouse.gold_layer.customer_360_analytics
# MAGIC where attribution_campaign is not null
# MAGIC or lifetime_value > 300
# MAGIC order by lifetime_value desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM project_lakehouse.gold_layer.customer_360_analytics 
# MAGIC WHERE attribution_campaign IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe history project_lakehouse.gold_layer.customer_360_analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'version 0 (old)' as version, count(*) as row_count from project_lakehouse.gold_layer.customer_360_analytics version as of 0
# MAGIC union all
# MAGIC select 'version 1 (new)' as version, count(*) as row_count from project_lakehouse.gold_layer.customer_360_analytics version as of 3