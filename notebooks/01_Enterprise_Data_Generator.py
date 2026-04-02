# Databricks notebook source
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from google.cloud import storage

VOLUME_KEY_PATH = "/Volumes/project_lakehouse/bronze_layer/credentials/gcp-key.json"
BUCKET_NAME = "swiftshop-datalake-bronze" 

def upload_to_gcs(local_data, gcs_path, is_json=False):
    client = storage.Client.from_service_account_json(VOLUME_KEY_PATH)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    if is_json:
        blob.upload_from_string(json.dumps(local_data), content_type='application/json')
    else:
        blob.upload_from_string(local_data.to_csv(index=False), content_type='text/csv')
    print(f"Data uploaded to {gcs_path}")


# COMMAND ----------

#Generate a unique timestamp for Job Run
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# COMMAND ----------

#Look at your actual Silver table to see where we left off
try:
    existing_df = spark.table("project_lakehouse.silver_layer.dim_customers") \
                           .filter("is_current = true") \
                           .toPandas()
    current_max_id = existing_df['customer_id'].max()
except:
    current_max_id = 0

# COMMAND ----------

#ORDERS: Create a NEW file for every run so Auto Loader sees it
n = 100 
start_id = np.random.randint(10000, 90000)
orders = pd.DataFrame({
    "order_id": np.arange(start_id, start_id + n),
    "customer_id": np.random.randint(1, current_max_id+1, n), # Increased range to include potential new users
    "amount": np.random.uniform(20, 500, n).round(2),
    "order_date": [datetime.now().strftime('%Y-%m-%d %H:%M:%S') for _ in range(n)],
    "store_id": np.random.choice([1, 2, 3, 4, 5], n),
    "status": np.random.choice(["COMPLETED", "PENDING", "CANCELLED"], n, p=[0.8, 0.1, 0.1])
})
upload_to_gcs(orders, f"raw/pos_system/orders/orders_{run_id}.csv")

# COMMAND ----------

#WEB EVENTS: Same timestamp logic
events = [
    {
        "user_id": int(np.random.randint(1, current_max_id+1)), 
        "event": "click", 
        "ts": datetime.now().isoformat(), 
        "platform": np.random.choice(["ios", "android", "web"]), 
        "url": np.random.choice(["/home", "/products/electronics", "/cart", "checkout"])
    } 
    for _ in range(500)
]
upload_to_gcs(events, f"raw/web_platform/events/events_{run_id}.json", is_json=True)



# COMMAND ----------

#CUSTOMERS: The "Growing" Master List
def generate_incremental_customers():

    if not existing_df.empty:
        #Pick 5 random users to "Upgrade" or "Downgrade"
        update_ids = np.random.choice(existing_df['customer_id'], size=min(5, len(existing_df)), replace=False)
        existing_df.loc[existing_df['customer_id'].isin(update_ids), 'segment'] = np.random.choice(["PREMIUM", "STANDARD"])
        print(f"Simulating updates for Customer IDs: {update_ids}")

    new_ids = np.arange(current_max_id + 1, current_max_id + 15 + 1)
    new_customers = pd.DataFrame({
        "customer_id": new_ids,
        "first_name": [f"User_{i}" for i in new_ids],
        "email": [f"user_{i}@swiftshop.com" for i in new_ids],
        "country": np.random.choice(["USA", "Canada", "UK", "Germany", "France"], 15),
        "segment": "NEW"
    })
    print(f"Generated {15} new customers (IDs {new_ids[0]} to {new_ids[-1]})")

    #COMBINE: This is your new "Master List" to be uploaded
    full_master_list = pd.concat([existing_df[["customer_id", "first_name", "email", "country", "segment"]], new_customers], ignore_index=True)
    
    return full_master_list

final_customers = generate_incremental_customers()
upload_to_gcs(final_customers, "raw/crm/customers/customers_master.csv")


# COMMAND ----------

# n = 1000
# orders = pd.DataFrame({
#     "order_id": np.arange(1000, 1000 + n),
#     "customer_id": np.random.randint(1, 500, n),
#     "amount": np.random.uniform(20, 500, n).round(2),
#     "order_date": [(datetime.now() - timedelta(days=np.random.randint(0,30))).strftime('%Y-%m-%d" %H:%M:%S') for _ in range(n)],
#     "store_id": np.random.choice([1, 2, 3, 4, 5], n),
#     "status": np.random.choice(["COMPLETED", "PENDING", "CANCELLED"], n, p=[0.8, 0.1, 0.1])
# })
# upload_to_gcs(orders, "raw/pos_system/orders/orders_batch_1.csv")


# events = [
#     {
#         "user_id": int(np.random.randint(1, 550)), 
#         "event": "click", 
#         "ts": datetime.now().isoformat(), 
#         "platform": np.random.choice(["ios", "android", "web"]), 
#         "url": np.random.choice(["/home", "/products/electronics", "/cart", "checkout"])
#     } 
#     for _ in range(2000)
# ]
# upload_to_gcs(events, "raw/web_platform/events/events_batch_1.json", is_json=True)


# def generate_customers():
#     customers = pd.DataFrame({
#         "customer_id": np.arange(1, 551),
#         "first_name": [f"User_{i}" for i in range(1, 551)],
#         "email": [f"user_name{i}@swiftshop.com" for i in range(1, 551)],
#         "country": np.random.choice(["USA", "Canada", "UK", "Germany", "France"], 550),
#         "segment": np.random.choice(["PREMIUM", "STANDARD", "NEW"], 550, p=[0.2, 0.5, 0.3])
#     })
#     return customers
# customers_df = generate_customers()
# upload_to_gcs(customers_df, "raw/crm/customers/customers_master.csv")


# #Force User 1 to be 'PREMIUM' 
# customers_df.loc[customers_df['customer_id'] == 1, 'segment'] = 'PREMIUM'
# customers_df.loc[customers_df['customer_id'] == 1, 'country'] = 'Germany'
# upload_to_gcs(customers_df, "raw/crm/customers/customers_master.csv")
# print("Source data updated: User 1 is now a Premium customer in Germany")


# new_events = [
#     {
#         "user_id":999,
#         "event": "purchase_attempt",
#         "ts": datetime.now().isoformat(),
#         "platform": np.random.choice(["ios", "android", "web"]), 
#         "url": np.random.choice(["/home", "/products/electronics", "/cart", "checkout"]),
#         "marketing_campaign_id": "SUMMER_SALE_2026"
#     }
#     for _ in range(10)
# ]
# upload_to_gcs(new_events, "raw/web_platform/events/events_batch_2_new_schema.json", is_json=True)
# print("New batch with evolved schema uploaded")