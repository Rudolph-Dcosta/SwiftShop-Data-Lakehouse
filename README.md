# SwiftShop-Data-Lakehouse
A production-grade Medallion Lakehouse on Databricks/GCP

SwiftShop: End-to-End Medallion Lakehouse
Project Overview
This project implements a production-grade Medallion Architecture on Databricks to process retail data from Google Cloud Storage (GCS). It transforms raw POS, CRM, and Web Event data into actionable executive insights, featuring historical tracking and automated orchestration.

Architecture Diagram
Source: Google Cloud Storage (GCS) - Raw CSV/JSON Landing Zone.
Ingestion (Bronze): Incremental loading using Spark Auto Loader for schema evolution and cost efficiency.
Transformation (Silver): Data cleaning and SCD Type 2 (Slowly Changing Dimensions) implementation to track customer profile history.
Analytics (Gold): Aggregated business views for Executive KPIs, Revenue Segmentation, and Conversion Analysis.
Orchestration: Databricks Workflows (Jobs) for automated, dependency-aware execution.

Tech Stack
Platform: Databricks (Lakehouse)
Language: PySpark (Spark SQL & Python)
Storage: Delta Lake (for ACID transactions & Time Travel)
Cloud: Google Cloud Platform (GCS, Service Accounts)
Visualization: Databricks SQL Dashboards
Compute: Job Clusters & Serverless


Key Engineering Features
1. Automated Data Evolution
I built a custom Python data generator that simulates real-world business growth by injecting new records and updating existing ones, testing the pipeline's robustness against changing data.

2. SCD Type 2 Implementation
Instead of overwriting customer data, the pipeline uses the Delta MERGE command to maintain a full history of customer segments. This allows the business to see exactly when a customer moved from "Standard" to "Premium."

4. Resilience & Optimization
Auto Loader: Implemented cloudFiles to handle 30-minute incremental batches without re-processing old files.
Job Clusters: Configured dedicated Job Clusters to reduce compute costs by ~30% compared to interactive clusters.
Identity Management: Integrated GCP Service Accounts for secure, hands-off authentication.


Executive Insights
The final dashboard provides four critical views:
Revenue Health: LTV (Lifetime Value) and Revenue by Segment.
Growth Metrics: New vs. Returning customer counts.
Conversion Funnel: Web-to-Purchase conversion rates segmented by Platform (iOS/Android/Web).
Data Quality Audit: A real-time "Health Table" that flags negative amounts, null IDs, or schema mismatches.










