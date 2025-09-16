## Mini Data Platform: End-to-End ETL Pipeline with Docker for Logistics and Supply Chain Analytics

### Overview
This project is a lightweight end-to-end ETL data platform for logistics and supply chain analytics, built entirely with Docker Compose. It demonstrates how teams can collect, process, store, and visualize data to directly drive business value.

### Project Description
The Mini Data Platform integrates four core open-source components:

PostgreSQL – relational database for structured data storage.

Apache Airflow – orchestrates and schedules ETL pipelines.

MinIO – scalable, S3-compatible file storage for raw data.

Metabase – interactive dashboards and reports for analytics.

Data Flow:
Upload raw CSV files → Airflow processes and cleans data → results stored in PostgreSQL → Metabase visualizes insights.

### Architecture
