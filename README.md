# ETL Pipeline for Web Traffic Logs

This Jupyter Notebook demonstrates an ETL (Extract, Transform, Load) pipeline for processing web traffic logs. The pipeline fetches data from a demo API, processes it, and stores it in an SQLite database. It also includes steps to filter and aggregate the data.

## File Structure

- `etl_pipeline.ipynb`: The main notebook that contains the ETL pipeline code.
- `README.md`: This file includes entire information about notebook working.
- `fashion_brand_logs.db`: This is the SQLite database file.

## Database Considerations

- In the current notebook, SQLite is used as the persistent database since this code has been developed in a local environment. SQLite is a lightweight, file-based database that is suitable for development and testing. However, for live production environments, more robust and scalable databases are recommended.
-  For production, use a database suited to your cloud platform and use case: Azure SQL Database or Synapse Analytics (Azure), Amazon RDS or Redshift (AWS), Cosmos DB or DynamoDB (NoSQL), and Delta Lake tables (Databricks) for scalable, ACID-compliant storage.


## Tables and Views Created
- `pageviews`: A table that stores information about API data.
- `channel`: A table that stores information about channels.
- `filtered_pageviews`: A view that stores information about pageview counts per channel.

## Orchestration & Scheduling
- This Python notebook can be orchestrated using tools like Apache Airflow, Azure Data Factory (ADF), or other scheduling frameworks, depending on the cloud environment. 

## Containerization (Docker)
- This notebook does not use Docker since it can be executed directly or orchestrated via Apache Airflow, Azure Data Factory, or other workflow automation tools. Docker is typically used when packaging full-fledged Python applications with a structured entry point (main.py), ensuring consistency across deployments.
