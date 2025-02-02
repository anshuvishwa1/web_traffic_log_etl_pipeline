# ETL Pipeline for Web Traffic Logs

This Jupyter Notebook demonstrates an ETL (Extract, Transform, Load) pipeline for processing web traffic logs. The pipeline fetches data from a demo API, processes it, and stores it in an SQLite database. It also includes steps to filter and aggregate the data.

## File Structure

- `etl_pipeline.ipynb` – Jupyter Notebook containing the ETL pipeline implementation.
- `etl_pipeline.py` – Standalone Python script for executing the ETL pipeline.
- `fashion_brand_logs.db` – SQLite database file used for storing processed data.
- `requirements.txt` – List of dependencies required to run the ETL pipeline.
- `Dockerfile` – Configuration file for containerizing the application using Docker.
- `README.md` – Documentation explaining the project's functionality, setup, and execution.

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
- This project includes a Dockerfile to containerize the Python code(`etl_pipeline.py`) for consistent execution across environments. The ETL pipeline (etl_pipeline.py) and dependencies (requirements.txt) are packaged within Docker, allowing for seamless deployment.
- `etl_pipeline.ipynb` notebook can be executed directly or orchestrated via Apache Airflow, Azure Data Factory, or other workflow automation tools. 

## Setup and Execution Instructions

Follow these steps to create and run the application:

1. Navigate to the directory containing the application code.
2. Build the Docker image:
    ```sh
    docker build -t etl_pipeline .
    ```
3. Run the container using the following command:
    ```sh
    docker run etl_pipeline
    ```
