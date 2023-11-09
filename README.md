# homebase_assignment
# Heart Disease Data Analysis Repository

This repository contains scripts and configurations for extracting, transforming, loading (ETL), and analyzing heart disease data.

## Prerequisites

Before proceeding, ensure you have the following prerequisites installed:

- Python 3.8 or higher
- Airflow
- PostgreSQL database
- ClickHouse database

## Data Extraction and Transformation

1. **Extract data from source:**

   Run the `etl.py` script to extract data from the source system and load it into a staging table in PostgreSQL.

   ```bash
   python etl.py
   

## Data Loading and Analysis

1. **Create ClickHouse DAG:**

   Copy the `postgres_to_clickhouse_dag.py` file to the DAGS folder of your Airflow installation. This file defines an Airflow DAG that will periodically transfer data from PostgreSQL to ClickHouse.

   bash
   cp postgres_to_clickhouse_dag.py <your-airflow-dags-folder>/
   

2. **Trigger ClickHouse DAG:**

   Trigger the ClickHouse DAG to load data from PostgreSQL to ClickHouse. You can do this manually from the Airflow web interface or schedule it to run automatically.

3. **Analyze data in ClickHouse:**

   Use the `heart_disease_query.sql` file to explore and analyze the heart disease data in ClickHouse. This file contains sample queries to get insights into the data.

   sql
   CREATE DATABASE IF NOT EXISTS heart_disease;

   USE heart_disease;

   -- Load data from ClickHouse
   SELECT * FROM heart_disease_data;

   -- Analyze data using SQL queries
   SELECT COUNT(*) FROM heart_disease_data WHERE diagnosis = 'Heart Disease';
   SELECT age_group, COUNT(*) AS count FROM heart_disease_data GROUP BY age_group;
   

## Contributing

Contributions are welcome! Please feel free to open issues or pull requests with suggestions, improvements, or bug fixes.