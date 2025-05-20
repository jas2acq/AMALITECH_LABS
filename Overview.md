# Overview

The Flight Price Analysis Pipeline is an end-to-end ETL (Extract, Transform, Load) workflow designed to process flight price data for Bangladesh. It ingests raw data from a CSV file (`Flight_Price_Dataset_of_Bangladesh.csv`), validates and transforms it, computes key performance indicators (KPIs), and loads the results into a PostgreSQL data warehouse for analytics. Orchestrated using Apache Airflow with a Dockerized environment, the pipeline leverages the `Main_pipeline` DAG to execute tasks sequentially every 3 hours.

## Pipeline Architecture and Execution Flow
The pipeline processes data through the following stages:
1. **Load Raw Data to MySQL**: Ingests data from `Flight_Price_Dataset_of_Bangladesh.csv` into the `flight_data` table in MySQL (`mysql-flight` container).
2. **Validate Data**: Performs quality checks on the raw data in MySQL.
3. **Transform Data**: Cleans and transforms the validated data, saving it to `/opt/airflow/data/processed/flight_data_transformed.csv`.
4. **Compute KPIs**: Calculates KPIs (e.g., average fare per airline) and saves them to `/opt/airflow/data/kpi_outputs/flight_kpis.csv`.
5. **Load to PostgreSQL**: Loads the transformed data into the `flight_data_warehouse` table in the `postgres-analytics` container.

The `Main_pipeline` DAG executes these tasks sequentially: `load_csv_to_mysql` → `validate_data` → `transform_data` → `compute_kpis` → `load_to_postgres`. The pipeline ensures data quality at each step, enabling reliable analytics on flight pricing trends.