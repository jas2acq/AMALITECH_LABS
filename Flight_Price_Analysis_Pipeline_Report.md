# Flight Price Analysis Pipeline Report

## Pipeline Architecture and Execution Flow

The pipeline is orchestrated by a single Airflow DAG, `Main_pipeline`, which runs every 3 hours (`0 */3 * * *`). It processes flight price data through a sequential ETL workflow:

1. **Load Raw Data to MySQL**:
   - Source: `Flight_Price_Dataset_of_Bangladesh.csv` (57,000 rows).
   - Destination: MySQL table `flight_data` in the `mysql-flight` container.
2. **Validate Data**:
   - Reads data from MySQL and performs quality checks.
3. **Transform Data**:
   - Cleans and transforms the validated data, saving it to a CSV in `/opt/airflow/data/processed/flight_data_transformed.csv`.
4. **Compute KPIs**:
   - Calculates KPIs from the transformed data and saves them to `/opt/airflow/data/kpi_outputs/flight_kpis.csv`.
5. **Load to PostgreSQL**:
   - Loads the transformed data into the `flight_data_warehouse` table in the `postgres-analytics` container.

**Execution Flow**:
- The tasks are executed sequentially: `load_csv_to_mysql` → `validate_data` → `transform_data` → `compute_kpis` → `load_to_postgres`.
- Data moves from raw CSV → MySQL → processed CSV → PostgreSQL, with KPIs computed and saved along the way.

## Description of Each Airflow DAG/Task

### DAG: Main_pipeline
- **ID**: `Main_pipeline`
- **Schedule**: Every 3 hours (`0 */3 * * *`)
- **Description**: End-to-end flight price analysis pipeline for Bangladesh.
- **Tags**: `flight_price`, `etl`, `pipeline`
- **Default Args**:
  - Owner: `airflow`
  - Start Date: May 16, 2025
  - Retries: 2 (with 1-minute delay)

#### Task: load_csv_to_mysql
- **Function**: `load_csv_to_mysql`
- **Description**: Ingests raw flight data from `Flight_Price_Dataset_of_Bangladesh.csv` into the `flight_data` table in MySQL. Skips ingestion if the table already contains data.
- **Implementation**:
  - Reads the CSV in chunks of 100 rows to manage memory usage.
  - Inserts data into MySQL using `pandas.to_sql`.
  - Logs progress for each chunk.

#### Task: validate_data
- **Function**: `validate_data`
- **Description**: Performs quality checks on the raw data in the MySQL `flight_data` table.
- **Checks**:
  - Ensures required columns exist (e.g., `Airline`, `Total Fare (BDT)`).
  - Identifies missing/null values in critical columns.
  - Validates numeric columns (e.g., fares are non-negative).
  - Checks date/time formats and categorical columns for empty strings.
  - Validates `Duration (hrs)` and `Days Before Departure` for reasonable ranges.
- **Outcome**: Logs warnings for non-critical issues; raises `ValueError` for critical errors (e.g., missing columns).

#### Task: transform_data
- **Function**: `transform_data`
- **Description**: Cleans and transforms the validated data, saving the result to `flight_data_transformed.csv`.
- **Transformations**:
  - Converts `Departure Date & Time` and `Arrival Date & Time` to datetime.
  - Extracts `Departure Hour`, `Departure DayOfWeek`, and `Arrival Hour`.
  - Ensures numeric columns (e.g., `Total Fare (BDT)`) are properly typed, filling missing values with 0.
  - Handles categorical columns by filling missing values with "Unknown" and stripping whitespace.
  - Creates a `Route` column (`Source-Destination`) and drops redundant columns (`Source Name`, `Destination Name`).

#### Task: compute_kpis
- **Function**: `compute_kpis`
- **Description**: Calculates KPIs from the transformed data and saves them to `flight_kpis.csv`.
- **KPIs**:
  - Average Total Fare per Airline.
  - Number of Flights per Route.
  - Average Duration per Stopover type.
- **Implementation**:
  - Reads transformed data from `flight_data_transformed.csv`.
  - Groups and aggregates data using `pandas`.
  - Combines KPIs into a single CSV file.

#### Task: load_to_postgres
- **Function**: `load_to_postgres`
- **Description**: Loads the transformed data into the `flight_data_warehouse` table in the PostgreSQL analytics database.
- **Implementation**:
  - Reads from `flight_data_transformed.csv`.
  - Loads data in chunks of 5,000 rows to manage memory.
  - Uses `pandas.to_sql` with `if_exists='replace'`.

## KPI Definitions and Computation Logic

The pipeline computes the following KPIs, stored in `/opt/airflow/data/kpi_outputs/flight_kpis.csv`:

1. **Average Total Fare per Airline**:
   - **Definition**: The average total fare (in BDT) for flights operated by each airline.
   - **Computation**: Groups the transformed data by `Airline` and calculates the mean of `Total Fare (BDT)`.
   - **Formula**: `df.groupby('Airline')['Total Fare (BDT)'].mean()`

2. **Number of Flights per Route**:
   - **Definition**: The total number of flights for each route (e.g., `Source-Destination`).
   - **Computation**: Groups the data by `Route` and counts the number of occurrences.
   - **Formula**: `df.groupby('Route').size()`

3. **Average Duration per Stopover Type**:
   - **Definition**: The average flight duration (in hours) for each stopover type (e.g., direct, one stop).
   - **Computation**: Groups the data by `Stopovers` and calculates the mean of `Duration (hrs)`.
   - **Formula**: `df.groupby('Stopovers')['Duration (hrs)'].mean()`

## Challenges Encountered and How They Were Resolved

1. **Long-Running Data Ingestion**:
   - **Challenge**: The initial `data_ingestion` DAG (predecessor to `Main_pipeline`) took over 28 minutes to ingest 57,000 rows into MySQL due to a single large `df.to_sql` operation.
   - **Resolution**:
     - Implemented chunked ingestion in `load_csv_to_mysql.py`, processing 100 rows per chunk to reduce memory usage and improve performance.
     - Increased resources in `docker-compose.yaml` for the `mysql-flight` and `airflow-scheduler` containers (e.g., 1 CPU, 1G memory for MySQL).

2. **Environment Variable Loading**:
   - **Challenge**: The `.env` file was not loading correctly, causing MySQL connection failures.
   - **Resolution**: Ensured the `.env` file was mounted correctly (`./.env:/opt/airflow/.env`) and loaded using `python-dotenv` in the scripts.

3. **Data Validation Issues**:
   - **Challenge**: Missing or malformed data in the raw CSV could cause downstream failures.
   - **Resolution**: Added the `validate_data` task to perform comprehensive checks (e.g., missing columns, invalid dates) before transformation, logging warnings and raising errors for critical issues.