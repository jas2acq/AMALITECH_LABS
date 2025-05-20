# Setup

## Dependencies
The pipeline relies on the following Python packages, installed via `requirements.txt`:
- `pandas`: For data manipulation and analysis.
- `sqlalchemy`: For database interactions with MySQL and PostgreSQL.
- `mysql-connector-python`: To connect to the MySQL database.
- `psycopg2-binary`: To connect to PostgreSQL databases.
- `python-dotenv`: To load environment variables from a `.env` file.

## Environment Setup
The pipeline runs in a Dockerized environment defined by a `docker-compose.yaml` file. Key components include:

### Services
- **PostgreSQL (Airflow Metadata)**:
  - Image: `postgres:13`
  - Port: `5433:5432`
  - Healthcheck: Ensures database readiness with `pg_isready`.
- **MySQL (Flight Data)**:
  - Image: `mysql:5.7`
  - Port: `3307:3306`
  - Healthcheck: Uses `mysqladmin ping` to confirm availability.
- **PostgreSQL (Analytics)**:
  - Image: `postgres:13`
  - Port: `5434:5432`
  - Healthcheck: Uses `pg_isready` for readiness.
- **Airflow Webserver**:
  - Port: `8080:8080`
  - Healthcheck: Uses `curl` to check `/health` endpoint.
- **Airflow Scheduler**:
  - Healthcheck: Verifies the scheduler job is running.
- **Airflow Init**:
  - Initializes the Airflow environment and database.

### Docker Compose Configuration
- **Base Image**: `apache/airflow:2.8.1-python3.10`
- **Executor**: `LocalExecutor`
- **Volumes**:
  - `./dags:/opt/airflow/dags`: Mounts DAG files.
  - `./logs:/opt/airflow/logs`: Stores task logs.
  - `./data/raw:/opt/airflow/data/raw`: Raw data directory.
  - `./data/processed:/opt/airflow/data/processed`: Processed data directory.
  - `./data/kpi_outputs:/opt/airflow/data/kpi_outputs`: KPI output directory.
  - `./.env:/opt/airflow/.env`: Environment variables.
- **Network**: `airflow_network` (bridge driver).

### Environment Variables
Loaded from `.env`:
- **Airflow**:
  - `AIRFLOW_DB_USER=airflow`
  - `AIRFLOW_DB_PASSWORD=airflow_pass`
  - `AIRFLOW_DB_NAME=airflow`
- **MySQL**:
  - `FLIGHT_DB_USER=flight_user`
  - `FLIGHT_DB_PASSWORD=flight_pass`
  - `FLIGHT_DB_NAME=flight_data`
  - `FLIGHT_DB_HOST=mysql-flight`
- **PostgreSQL (Analytics)**:
  - `POSTGRES_ANALYTICS_DB=flight_analytics_db`
  - `POSTGRES_ANALYTICS_USER=analytics_user`
  - `POSTGRES_ANALYTICS_PASSWORD=1234`
  - `POSTGRES_ANALYTICS_HOST=postgres-analytics`
- **Directories**:
  - `DATA_RAW_DIR=/opt/airflow/data/raw`
  - `DATA_PROCESSED_DIR=/opt/airflow/data/processed`
  - `DATA_KPI_OUTPUTS_DIR=/opt/airflow/data/kpi_outputs`

## Project Structure
The project directory structure is as follows:
- `dags/`: Contains Airflow DAG definitions and source scripts.
  - `pipeline.py`: Defines the `Main_pipeline` DAG.
  - `src/`: Contains task logic scripts.
    - `load_csv_to_mysql.py`: Ingests raw CSV data into MySQL.
    - `validate_data.py`: Validates the ingested data.
    - `transform_data.py`: Transforms and cleans the validated data.
    - `compute_kpis.py`: Computes KPIs from transformed data.
    - `load_to_postgres.py`: Loads transformed data into PostgreSQL.
- `data/`: Stores data at various stages.
  - `raw/`: Contains the raw CSV file (`Flight_Price_Dataset_of_Bangladesh.csv`).
  - `processed/`: Stores transformed data (`flight_data_transformed.csv`).
  - `kpi_outputs/`: Stores computed KPIs (`flight_kpis.csv`).
- `logs/`: Stores Airflow task logs.
- `plugins/`: Custom Airflow plugins (currently unused).
- `.env`: Environment variables for configuration.
- `requirements.txt`: Lists Python dependencies.
- `docker-compose.yaml`: Defines the Docker services.