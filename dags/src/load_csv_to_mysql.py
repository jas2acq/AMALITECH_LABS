import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Configure logging for this module
logger = logging.getLogger(__name__)

def load_csv_to_mysql():
    """
    Connects to MySQL, checks if 'flight_data' table is populated.
    If empty or non-existent, reads 'Flight_Price_Dataset_of_Bangladesh.csv'
    and inserts all data in chunks of 100 rows.
    Skips ingestion if the table is already populated.
    """
    user = os.environ.get('FLIGHT_DB_USER')
    password = os.environ.get('FLIGHT_DB_PASSWORD')
    host = os.environ.get('FLIGHT_DB_HOST')
    database = os.environ.get('FLIGHT_DB_NAME')
    table_name = 'flight_data'

    logger.info(f"Attempting to ingest flight data into '{table_name}'.")
    logger.info(f"Connecting to MySQL: User='{user}', Host='{host}', Database='{database}'")

    if not all([user, password, host, database]):
        raise ValueError(f"One or more database environment variables are missing. "
                         f"Please ensure FLIGHT_DB_USER, FLIGHT_DB_PASSWORD, FLIGHT_DB_HOST, "
                         f"and FLIGHT_DB_NAME are set in your .env file and accessible by Airflow. "
                         f"Currently: USER={user}, HOST={host}, DB_NAME={database}")

    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:3306/{database}')

    try:
        table_exists = False
        with engine.connect() as connection:
            check_table_sql = text(
                f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{database}' AND table_name = '{table_name}' LIMIT 1;"
            )
            result = connection.execute(check_table_sql).fetchone()
            if result:
                table_exists = True

            if table_exists:
                count_result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                if count_result > 0:
                    logger.info(f"'{table_name}' table already contains {count_result} records. Skipping data ingestion to prevent duplicates.")
                    return

            logger.info(f"'{table_name}' table is empty or does not exist. Proceeding with data ingestion.")

        raw_dir = os.environ.get('DATA_RAW_DIR', '/opt/airflow/data/raw')
        file_path = f"{raw_dir}/Flight_Price_Dataset_of_Bangladesh.csv"

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Raw data file not found: {file_path}. "
                                    f"Ensure './data/raw/Flight_Price_Dataset_of_Bangladesh.csv' "
                                    f"exists on your host and is mounted correctly to '/opt/airflow/data/raw' "
                                    f"in your Docker Compose setup.")
        logger.info(f"Reading full CSV from: {file_path}")

        df = pd.read_csv(file_path)
        total_rows = len(df)
        logger.info(f"Successfully read {total_rows} rows from CSV.")

        chunk_size = 100
        num_chunks = (total_rows + chunk_size - 1) // chunk_size

        logger.info(f"Starting insertion into '{table_name}' table in chunks of {chunk_size} rows...")

        for i in range(0, total_rows, chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            current_chunk_number = (i // chunk_size) + 1

            logger.info(f"Inserting chunk {current_chunk_number}/{num_chunks} ({len(chunk)} rows)...")
            chunk.to_sql(table_name, engine, if_exists='append', index=False)
            logger.info(f"Chunk {current_chunk_number} inserted successfully.")

        logger.info(f"Successfully inserted all {total_rows} rows into {table_name} table in chunks.")

    except Exception as e:
        logger.error(f"An error occurred during data ingestion: {e}")
        raise