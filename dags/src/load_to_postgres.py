import os
import pandas as pd
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)

def load_to_postgres():
    """
    Reads the final processed data from CSV in 'data/processed' and loads it into PostgreSQL in chunks.
    """
    processed_dir = os.environ.get('DATA_PROCESSED_DIR', '/opt/airflow/data/processed')
    source_file_path = os.path.join(processed_dir, 'flight_data_transformed.csv')

    postgres_user = os.environ.get('POSTGRES_ANALYTICS_USER')
    postgres_password = os.environ.get('POSTGRES_ANALYTICS_PASSWORD')
    postgres_host = os.environ.get('POSTGRES_ANALYTICS_HOST')
    postgres_database = os.environ.get('POSTGRES_ANALYTICS_DB')
    target_table_postgres = 'flight_data_warehouse'

    if not all([postgres_user, postgres_password, postgres_host, postgres_database]):
        raise ValueError(
            "Missing one or more PostgreSQL environment variables. "
            "Please ensure POSTGRES_ANALYTICS_USER, POSTGRES_ANALYTICS_PASSWORD, "
            "POSTGRES_ANALYTICS_HOST, and POSTGRES_ANALYTICS_DB are set."
        )

    logger.info(f"Starting data load from CSV '{source_file_path}' to PostgreSQL '{target_table_postgres}'.")

    try:
        if not os.path.exists(source_file_path):
            raise FileNotFoundError(f"Processed CSV file not found: {source_file_path}. "
                                    "Ensure 'transform_data' task completed successfully and saved the file.")

        df = pd.read_csv(source_file_path)
        total_rows = len(df)
        logger.info(f"Read {total_rows} rows from CSV '{source_file_path}'.")

        if df.empty:
            logger.warning(f"Source CSV '{source_file_path}' is empty. No data to load to PostgreSQL.")
            return

        postgres_engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:5432/{postgres_database}')

        chunk_size = 5000
        logger.info(f"Loading data into PostgreSQL table '{target_table_postgres}' in chunks of {chunk_size} rows.")

        df.to_sql(
            target_table_postgres,
            postgres_engine,
            if_exists='replace',
            index=False,
            chunksize=chunk_size
        )
        logger.info(f"Successfully loaded all {total_rows} rows into PostgreSQL table '{target_table_postgres}'.")

    except Exception as e:
        logger.error(f"An error occurred during loading to PostgreSQL: {e}")
        raise