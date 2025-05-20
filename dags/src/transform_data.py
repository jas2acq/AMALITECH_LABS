import os
import pandas as pd
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)

def transform_data():
    """
    Reads validated data from MySQL, performs cleaning and transformations,
    and saves the processed data to a CSV file in the processed directory.
    """
    user = os.environ.get('FLIGHT_DB_USER')
    password = os.environ.get('FLIGHT_DB_PASSWORD')
    host = os.environ.get('FLIGHT_DB_HOST')
    database = os.environ.get('FLIGHT_DB_NAME')
    source_table = 'flight_data' # Assuming raw/validated data is here

    processed_dir = os.environ.get('DATA_PROCESSED_DIR', '/opt/airflow/data/processed')
    processed_file_path = os.path.join(processed_dir, 'flight_data_transformed.csv')

    # Ensure the processed directory exists
    os.makedirs(processed_dir, exist_ok=True)

    logger.info(f"Starting data transformation from '{source_table}' and saving to '{processed_file_path}'.")

    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:3306/{database}')

    try:
        df = pd.read_sql_table(source_table, engine)
        logger.info(f"Read {len(df)} rows from '{source_table}' for transformation.")

        if df.empty:
            logger.warning(f"Source table '{source_table}' is empty. No data to transform.")
            return

        # --- Transformations ---
        for col in ['Departure Date & Time', 'Arrival Date & Time']:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        df['Departure Hour'] = df['Departure Date & Time'].dt.hour
        df['Departure DayOfWeek'] = df['Departure Date & Time'].dt.day_name()
        df['Arrival Hour'] = df['Arrival Date & Time'].dt.hour

        numeric_cols = ['Duration (hrs)', 'Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Total Fare (BDT)', 'Days Before Departure']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        categorical_cols = ['Airline', 'Source', 'Destination', 'Stopovers', 'Aircraft Type', 'Class', 'Booking Source', 'Seasonality']
        for col in categorical_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown').astype(str).str.strip()

        df['Route'] = df['Source'] + '-' + df['Destination']
        df = df.drop(columns=['Source Name', 'Destination Name'], errors='ignore')

        # --- Save the transformed data to CSV ---
        df.to_csv(processed_file_path, index=False)
        logger.info(f"Successfully transformed data and saved {len(df)} rows to '{processed_file_path}'.")

    except Exception as e:
        logger.error(f"An error occurred during data transformation: {e}")
        raise