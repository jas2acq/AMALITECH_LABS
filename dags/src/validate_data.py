import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import numpy as np

logger = logging.getLogger(__name__)

def validate_data():
    """
    Reads raw data from MySQL and performs detailed validation checks
    based on the provided dataset context.
    Logs warnings/errors but does not modify data.
    Raises ValueError if critical issues are found.
    """
    user = os.environ.get('FLIGHT_DB_USER')
    password = os.environ.get('FLIGHT_DB_PASSWORD')
    host = os.environ.get('FLIGHT_DB_HOST')
    database = os.environ.get('FLIGHT_DB_NAME')
    raw_table_name = 'flight_data'

    logger.info(f"Starting detailed data validation checks on '{raw_table_name}' table in MySQL.")

    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:3306/{database}')

    try:
        df = pd.read_sql_table(raw_table_name, engine)
        logger.info(f"Read {len(df)} rows for validation checks.")

        if df.empty:
            logger.warning(f"Raw table '{raw_table_name}' is empty. No data to validate.")
            return

        required_columns = [
            'Airline', 'Source', 'Source Name', 'Destination', 'Destination Name',
            'Departure Date & Time', 'Arrival Date & Time', 'Duration (hrs)',
            'Stopovers', 'Aircraft Type', 'Class', 'Booking Source',
            'Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Total Fare (BDT)',
            'Seasonality', 'Days Before Departure'
        ]

        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"CRITICAL VALIDATION ERROR: Missing required columns: {', '.join(missing_cols)}")
        logger.info("Validation Check: All required columns exist.")

        critical_null_check_cols = [
            'Airline', 'Source', 'Destination', 'Departure Date & Time', 'Arrival Date & Time',
            'Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Total Fare (BDT)'
        ]
        for col in critical_null_check_cols:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"Validation Warning: Column '{col}' has {null_count} missing/null values.")

        fare_cols = ['Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Total Fare (BDT)']
        for col in fare_cols:
            non_numeric_count = pd.to_numeric(df[col], errors='coerce').isnull().sum() - df[col].isnull().sum()
            if non_numeric_count > 0:
                logger.warning(f"Validation Warning: Column '{col}' has {non_numeric_count} non-numeric entries.")

            negative_fare_count = df[pd.to_numeric(df[col], errors='coerce') < 0].shape[0]
            if negative_fare_count > 0:
                logger.warning(f"Validation Warning: Column '{col}' has {negative_fare_count} negative fare values.")

        datetime_cols = ['Departure Date & Time', 'Arrival Date & Time']
        for col in datetime_cols:
            invalid_date_count = pd.to_datetime(df[col], errors='coerce').isnull().sum()
            if invalid_date_count > 0:
                logger.warning(f"Validation Warning: Column '{col}' has {invalid_date_count} invalid date/time formats.")

        if 'Duration (hrs)' in df.columns:
            df['Duration (hrs)'] = pd.to_numeric(df['Duration (hrs)'], errors='coerce')
            duration_invalid_count = df['Duration (hrs)'].isnull().sum()
            if duration_invalid_count > 0:
                logger.warning(f"Validation Warning: 'Duration (hrs)' has {duration_invalid_count} non-numeric entries.")
            negative_duration_count = df[df['Duration (hrs)'] < 0].shape[0]
            if negative_duration_count > 0:
                logger.warning(f"Validation Warning: 'Duration (hrs)' has {negative_duration_count} negative values.")

        if 'Days Before Departure' in df.columns:
            df['Days Before Departure'] = pd.to_numeric(df['Days Before Departure'], errors='coerce')
            days_invalid_count = df['Days Before Departure'].isnull().sum()
            if days_invalid_count > 0:
                logger.warning(f"Validation Warning: 'Days Before Departure' has {days_invalid_count} non-integer entries.")
            invalid_range_count = df[(df['Days Before Departure'] < 1) | (df['Days Before Departure'] > 90)].shape[0]
            if invalid_range_count > 0:
                logger.warning(f"Validation Warning: 'Days Before Departure' has {invalid_range_count} values outside the 1-90 range.")

        categorical_str_cols = [
            'Airline', 'Source', 'Source Name', 'Destination', 'Destination Name',
            'Stopovers', 'Aircraft Type', 'Class', 'Booking Source', 'Seasonality'
        ]
        for col in categorical_str_cols:
            if col in df.columns:
                empty_string_count = df[col].astype(str).str.strip().eq('').sum()
                if empty_string_count > 0:
                    logger.warning(f"Validation Warning: Column '{col}' has {empty_string_count} empty strings after stripping whitespace.")

        logger.info("Data validation checks completed successfully.")

    except Exception as e:
        logger.error(f"Error during data validation: {e}")
        raise