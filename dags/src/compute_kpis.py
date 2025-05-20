import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def compute_kpis():
    """
    Reads transformed data from the processed CSV, computes key performance indicators (KPIs),
    and saves them to a CSV file in the KPI outputs directory.
    """
    processed_dir = os.environ.get('DATA_PROCESSED_DIR', '/opt/airflow/data/processed')
    processed_file_path = os.path.join(processed_dir, 'flight_data_transformed.csv')

    kpi_outputs_dir = os.environ.get('DATA_KPI_OUTPUTS_DIR', '/opt/airflow/data/kpi_outputs')
    kpi_file_path = os.path.join(kpi_outputs_dir, 'flight_kpis.csv')

    # Ensure the KPI outputs directory exists
    os.makedirs(kpi_outputs_dir, exist_ok=True)

    logger.info(f"Starting KPI computation from '{processed_file_path}' and saving to '{kpi_file_path}'.")

    try:
        if not os.path.exists(processed_file_path):
            raise FileNotFoundError(f"Processed data CSV not found: {processed_file_path}. "
                                    "Ensure 'transform_data' task completed successfully.")

        df = pd.read_csv(processed_file_path)
        logger.info(f"Read {len(df)} rows from '{processed_file_path}' for KPI computation.")

        if df.empty:
            logger.warning(f"Source CSV '{processed_file_path}' is empty. No data to compute KPIs.")
            return

        # Ensure numeric columns are actually numeric after reading from CSV
        numeric_cols_for_kpis = ['Total Fare (BDT)', 'Duration (hrs)']
        for col in numeric_cols_for_kpis:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)


        # 1. Average Total Fare per Airline
        avg_fare_airline = df.groupby('Airline')['Total Fare (BDT)'].mean().reset_index()
        avg_fare_airline.rename(columns={'Total Fare (BDT)': 'Average Total Fare'}, inplace=True)
        logger.info("Computed Average Total Fare per Airline.")

        # 2. Number of flights per Route
        flights_per_route = df.groupby('Route').size().reset_index(name='Number of Flights')
        logger.info("Computed Number of Flights per Route.")

        # 3. Average Duration per Stopover type
        avg_duration_stopover = df.groupby('Stopovers')['Duration (hrs)'].mean().reset_index()
        avg_duration_stopover.rename(columns={'Duration (hrs)': 'Average Duration'}, inplace=True)
        logger.info("Computed Average Duration per Stopover type.")


        kpi_combined_df = pd.concat([
            avg_fare_airline.set_index('Airline').stack().reset_index(name='Value').rename(columns={'level_1': 'KPI_Name'}),
            flights_per_route.set_index('Route').stack().reset_index(name='Value').rename(columns={'level_1': 'KPI_Name'}),
            avg_duration_stopover.set_index('Stopovers').stack().reset_index(name='Value').rename(columns={'level_1': 'KPI_Name'})
        ], ignore_index=True)

        kpi_combined_df.to_csv(kpi_file_path, index=False)
        logger.info(f"Successfully computed and saved KPIs to '{kpi_file_path}'.")

    except Exception as e:
        logger.error(f"An error occurred during KPI computation: {e}")
        raise