import os
import logging
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
import sys
from utility import (
    fetch_and_save_raw_data,
    load_data,
    preprocess_data,
    save_processed_data,
    calculate_kpis,
    analyze_franchises,
    analyze_directors,
    visualize_data
)

def setup_logging(log_file_path):
    """Configure logging with file rotation and console output."""
    logger = logging.getLogger('movie_pipeline')
    logger.setLevel(logging.INFO)
    logger.handlers = []

    # File handler with rotation
    file_handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(console_handler)

def main():
    """Main function to run the movie data analysis pipeline."""
    try:
        # Load environment variables
        load_dotenv()

        # Retrieve and validate environment variables
        api_key = os.getenv('TMDB_API_KEY')
        raw_parquet_path = os.getenv('RAW_DATA_PATH')
        processed_parquet_path = os.getenv('PROCESSED_DATA_PATH')
        log_file_path = os.getenv('LOG_FILE_PATH')

        missing_vars = []
        if not api_key:
            missing_vars.append('TMDB_API_KEY')
        if not raw_parquet_path:
            missing_vars.append('RAW_DATA_PATH')
        if not processed_parquet_path:
            missing_vars.append('PROCESSED_DATA_PATH')
        if not log_file_path:
            missing_vars.append('LOG_FILE_PATH')
        
        if missing_vars:
            logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
            return

        # Setup logging
        setup_logging(log_file_path)
        logger = logging.getLogger('movie_pipeline')
        logger.info("Starting movie data pipeline.")

        # Movie IDs from project document
        movie_ids = [
            0, 299534, 19995, 140607, 299536, 597, 135397, 420818,
            24428, 168259, 99861, 284054, 12445, 181808, 330457,
            351286, 109445, 321612, 260513
        ]

        # Step 1: Fetch and save raw data
        logger.info("Starting data fetching")
        raw_df = fetch_and_save_raw_data(movie_ids, api_key, raw_parquet_path)
        if raw_df is None:
            logger.error("Failed to fetch raw data. Exiting pipeline.")
            return

        # Step 2: Load raw data (if not already loaded)
        logger.info("Loading raw data")
        movie_df = load_data(raw_parquet_path)
        if movie_df is None:
            logger.error("Failed to load raw data. Exiting pipeline.")
            return

        # Step 3: Preprocess data
        logger.info("Preprocessing data")
        cleaned_movie_df = preprocess_data(movie_df)
        if cleaned_movie_df.count() == 0:
            logger.error("No data after preprocessing. Exiting pipeline.")
            return

        # Step 4: Save processed data
        logger.info("Saving processed data")
        save_processed_data(cleaned_movie_df, processed_parquet_path)

        # Step 5: Calculate KPIs
        logger.info("Calculating KPIs")
        kpis = calculate_kpis(cleaned_movie_df)
        for kpi_name, kpi_df in kpis.items():
            logger.info(f"Showing results for {kpi_name}:")
            kpi_df.show()

        # Step 6: Analyze franchises
        logger.info("Analyzing franchises")
        franchise_df = analyze_franchises(cleaned_movie_df)
        logger.info("Franchise analysis results:")
        franchise_df.show()

        # Step 7: Analyze directors
        logger.info("Analyzing directors")
        director_df = analyze_directors(cleaned_movie_df)
        logger.info("Director analysis results:")
        director_df.show()

        # Step 8: Generate visualizations
        logger.info("Generating visualizations")
        visualize_data(cleaned_movie_df)

        logger.info("Movie data pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()