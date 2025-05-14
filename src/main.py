import os
import logging
from dotenv import load_dotenv
from utility import (
    fetch_movie_data, save_raw_data, load_data, preprocess_data,
    save_processed_data, calculate_kpis, analyze_directors, analyze_franchises, plot_visualizations
)

# Determine the project root (parent directory of src/)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Configure logging with absolute path
logs_dir = os.path.join(project_root, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Set up the logger for this module
logger = logging.getLogger('movie_pipeline')
# Clear any existing handlers to prevent duplicates
logger.handlers.clear()
logger.setLevel(logging.INFO)
# Add FileHandler for logging to file
file_handler = logging.FileHandler(os.path.join(logs_dir, 'main.log'))
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
# Add StreamHandler for logging to console
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)

def main():
    """Main function to orchestrate the movie data pipeline."""
    logger.info("Starting movie data pipeline")

    # Load environment variables
    load_dotenv(os.path.join(project_root, '.env'))
    api_key = os.getenv("movie_api_key")
    if not api_key:
        logger.error("API key not found in .env file")
        raise ValueError("API key is required")

    # Define paths using absolute paths
    raw_csv_path = os.path.join(project_root, 'data', 'raw', 'raw_data.csv')
    raw_json_path = os.path.join(project_root, 'data', 'raw', 'raw_data.json')
    processed_csv_path = os.path.join(project_root, 'data', 'processed', 'processed_data.csv')
    processed_json_path = os.path.join(project_root, 'data', 'processed', 'processed_data.json')

    # Movie IDs for fetching (excluding 0 as it's invalid)
    movie_ids = [
        0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259,
        99861, 284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513
    ]

    # Step 1: Fetch and save raw data
    logger.info("Fetching movie data")
    movie_data = fetch_movie_data(movie_ids, api_key)
    raw_df = save_raw_data(movie_data, raw_csv_path, raw_json_path)
    if raw_df is None:
        logger.warning("No raw data fetched. Attempting to load existing data")
        raw_df = load_data(raw_csv_path, raw_json_path)
        if raw_df is None:
            logger.error("No existing data found. Exiting pipeline")
            return

    # Step 2: Preprocess data
    logger.info("Preprocessing data")
    processed_df = preprocess_data(raw_df)
    save_processed_data(processed_df, processed_csv_path, processed_json_path)

    # Step 3: Calculate KPIs
    logger.info("Calculating KPIs")
    kpis = calculate_kpis(processed_df)
    for kpi_name, kpi_df in kpis.items():
        logger.info(f"\n{kpi_name.replace('_', ' ').title()}:\n{kpi_df}")
        commentary = getattr(kpi_df, 'commentary', "No commentary available due to calculation failure.")
        logger.info(f"Commentary: {commentary}")

    # Step 4: Analyze directors
    logger.info("Analyzing directors")
    director_stats = analyze_directors(processed_df)
    logger.info(f"\nDirector Statistics:\n{director_stats}")
    director_commentary = getattr(director_stats, 'commentary', "No commentary available due to calculation failure.")
    logger.info(f"Commentary: {director_commentary}")

    # Step 5: Analyze franchises
    logger.info("Analyzing franchises")
    franchise_stats = analyze_franchises(processed_df)
    logger.info(f"\nFranchise Statistics:\n{franchise_stats}")
    if not franchise_stats.empty:
        franchise_commentary = getattr(franchise_stats, 'commentary', "No commentary available due to calculation failure.")
        logger.info(f"Commentary: {franchise_commentary}")
    else:
        logger.info("Commentary: No franchise data available to analyze.")

    # Step 6: Generate visualizations
    logger.info("Generating visualizations")
    plot_visualizations(processed_df)

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise