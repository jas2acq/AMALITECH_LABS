import utility 
import logging
import os
from dotenv import load_dotenv
load_dotenv()

def main():
    """
    Main function to run the movie data pipeline.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 1. Fetch raw data
    movie_api_key = os.getenv("movie_api_key")  # Replace with your API key
    movie_ids_list = [299534, 19995, 140607, 299536, 597, 135397,
                       420818, 24428, 168259, 99861, 284054, 12445,
                       181808, 330457, 351286, 109445, 321612, 260513]
    
    raw_movie_spark_df = utility.create_raw_movies_spark_dataframe(movie_ids_list, movie_api_key, schema=utility.movie_schema)
    
    # 2. Check for errors
    if 'error' in raw_movie_spark_df.columns:
        error_df = raw_movie_spark_df.filter(raw_movie_spark_df["error"].isNotNull())
        if error_df.count() > 0:
            logging.warning("Errors encountered during API requests:")
            error_df.show()
    else:
        logging.info("No errors encountered during API requests.")
        
    # 3. Process data 
    valid_df = raw_movie_spark_df.filter(raw_movie_spark_df["error"].isNull()) if 'error' in raw_movie_spark_df.columns else raw_movie_spark_df
    cleaned_movie_df = utility.create_cleaned_movie_dataframe_spark(valid_df)

    # 4. Apply KPI calculations
    cleaned_movie_df_with_kpis = utility.apply_kpi_calculations(cleaned_movie_df)
    
    # 5. Visualizations (optional)
    utility.visualize_data(cleaned_movie_df_with_kpis)

if __name__ == "__main__":
    main()