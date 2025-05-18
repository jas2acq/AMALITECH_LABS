# Movie Data Pipeline

The Movie Data Pipeline is a Python-based application that fetches movie data from the [The Movie Database (TMDB) API](https://www.themoviedb.org/), processes it using Apache Spark, and generates key performance indicators (KPIs) and visualizations. It handles data fetching, preprocessing, analysis, and visualization, with robust error handling and logging.

## Features
- **Data Fetching**: Retrieves movie details (e.g., budget, revenue, genres, cast) from TMDB for specified movie IDs.
- **Data Processing**: Uses Spark to clean and transform raw data into a structured format.
- **KPIs**: Calculates metrics like top-grossing movies, highest ROI, and director performance.
- **Visualizations**: Generates plots (e.g., Revenue vs Budget, ROI by Genre) using Matplotlib and Seaborn.
- **Error Handling**: Logs failed API calls (e.g., invalid movie IDs) and ensures pipeline reliability.
- **Dockerized Setup**: Runs in a containerized environment with Spark and Python dependencies.

## Quick Start
1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd movie-data-pipeline
   ```
2. **Set Up Environment**:
   Create a `.env` file with required variables (see [Setup](setup.md)):
   ```plaintext
   TMDB_API_KEY=your_api_key
   RAW_DATA_PATH=/app/data/raw/movies.parquet
   PROCESSED_DATA_PATH=/app/data/processed/movies_processed.parquet
   SPARK_MASTER_URL=spark://spark-master:7077
   VISUALIZATION_PATH=/app/data/visualizations
   LOG_FILE_PATH=/app/logs/movie_pipeline.log
   ```
3. **Run with Docker**:
   ```bash
   docker-compose up -d
   ```
4. **View Outputs**:
   - Logs: `/app/logs/movie_pipeline.log`
   - Raw data: `/app/data/raw/movies.parquet`
   - Processed data: `/app/data/processed/movies_processed.parquet`
   - Visualizations: `/app/data/visualizations/`

## Documentation
- [Overview](overview.md): Project goals, architecture, and workflow.
- [Setup](setup.md): Detailed setup instructions, including prerequisites and configuration.
- [Documentation](documentation.md): Technical details on code structure, functions, and pipeline stages.