# Technical Documentation

This document details the Movie Data Pipeline’s code structure, key functions, and pipeline stages.

## Code Structure
```plaintext
movie-data-pipeline/
├── src/
│   ├── __init__.py
│   ├── main.py          # Entry point, orchestrates pipeline execution
│   ├── utility.py       # Core pipeline logic (fetching, processing, analysis, visualization)
├── data/
│   ├── raw/            # Stores raw Parquet files
│   ├── processed/      # Stores processed Parquet files
│   ├── visualizations/ # Stores PNG plots
├── logs/
│   ├── movie_pipeline.log # Pipeline logs
├── .env                 # Environment variables
├── requirements.txt     # Python dependencies
├── docker-compose.yml   # Docker service definitions
```

## Key Files
- **main.py**:
  - Initializes logging, Spark session, and environment variables.
  - Defines movie IDs and orchestrates pipeline stages (fetch, process, analyze, visualize).
- **utility.py**:
  - Contains functions for data fetching, preprocessing, KPI calculation, analysis, and visualization.
  - Uses Spark for distributed processing and Pandas/Matplotlib for visualization.
- **.env**:
  - Configures API key, file paths, and Spark settings.
- **requirements.txt**:
  - Lists dependencies (e.g., `pyspark`, `requests`, `pandas`, `matplotlib`).
- **docker-compose.yml**:
  - Defines Spark cluster and Python runner services.

## Pipeline Stages
### 1. Initialization
- **Functions**:
  - `setup_logging` (main.py): Configures logging with file rotation and console output (level: INFO).
  - `get_spark_session` (utility.py): Creates a singleton Spark session with optimized configs.
- **Details**:
  - Logger: `movie_pipeline`, writes to `/app/logs/movie_pipeline.log`.
  - Spark: Configured with 2GB driver/executor memory, 2 cores, and distributes `utility.py`.

### 2. Data Fetching
- **Functions**:
  - `fetch_movie_data_single` (utility.py): Fetches TMDB data for one movie ID with retries (max 3).
  - `fetch_batch` (utility.py): Processes a batch of IDs, logging successes and failures.
  - `fetch_and_save_raw_data` (utility.py): Parallelizes fetching using Spark RDDs, saves to Parquet.
- **Details**:
  - Batch size: 5 IDs, with 1-second rate limit delay.
  - Schema: Includes `movie_id`, `error`, `title`, `budget`, `genres`, `credits`, etc.
  - Error Handling: Logs warnings per retry attempt and errors for failed IDs (e.g., 404 for ID 0).
  - Output: `/app/data/raw/movies.parquet`.

### 3. Data Preprocessing
- **Functions**:
  - `preprocess_data` (utility.py): Chains transformations using `pipe`.
  - Transformations (utility.py):
    - `filter_released`: Keeps only released movies.
    - `process_genres`, `process_languages`, etc.: Convert arrays to pipe-separated strings.
    - `convert_budget_musd`, `convert_revenue_musd`: Convert to MUSD, replace 0 with null.
    - `reorder_columns`, `drop_unwanted_columns`: Structure output.
- **Details**:
  - Handles nested fields (e.g., `credits.cast`, `belongs_to_collection`).
  - Ensures data consistency (e.g., numeric `budget`, datetime `release_date`).
  - Output: `/app/data/processed/movies_processed.parquet`.

### 4. KPI Calculation
- **Function**:
  - `calculate_kpis` (utility.py): Computes 12 KPIs.
- **KPIs**:
  - `top_10_revenue`, `top_10_budget`, `top_10_profit`, `bottom_10_profit`: Financial metrics.
  - `top_10_roi`, `bottom_10_roi`: ROI for movies with budget ≥ $10M.
  - `most_voted`, `highest_rated`, `lowest_rated`, `most_popular`: Audience metrics.
  - `uma_tarantino`: Movies with Uma Thurman and Quentin Tarantino.
  - `sci_fi_bruce`: Sci-Fi/Action movies with Bruce Willis.
- **Details**:
  - Uses Spark SQL for filtering and sorting.
  - Logs each KPI calculation.
  - Outputs DataFrames displayed in the console.

### 5. Analysis
- **Functions**:
  - `analyze_franchises` (utility.py): Aggregates metrics by collection (e.g., Avengers).
  - `analyze_directors` (utility.py): Aggregates metrics by director.
- **Details**:
  - Metrics: Movie count, total/mean budget, revenue, rating.
  - Handles multi-director films by exploding `director` column.
  - Outputs sorted DataFrames displayed in the console.

### 6. Visualization
- **Function**:
  - `visualize_data` (utility.py): Generates 5 plots using Pandas/Matplotlib/Seaborn.
- **Plots**:
  - **Revenue vs Budget**: Scatter with break-even line.
  - **ROI by Genre**: Bar plot of median ROI per genre.
  - **Popularity vs Rating**: Scatter plot.
  - **Yearly Revenue Trends**: Line plot of total revenue by release year.
  - **Franchise vs Standalone**: Box plot of revenue.
- **Details**:
  - Converts Spark DataFrame to Pandas for plotting.
  - Saves PNGs to `/app/data/visualizations/`.
  - Logs each plot’s creation.

## Key Functions (utility.py)
- **get_spark_session**: Ensures a single Spark session with `utility.py` distributed.
- **fetch_movie_data_single**:
  - Input: Movie ID, API key.
  - Output: Dictionary with data or error, plus log messages.
  - Retries: 3 attempts, 0.2s delay between retries.
- **fetch_and_save_raw_data**:
  - Input: List of movie IDs, API key, Parquet path.
  - Output: Spark DataFrame of valid data.
  - Parallelizes fetching with RDDs.
- **preprocess_data**:
  - Input: Raw DataFrame.
  - Output: Processed DataFrame with structured columns.
- **calculate_kpis**:
  - Input: Processed DataFrame.
  - Output: Dictionary of KPI DataFrames.
- **visualize_data**:
  - Input: Processed DataFrame.
  - Output: PNG files in visualization directory.

## Logging
- **Logger**: `movie_pipeline`, level INFO.
- **Outputs**:
  - File: `/app/logs/movie_pipeline.log` (10MB, 5 backups).
  - Console: Mirrors file logs.
- **Levels**:
  - DEBUG: API call attempts (disabled by default).
  - INFO: Pipeline progress, successes.
  - WARNING: Retry attempts for failed fetches.
  - ERROR: Failed fetches, critical errors.
- **Example**:
  ```
  2025-05-18 08:11:33 movie_pipeline WARNING - Attempt 1 failed for movie ID 0: 404 Client Error: ...
  2025-05-18 08:11:33 movie_pipeline ERROR - Failed to fetch movie ID 0: 404 Client Error: ...
  ```

## Error Handling
- **API Errors**: Retries (3x) for network issues, logs 404s for invalid IDs.
- **Spark Errors**: Caught and logged during preprocessing and analysis.
- **File Errors**: Ensures directories exist, logs OS errors.
- **Invalid Data**: Filters out null or invalid records (e.g., budget = 0).

## Extensibility
- Add movie IDs in `main.py` to fetch more data.
- Extend `calculate_kpis` for new metrics.
- Add plots in `visualize_data` for additional insights.
- Modify `preprocess_data` for new transformations.

See [Setup](#setupmd) for installation and [Overview](#overviewmd) for project context.