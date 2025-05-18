# Project Overview

## Purpose
The Movie Data Pipeline fetches, processes, and analyzes movie data from the TMDB API to provide insights into movie performance, including revenue, budget, ROI, and ratings. It’s designed for data analysts and researchers interested in movie industry trends, offering a scalable, automated pipeline with robust error handling and visualizations.

## Goals
- **Data Collection**: Retrieve comprehensive movie data for a list of TMDB movie IDs.
- **Data Processing**: Clean and structure data for analysis, handling missing or invalid entries.
- **Analysis**: Compute KPIs (e.g., top 10 revenue, highest-rated movies) and aggregate metrics by franchise and director.
- **Visualization**: Generate plots to visualize trends (e.g., Revenue vs Budget, ROI by Genre).
- **Reliability**: Ensure fault tolerance with retry mechanisms, logging, and error reporting.

## Architecture
The pipeline is built using Python, Apache Spark, and Docker, with the following components:
- **Data Source**: TMDB API provides movie details (e.g., title, budget, revenue, cast, crew).
- **Processing Engine**: Apache Spark handles distributed data processing, supporting large datasets.
- **Storage**: Raw and processed data are stored as Parquet files for efficient querying.
- **Visualization**: Matplotlib and Seaborn generate plots saved as PNG files.
- **Orchestration**: Docker Compose manages services (Python runner, Spark master/worker).
- **Configuration**: Environment variables (via `.env`) specify API keys, file paths, and Spark settings.
- **Logging**: Comprehensive logging captures pipeline progress, errors, and warnings.

## Workflow
1. **Initialization**:
   - Load environment variables (e.g., TMDB API key, file paths).
   - Initialize Spark session and configure logging.
2. **Data Fetching**:
   - Fetch movie data for specified IDs in batches (5 IDs per batch, with rate limiting).
   - Handle errors (e.g., 404 for invalid IDs) with retries and logging.
   - Save raw data to `/app/data/raw/movies.parquet`.
3. **Data Preprocessing**:
   - Filter for released movies, convert budgets/revenues to MUSD, and process arrays (e.g., genres, cast).
   - Save processed data to `/app/data/processed/movies_processed.parquet`.
4. **Analysis**:
   - Calculate KPIs (e.g., top 10 revenue, ROI, most popular movies).
   - Aggregate metrics by franchise and director.
5. **Visualization**:
   - Generate plots (e.g., Revenue vs Budget with break-even line, Popularity vs Rating).
   - Save to `/app/data/visualizations/`.
6. **Output**:
   - Log pipeline progress and errors to `/app/logs/movie_pipeline.log`.
   - Display KPI results in the console.

## Key Outputs
- **Raw Data**: Parquet file with raw TMDB API responses.
- **Processed Data**: Structured Parquet file with cleaned and transformed data.
- **KPIs**: Tables for top/bottom revenue, profit, ROI, ratings, and specific queries (e.g., Uma Thurman with Quentin Tarantino).
- **Visualizations**: PNG files showing trends and relationships.
- **Logs**: Detailed logs for debugging and monitoring.

See [Documentation](#documentationmd) for technical details and [Setup](#setupmd) for installation instructions.