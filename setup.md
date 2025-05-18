# Setup Instructions

This guide explains how to set up and run the Movie Data Pipeline using Docker, Python, and Apache Spark.

## Prerequisites
- **Docker**: Install Docker and Docker Compose:
  - [Docker Installation](https://docs.docker.com/get-docker/)
  - [Docker Compose Installation](https://docs.docker.com/compose/install/)
- **Git**: For cloning the repository.
- **TMDB API Key**: Obtain a free API key from [TMDB](https://www.themoviedb.org/settings/api).
- **Hardware**:
  - Minimum: 4GB RAM, 2 CPU cores.
  - Recommended: 8GB RAM, 4 CPU cores for Spark processing.
- **Operating System**: Linux, macOS, or Windows (with WSL2 for Windows).

## Project Structure
```plaintext
movie-data-pipeline/
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── utility.py
├── data/
│   ├── raw/
│   ├── processed/
│   ├── visualizations/
├── logs/
│   ├── movie_pipeline.log
├── .env
├── requirements.txt
├── docker-compose.yml
```

## Installation Steps
1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd movie-data-pipeline
   ```

2. **Configure Environment Variables**:
   Create a `.env` file in the project root with the following:
   ```plaintext
   TMDB_API_KEY=your_tmdb_api_key
   RAW_DATA_PATH=/app/data/raw/movies.parquet
   PROCESSED_DATA_PATH=/app/data/processed/movies_processed.parquet
   SPARK_MASTER_URL=spark://spark-master:7077
   VISUALIZATION_PATH=/app/data/visualizations
   LOG_FILE_PATH=/app/logs/movie_pipeline.log
   ```
   - Replace `your_tmdb_api_key` with your TMDB API key.
   - Ensure paths use `/app/` as the base (Docker container’s filesystem).

3. **Verify Requirements**:
   Ensure `requirements.txt` includes:
   ```plaintext
   pyspark==3.5.5
   requests==2.32.3
   python-dotenv==1.1.0
   pandas==2.2.3
   matplotlib==3.10.3
   seaborn==0.13.2
   py4j==0.10.9.7
   ```

4. **Set Up Docker**:
   Ensure `docker-compose.yml` defines services for the Python runner and Spark cluster:
   ```yaml
   version: '3.8'
   services:
     spark-master:
       image: bitnami/spark:3.5.3
       ports:
         - "8080:8080"
         - "7077:7077"
       environment:
         - SPARK_MODE=master
     spark-worker:
       image: bitnami/spark:3.5.3
       environment:
         - SPARK_MODE=worker
         - SPARK_MASTER_URL=spark://spark-master:7077
       depends_on:
         - spark-master
     python-runner:
       image: python:3.9
       volumes:
         - ./src:/app/src
         - ./data:/app/data
         - ./logs:/app/logs
         - ./.env:/app/.env
         - ./requirements.txt:/app/requirements.txt
       command: >
         bash -c "pip install -r requirements.txt && python src/main.py"
       depends_on:
         - spark-master
       environment:
         - PYTHONPATH=/app/src
   ```

5. **Run the Pipeline**:
   Start the Docker services:
   ```bash
   docker-compose up -d
   ```
   - This launches the Spark cluster and Python runner.
   - The pipeline fetches data, processes it, and generates outputs.

6. **Monitor Progress**:
   - **Logs**:
     ```bash
     docker logs movie-data-pipeline-python-runner-1
     cat logs/movie_pipeline.log
     ```
     Expect logs like:
     ```
     2025-05-18 08:10:32 movie_pipeline INFO - Starting movie data pipeline.
     2025-05-18 08:11:33 movie_pipeline ERROR - Failed to fetch movie ID 0: 404 Client Error: ...
     2025-05-18 08:12:17 movie_pipeline INFO - Movie data pipeline completed successfully.
     ```
   - **Outputs**:
     - Raw data: `/app/data/raw/movies.parquet`
     - Processed data: `/app/data/processed/movies_processed.parquet`
     - Visualizations: `/app/data/visualizations/` (e.g., `revenue_vs_budget.png`)

7. **Stop the Pipeline**:
   ```bash
   docker-compose down
   ```

## Troubleshooting
- **API Key Issues**:
  - Verify the TMDB API key:
    ```bash
    curl "https://api.themoviedb.org/3/movie/19995?api_key=your_api_key"
    ```
  - Ensure `TMDB_API_KEY` is set in `.env`.
- **Spark Connection Errors**:
  - Check if `SPARK_MASTER_URL` matches `spark://spark-master:7077`.
  - Ensure Spark services are running:
    ```bash
    docker ps
    ```
- **Missing Logs**:
  - Verify `LOG_FILE_PATH` in `.env`.
  - Check log permissions:
    ```bash
    ls -l logs/
    ```
- **No Data Fetched**:
  - Ensure movie IDs in `main.py` are valid (e.g., 19995 for *Avatar*).
  - Check for 404 errors in logs for invalid IDs.
- **Visualization Errors**:
  - Confirm `matplotlib` and `seaborn` in `requirements.txt`.
  - Check `VISUALIZATION_PATH` directory permissions.

## Manual Execution (Optional)
To run without Docker:
1. Install Python 3.9 and Spark 3.5.3 locally.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set environment variables:
   ```bash
   export TMDB_API_KEY=your_api_key
   export SPARK_MASTER_URL=spark://localhost:7077
   ...
   ```
4. Run:
   ```bash
   python src/main.py
   ```

See [Documentation](#documentationmd) for code details and [Overview](#overviewmd) for project context.