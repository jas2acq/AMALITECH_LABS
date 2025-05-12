# User Guide: Running the Spark Streaming Data Pipeline

This guide provides step-by-step instructions on how to set up and run the data ingestion pipeline project using Docker Compose.

## Prerequisites

Before you begin, ensure you have the following installed on your host machine:

* **Docker:** (Docker Desktop for Windows/macOS, or Docker Engine for Linux) - Includes Docker Compose.
* **A Text Editor:** To edit files like `.env` and potentially the scripts.

## Project Setup

1.  **Clone or Download Project Files:** Get the project files onto your host machine. Ensure you have:
    * `docker-compose.yml`
    * Your Python script (`generator.py`) and its requirements file (`requirements.txt`) - place these in a directory (e.g., `./src`).
    * Your Spark script (`spark_streaming_to_postgres.py`) - place this in the same directory (e.g., `./src`).
    * Your SQL setup script (`postgres_setup.sql`) - place this in a directory (e.g., `./sql`).
    * The PostgreSQL JDBC Driver JAR file (`postgresql-42.7.5.jar`) - place this in your project's root directory or a dedicated `jars` directory if you modify the compose file path.
    * A `.env` file (create this if it doesn't exist).

2.  **Create Necessary Directories:** In your project's root directory (where `docker-compose.yml` is located), create the following directories:
    ```bash
    mkdir data
    mkdir log
    mkdir sql  # If not already created for postgres_setup.sql
    mkdir src  # If not already created for your scripts
    # mkdir jars # If you plan to put the JAR here and update docker-compose.yml
    ```
    * Place `generator.py`, `spark_streaming_to_postgres.py`, and `requirements.txt` inside the `./src` directory.
    * Place `postgres_setup.sql` inside the `./sql` directory.
    * Place `postgresql-42.7.5.jar` in your project root (or update `docker-compose.yml` if you put it elsewhere).

3.  **Configure the `.env` File:** Create a file named `.env` in your project's root directory (if it doesn't exist) and add the following variables. Replace the placeholder values with your desired configuration.

    ```dotenv
    # Database Configuration
    POSTGRES_USER=userpostgres
    POSTGRES_PASSWORD=1234
    PROJECT_DB_NAME=purchase_db
    POSTGRES_PORT=5432 # Port on your host machine to access Postgres

    # Generator Configuration
    TIME_LIMIT=120 # Time limit for generator script in seconds, or 'continuous'
    ```
    Ensure there are no spaces around the `=` signs in the `.env` file.

4.  **Verify `docker-compose.yml`:** Ensure your `docker-compose.yml` file is correctly configured, matching the structure and paths we finalized in previous steps (especially the `command` blocks and volume mounts).

## Running the Project

1.  **Open Your Terminal or Command Prompt:** Navigate to your project's root directory (where the `docker-compose.yml` file is located).
2.  **Build and Run Services:** Use the `docker compose up` command to build the necessary images (if changes were made or it's the first run) and start all services defined in the `docker-compose.yml` file in detached mode (`-d`).

    ```bash
    docker compose up --build -d
    ```
    * `--build`: Builds images if they don't exist or Dockerfile/context has changed.
    * `-d`: Runs containers in detached mode (in the background).

3.  **Monitor Service Logs:** Watch the logs of your services to ensure they start correctly and observe their activity. You can view logs for all services or specific services.

    * View logs for all services:
        ```bash
        docker compose logs -f
        ```
        (`-f` follows the logs in real-time)
    * View logs for a specific service (e.g., the Spark streaming job):
        ```bash
        docker compose logs -f spark-streaming-job
        ```
        (Replace `spark-streaming-job` with `fresh-python-runner`, `fresh-postgres`, `fresh-spark-master`, or `fresh-spark-worker` as needed)

## Verifying Output

While the services are running:

1.  **Check Generated Files:** Look in your local `./data` directory on your host machine. You should see `.csv` files being created by the `fresh-python-runner` container.
2.  **Check Application Logs:** Look in your local `./log` directory on your host machine. You should find `generator.log` and `spark-stream.log` (or similar, based on your script's logging config) containing output from your scripts.
3.  **Check Data in PostgreSQL:** Connect to your PostgreSQL database using a client (like `psql`, DBeaver, pgAdmin, etc.) on your host machine via the port you mapped (e.g., `localhost:${POSTGRES_PORT}`). Connect to the database named `${PROJECT_DB_NAME}` using the user `${POSTGRES_USER}` and password `${POSTGRES_PASSWORD}` from your `.env`.
    * Run SQL queries to verify data is being ingested into the `purchase_data` table:
        ```sql
        SELECT COUNT(*) FROM purchase_data; -- Should show a count greater than 0
        SELECT * FROM purchase_data LIMIT 10; -- Inspect sample rows
        ```
4.  **Monitor Spark UI:** Access the Spark Master UI at `http://localhost:8080` to see running applications and workers. Access the Spark Application UI at `http://localhost:4040` (for `spark-streaming-job`) to view detailed streaming metrics, batches processed, and executor status.

## Stopping and Cleaning Up

When you are finished, you can stop and remove the Docker containers, networks, and volumes.

1.  **Stop Services:** Stop the running containers.
    ```bash
    docker compose stop
    ```
2.  **Stop and Remove Services:** Stop the containers and remove the containers, networks, and volumes. Using `-v` removes the named volumes (like `postgres_data`), which means your database data will be lost. Omit `-v` if you want to keep the database data for a future run.
    ```bash
    docker compose down -v
    ```
    * `down`: Stops and removes containers and networks by default.
    * `-v`: Removes volumes.

This guide covers the essential steps to get your data pipeline running and verify its basic functionality.