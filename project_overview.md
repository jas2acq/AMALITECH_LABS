# Project Overview: Spark Streaming Data Pipeline

This document provides a short overview of the project, describing its components and the flow of data through the system. The project demonstrates a basic data ingestion pipeline using Docker Compose, Python for data generation, Apache Spark Structured Streaming for processing, and PostgreSQL as a data sink.

## System Components

The system is orchestrated using **Docker Compose** and comprises the following services (containers), connected via a shared Docker network:

1.  **PostgreSQL Database (`fresh-postgres`):**
    * A relational database used to store the final, processed data.
    * Initialized with a setup script to create the target table (`purchase_data`).
    * Data is persisted using a Docker named volume (`postgres_data`).

2.  **Spark Master (`fresh-spark-master`):**
    * The central coordinator for the Spark cluster.
    * Manages Spark applications and allocates resources to workers.

3.  **Spark Worker (`fresh-spark-worker`):**
    * Worker nodes that execute tasks assigned by the Spark Master.
    * Runs the actual data processing logic in parallel.

4.  **Python Generator (`fresh-python-runner`):**
    * A Python script (`generator.py`) that simulates the generation of raw purchase data.
    * Writes data records as CSV files to a shared data directory.

5.  **Spark Streaming Job (`spark-streaming-job`):**
    * The Spark application driver that runs the Structured Streaming script (`spark_streaming_to_postgres.py`).
    * Configured to connect to the Spark Master and read data from the shared data directory.

## Data Flow

The data flows through the system in the following stages:

1.  **Data Generation:** The `generator.py` script in the `fresh-python-runner` container generates synthetic purchase data records.
2.  **Raw Data Landing:** The generator writes each batch of generated data as a separate CSV file into a **shared Data Volume** (mounted from `./data` on the host to `/opt/spark/data` in the relevant containers).
3.  **Streaming Data Source:** The `spark_streaming_to_postgres.py` script in the `spark-streaming-job` container is configured as a Spark Structured Streaming job that monitors the **Data Volume** directory for new CSV files.
4.  **Data Ingestion and Processing:** When new files appear, the Spark Streaming job reads them in micro-batches. The job driver (`spark-streaming-job`) coordinates with the Spark Master (`fresh-spark-master`) to execute processing tasks on the Spark Workers (`fresh-spark-worker`).
5.  **Transformation:** Within the Spark job, basic transformations are applied to the data (e.g., removing ID prefixes).
6.  **Data Sinking:** The processed data from each batch is written to the **PostgreSQL Database** (`fresh-postgres`) using a JDBC connection. This write operation is typically executed in parallel by tasks on the Spark Worker nodes.
7.  **Checkpointing:** The Spark Streaming job periodically saves checkpoint information (recording processed files and stream state) back to the **Data Volume** to enable fault tolerance and resume processing after restarts.
8.  **Logging:** Both the `python-scripts` and `spark-streaming-job` containers write application logs to a **shared Log Volume** (mounted from `./log` on the host to `/app/log` in the containers) for monitoring and debugging.

This pipeline provides a basic example of collecting data from a source, processing it in real-time using Spark Structured Streaming, and loading it into a database for storage and analysis. The modular nature provided by Docker Compose allows each component to run in isolation while communicating over the defined network.