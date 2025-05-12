# Project Purchase Data Pipeline - Manual Test Plan

This document outlines manual test cases for the data generation and Spark Structured Streaming pipeline that ingests data into a PostgreSQL database.

---

## How to Use This Document

1.  Ensure your Docker Compose setup and application code are ready for testing.
2.  Follow the "Steps" for each test case.
3.  Observe the behavior of the containers (using `docker compose ps`, `docker compose logs`), check generated files, and query the database as required.
4.  Record what actually happened in the "Actual Outcome" section.
5.  Compare the "Actual Outcome" to the "Expected Outcome" and mark the "Status" as Pass or Fail.

---

## Test Case 1: Data Generation - File Creation

* **Test Case ID:** GEN-001
* **Objective:** Verify that the `generator.py` script running in the `python-scripts` container successfully creates CSV files with the expected format in the mounted `./data` directory.
* **Environment:** Docker Compose up (at least `postgres`, `python-scripts`)
* **Steps:**
    1.  Ensure all Docker Compose services are stopped (`docker compose down -v`).
    2.  Manually clear the local `./data` directory on your host machine.
    3.  Run `docker compose up -d postgres python-scripts`.
    4.  Wait for at least 10 seconds (longer than the `delay_seconds` in `generator.py`) to allow files to be generated.
    5.  Check the `./data` directory on your host machine for newly created files.
    6.  Check the logs of the `fresh-python-runner` container (`docker compose logs fresh-python-runner`).
    7.  Optionally, open one of the generated CSV files to inspect its format and content.
* **Expected Outcome:**
    * The `fresh-python-runner` container starts and stays running.
    * New `.csv` files are created in the `./data` directory on your host machine.
    * The filenames follow the pattern `purchase_<ORDER_ID>_<TIMESTAMP>.csv`.
    * The `fresh-python-runner` logs show "Generated: /opt/spark/data/..." messages for each file created.
    * The content of the CSV files matches the expected schema and formatting (including prefixes, prices, etc.).
* **Actual Outcome:**
* **Status:** (Pass/Fail)

---

## Test Case 2: Spark Job Startup & Master Connection

* **Test Case ID:** SPARK-001
* **Objective:** Verify that the `spark-submit-job` container starts successfully and the Spark application connects to the Spark Master and is listed in the UI.
* **Environment:** Docker Compose up (at least `postgres`, `spark-master`, `spark-worker`, `spark-submit-job`)
* **Steps:**
    1.  Ensure core Spark/Postgres services are running (`docker compose up -d postgres spark-master spark-worker`).
    2.  Run `docker compose up -d spark-submit-job`.
    3.  Check the status of the `spark-submit-job` container (`docker compose ps`).
    4.  Check the logs of the `spark-submit-job` container (`docker compose logs spark-submit-job`).
    5.  Access the Spark Master UI in a browser (`http://localhost:8080`).
* **Expected Outcome:**
    * The `spark-submit-job` container starts and its status in `docker compose ps` is `Up`.
    * The `spark-submit-job` logs show messages indicating Spark startup (e.g., "SparkContext: Running Spark version...", "Submitted application: PurchaseDataStreamToPostgres").
    * The logs show the application connecting to the master (e.g., "Connecting to master spark://fresh-spark-master:7077...").
    * The Spark Master UI (`localhost:8080`) lists the "PurchaseDataStreamToPostgres" application as "RUNNING" in the "Running Applications" section.
* **Actual Outcome:**
* **Status:** (Pass/Fail)

---

## Test Case 3: Data Ingestion into PostgreSQL

* **Test Case ID:** PG-001
* **Objective:** Verify that data from the generated CSV files is successfully read by the Spark streaming job and ingested into the `purchase_data` table in the PostgreSQL database.
* **Environment:** All Docker Compose services running (`docker compose up -d`)
* **Steps:**
    1.  Ensure all Docker Compose services are running (`docker compose up -d`).
    2.  Ensure the `python-scripts` generator is creating files (check `./data` directory or `python-scripts` logs).
    3.  Ensure the `spark-submit-job` is running (check `docker compose ps` and `spark-submit-job` logs for startup messages).
    4.  Allow the generator to create several files (wait for at least 30 seconds or more).
    5.  Check the `spark-submit-job` logs (`docker compose logs spark-submit-job`) for messages like "Writing batch..." and "Successfully wrote batch...".
    6.  Access the PostgreSQL database (e.g., using `psql` on the host or a GUI tool like DBeaver).
    7.  Connect to the `purchase_db` database using the correct user (`userpostgres`) and password (`1234`).
    8.  Run the SQL query `SELECT COUNT(*) FROM purchase_data;`.
* **Expected Outcome:**
    * The `spark-submit-job` logs show successful batch write messages.
    * The `psql` command or GUI tool connects successfully.
    * The `SELECT COUNT(*)` query returns a count greater than 0.
    * (Optional) Query specific rows (`SELECT * FROM purchase_data LIMIT 10;`) to verify the data format and transformations (like removed prefixes).
* **Actual Outcome:**
* **Status:** (Pass/Fail)

---

## Test Case 4: Generator File Writing Error Handling

* **Test Case ID:** GEN-002
* **Objective:** Verify that the `generator.py` script correctly handles errors when writing files to the `./data` directory, logs the error, and stops execution.
* **Environment:** Docker Compose up (at least `postgres`, `python-scripts`)
* **Steps:**
    1.  Ensure services are stopped (`docker compose down -v`).
    2.  Create a scenario where the `python-scripts` container cannot write to `./data`. A simple way is to make the local `./data` directory on your host machine read-only for the user/group Docker runs as. (Finding the exact user/group in a container can be tricky, sometimes simulating other errors is easier).
    3.  Run `docker compose up -d postgres python-scripts`.
    4.  Check the logs of the `fresh-python-runner` container (`docker compose logs fresh-python-runner`).
    5.  Check the `./log/generator.log` file on your host.
* **Expected Outcome:**
    * The `fresh-python-runner` container might start but should stop shortly after attempting to write the first file.
    * The logs show an error message indicating a file writing failure (e.g., permission denied).
    * The error message should be logged using `logging.error` in both the console output and the `./log/generator.log` file.
    * The script should terminate (exit) due to the error being re-raised by `generate_purchase` and caught in the main loop.
* **Actual Outcome:**
* **Status:** (Pass/Fail)

---

## Test Case 5: Spark Database Write Error Handling (Temporary Downtime)

* **Test Case ID:** SPARK-002
* **Objective:** Verify that the Spark streaming job handles temporary database downtime as expected (logs write errors for affected batches, continues processing, and resumes writing when the database is back).
* **Environment:** All Docker Compose services running (`docker compose up -d`)
* **Steps:**
    1.  Ensure all services are running (`docker compose up -d`) and the Spark job is processing data (check logs for successful batch writes).
    2.  Temporarily stop the PostgreSQL container (`docker compose stop postgres`).
    3.  Allow the generator to create more files and the Spark job to attempt writing (wait for several seconds, allowing a few batches to be processed).
    4.  Check the `spark-submit-job` logs (`docker compose logs spark-submit-job`) for error messages.
    5.  Start the PostgreSQL container again (`docker compose start postgres`).
    6.  Continue checking the `spark-submit-job` logs to observe recovery.
* **Expected Outcome:**
    * When the database is stopped, `spark-submit-job` logs show `Error writing batch...` messages containing `PSQLException` related to connection failures (e.g., connection refused).
    * The `spark-submit-job` container/script should **not** stop; it should continue running and attempting to process subsequent batches from the file source (due to the `pass` in the `foreachBatch` error handling).
    * After `postgres` is restarted, Spark should eventually resume successfully writing batches, indicated by `Successfully wrote batch...` messages in the logs.
* **Actual Outcome:**
* **Status:** (Pass/Fail)

---

## Test Case 6: Spark Streaming Checkpointing

* **Test Case ID:** SPARK-003
* **Objective:** Verify that checkpointing allows the Spark streaming job to resume processing from where it left off after being stopped and restarted.
* **Environment:** All Docker Compose services running (`docker compose up -d`)
* **Steps:**
    1.  Ensure all services are running (`docker compose up -d`) and the Spark job is processing data.
    2.  Allow the generator to create a significant number of files (e.g., wait 1-2 minutes) and observe several "Successfully wrote batch..." messages in the Spark logs. Note the batch IDs being processed.
    3.  Stop *only* the `spark-submit-job` container (`docker compose stop spark-submit-job`).
    4.  Do NOT remove containers or volumes (`docker compose down` or `docker compose down -v`).
    5.  Start the `spark-submit-job` container again (`docker compose start spark-submit-job`).
    6.  Check the logs of the `spark-submit-job` container (`docker compose logs spark-submit-job`).
* **Expected Outcome:**
    * The `spark-submit-job` container restarts.
    * The logs indicate the stream is recovering from the checkpoint location.
    * Spark should *not* reprocess files/batches that were already successfully written before the stop.
    * Processing should resume with the next batch of files created *after* the last successfully checkpointed batch, or process any files that arrived while the stream was down, without duplicating previously processed data in the database.
* **Actual Outcome:**
* **Status:** (Pass/Fail)

---

## Test Case 7: Database Schema

* **Test Case ID:** PG-002
* **Objective:** Verify that the `purchase_data` table exists in the PostgreSQL database and its schema matches the expected structure for data ingestion.
* **Environment:** Docker Compose up (at least `postgres`)
* **Steps:**
    1.  Ensure the `postgres` container is running (`docker compose up -d postgres`).
    2.  Access the PostgreSQL database (e.g., using `psql` on the host or a GUI tool like DBeaver).
    3.  Connect to the `purchase_db` database using the correct user and password.
    4.  Run the SQL command to describe the table: `\d purchase_data`.
* **Expected Outcome:**
    * The command executes successfully.
    * The output lists columns with names (`customer_id`, `first_name`, `last_name`, `age`, `order_id`, `time_stamp`, `product_id`, `product_name`, `quantity`, `unit_price`) and types (`VARCHAR`, `INT`, `TIMESTAMP WITHOUT TIME ZONE`, `FLOAT` or `REAL`) that match the `transformed_df` schema and your `CREATE TABLE` script.
* **Actual Outcome:**
* **Status:** (Pass/Fail)