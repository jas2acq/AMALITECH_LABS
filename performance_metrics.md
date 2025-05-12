# Project Purchase Data Pipeline - Performance Metrics Report

This report summarizes the observed performance metrics for the Spark Structured Streaming job (`spark_streaming_to_postgres.py`) responsible for ingesting purchase data into the PostgreSQL database. It includes high-level streaming statistics and detailed execution context from the Spark UI to provide a comprehensive view of the pipeline's performance and identify potential issues.

---

## Objective

To monitor and report on the performance of the streaming ingestion pipeline over a specific period. The goal is to analyze key metrics like throughput, processing time, resource utilization, and identify potential bottlenecks by examining data from the Streaming, Executors, and Stages tabs of the Spark Application UI. This report also considers the broader system context, including connectivity to external dependencies.

---

## Environment Details

* **Date of Test:** 2025-05-12
* **Duration of Monitoring Period:** Approximately 5 minutes 21 seconds
* **Docker Compose Services Running:** postgres, spark-master, spark-worker, python-scripts, spark-submit-job
* **Spark Version:** Spark Version - 3.5.5
* **Generator Configuration:** The data generator (`generator.py`) was configured during the test to run on a time constraint `TIME_LIMIT` set to 120secs (Note: Script logs indicated running continuously in a previous turn, but report reflects user's config detail) and to simulate incoming delays `delay_seconds` set to 2secs.
* **Approximate Data Volume Processed:** Approximately 170 records or files were processed during the monitoring period (Based on Latest Batch ~168, Avg Input/Process ~1 record/sec over ~3 mins 49s/5 mins 21s)
* **Monitoring Sources:** Spark Application UI (http://localhost:4040 - Specifically the Structured Streaming, Executors, and Stages Tabs)
* **External Dependency Status:** Host to Database connection status via `localhost:5432` - Connectivity from Spark containers to `fresh-postgres` within the Docker network is crucial for job success.

---

## Observed Streaming Metrics (High-Level Overview)

Metrics observed from the Spark Application UI's "Structured Streaming" tab, providing an overall view of the stream's progress and average performance.

### Query Summary

* **Streaming Query Name:** < no name >
* **Status:** RUNNING
* **Run ID:** 67abf4f1-e4b0-4f6b-b324-5b178b13f8d
* **Start Time:** 2025/05/12 03:36:03
* **Total Completed Batches:** 168 

### Throughput

* **Average Input Rate:**
    * Observed Value (Avg records/sec): 1.04
    * Notes: Input rate fluctuates, generally staying below 2.0 records/sec.

* **Average Process Rate:**
    * Observed Value (Avg records/sec): 1.06
    * Notes: Process rate generally tracks the input rate closely and is slightly higher on average, suggesting the pipeline is keeping up during this period. Fluctuations mirror the input rate.

### Processing Time / Latency

* **Average Batch Duration:**
    * Observed Value (Avg ms): ~500ms - 1000ms 
    * Notes: Batch durations are mostly under 1000ms, with some spikes reaching up towards 2000ms (2 seconds).

* **Operation Duration Breakdown (within a typical batch):**
    * **Source (Reading Data):** Appears relatively quick compared to other operations.
    * **Process (Transformations):** Takes a notable portion of batch time.
    * **Sink (Writing to Database):** Appears to take a significant portion of batch time, similar to or potentially more than Process in some batches.
    * **Other (e.g., Trigger, Write Ahead Log):** Sink and Process operations appear to consume the largest portions of batch duration time.


### Data Volume per Batch

* **Average Input Rows per Batch:**
    * Observed Value (Avg records): Varies, often between 0 and ~8 records per batch.
    * Notes: The number of records processed per batch fluctuates significantly.

---

## Observed Execution Metrics (Detailed Context)

Metrics observed from the Spark Application UI's "Executors" and "Stages" tabs, providing deeper context on resource utilization, task distribution, and time spent in different parts of the Spark job execution.

### Executor Summary

* **Number of Active/Total Executors:** Active (2) Total (2)
* **Total Cores Across Executors:** 1
* **Total Task Time :** 5.9 s
* **Total Input Read:** 63.6 KB
* **Total Shuffle Read/Write:** 63.6 KB Read, 63.6 KB Write

### Per-Executor Metrics

(Collect and summarize key data for each executor listed on the Executors tab, especially focusing on metrics like Task Time, CPU Usage (if shown), Input/Shuffle data, and Task counts. Note any significant differences between executors.)

* **Executor ID:** 0 
    * **Status:** Active
    * **Cores:** 1
    * **Memory (Total/Used):** 30.5 GB / 0.0 B
    * **Tasks (Active/Complete/Failed/Total):** 0 Active / 157 Complete / 0 Failed / 157 Total
    * **Total Task Time:** 5.9 s
    * **Input Read:** 63.6 KB
    * **Shuffle Read/Write:** 63.6 KB Read, 63.6 KB Write
    * **Notes:** Data only shows one executor (ID 0) with 1 core. Its metrics match the overall summary metrics, suggesting the summary might be for a single executor or there's a reporting anomaly. This executor completed 168 tasks with no failures.


---

## Analysis and Observations

* **Overall Pipeline Health:** Based on Input vs Process Rates and Batch Durations, The average process rate (1.06 records/sec) is slightly higher than the average input rate (1.04 records/sec), which suggests the pipeline is generally keeping up with the data being generated during this period. Batch durations are mostly reasonable, though with some spikes.
* **Identifying Bottlenecks:**
    * If **Average Batch Duration** Operation duration breaks down it suggests both Sink and Process operations take significant time within a batch.
    * If a specific operation/stage is slow (high **Stage Duration**), examine **Executor** metrics for that stage's tasks.Executor metrics show non-zero Task Time (5.9s) and Input Read/Shuffle (63.6 KB), indicating work is being done...

* **Resource Utilization Efficiency:** Executor(s) completed a significant number of tasks (168 total, 0 failed) with minimal total task time (5.9s) relative to the run duration (~5mins). This might suggest the workload is light for the allocated resources or tasks are spending time waiting.
* **Error Impact:** No failed tasks or stages are visible in the Executor summary. The provided logs for the streaming job in this sequence did not show database write errors.
* **External Dependency Impact:** The Spark job logs provided in this sequence do not show corresponding write errors, suggesting the connection has been stable during this specific run.
* **Comparison to Generator Rate:**  The average input rate (1.04 records/sec) suggests files are arriving at roughly that rate on average over the period.

---
## Supporting Data
Can be located in the img directory


---