# Test Cases: Manual Test Plan

## Test Plan Overview
This manual test plan verifies the functionality of the Real-Time Heartbeat Monitoring System, focusing on data ingestion, heart rate spikes aggregation, and record counting.

## Test Cases

### Test Case 1: Data Ingestion
- **Description**: Verify that the producer sends data to Kafka and the consumer writes to PostgreSQL.
- **Steps**:
  1. Start all services with `docker-compose up --build -d`.
  2. Check producer logs: `docker logs fresh-kafka-producer-1`.
  3. Check consumer logs: `docker logs fresh-kafka-consumer-1`.
  4. Query PostgreSQL: `docker exec -it <postgres-container> psql -U kafkapostgres -d heartbeat_db -c "SELECT COUNT(*) FROM heartbeat_data;"`.
- **Expected Outcome**: Producer logs show data being sent, consumer logs show data being processed, and PostgreSQL shows >0 records.
- **Actual Outcome**: To be recorded after testing.

### Test Case 2: Heart Rate Spikes
- **Description**: Ensure the "Heart Rate Spikes" panel aggregates heart rates correctly.
- **Steps**:
  1. Access Grafana at `http://localhost:3000`.
  2. Open the "Kafka Dashboard" and view the "Heart Rate Spikes" panel.
  3. Compare with PostgreSQL query: `SELECT time_stamp, SUM(heart_rate) FROM heartbeat_data GROUP BY time_stamp LIMIT 50;`.
- **Expected Outcome**: The panel matches the PostgreSQL query results, showing summed heart rates over time.
- **Actual Outcome**: To be recorded after testing.

### Test Case 3: Record Count
- **Description**: Verify the "Record Count" panel displays the correct total.
- **Steps**:
  1. View the "Record Count" panel in Grafana.
  2. Query PostgreSQL: `SELECT COUNT(customer_id) FROM heartbeat_data LIMIT 50;`.
- **Expected Outcome**: The panel value matches the PostgreSQL query result.
- **Actual Outcome**: To be recorded after testing.

## Notes
- Record actual outcomes after executing each test.
- If a test fails, check logs and troubleshoot as per the README.