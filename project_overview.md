# Project Overview: Real-Time Heartbeat Monitoring System

## System Components
- **Producer**: Generates simulated heart rate data and publishes to a Kafka topic (`heartbeat`).
- **Kafka Cluster**: Streams data, managed by Zookeeper with a single topic.
- **Consumer**: Reads from Kafka and writes to PostgreSQL (`HeartbeatDB`).
- **PostgreSQL (HeartbeatDB)**: Stores the `heartbeat_data` table with columns `time_stamp`, `customer_id`, and `heart_rate`.
- **Grafana**: Visualizes data with two panels:
  - "Heart Rate Spikes": Timeseries showing summed heart rates by timestamp.
  - "Record Count": Stat showing total customer IDs.

## Data Flow
1. The producer generates heart rate data every few seconds and sends it to the Kafka `heartbeat` topic.
2. Kafka streams the data to the consumer.
3. The consumer processes messages and writes to the `heartbeat_data` table in PostgreSQL.
4. Grafana queries `heartbeat_data` to display:
   - "Heart Rate Spikes" (sum of heart rates grouped by timestamp, limited to 50 points).
   - "Record Count" (count of customer IDs, limited to 50 records).

## Design Choices
- **Kafka**: Used for real-time streaming with a single topic for simplicity.
- **PostgreSQL**: Chosen for reliable storage and compatibility with Grafana.
- **Grafana**: Provides visualization with a focus on spikes and counts, using a 5-second refresh.
- **Docker**: Ensures consistent deployment across environments.