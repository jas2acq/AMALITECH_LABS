# User Guide: Running the Real-Time Heartbeat Monitoring System

## Step-by-Step Instructions
1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd heartbeat-monitoring-system
   ```
2. **Set Up Docker Compose**:
   - Ensure `docker-compose.yml` is in the project root (refer to previous setup or create one with Zookeeper, Kafka, PostgreSQL, producer, consumer, and Grafana services).
3. **Start the System**:
   ```bash
   docker-compose up --build -d
   ```
   - Wait for all services to start (check with `docker-compose ps`).
4. **Verify Data Ingestion**:
   - Check producer logs: `docker logs fresh-kafka-producer-1`.
   - Check consumer logs: `docker logs fresh-kafka-consumer-1`.
   - Query PostgreSQL:
     ```bash
     docker exec -it <postgres-container> psql -U kafkapostgres -d heartbeat_db -c "SELECT * FROM heartbeat_data LIMIT 10;"
     ```
5. **Access Grafana Dashboard**:
   - Open `http://localhost:3000` in a browser.
   - Log in with `admin/admin`.
6. **Add Data Source**:
   - Go to **Configuration** > **Data Sources** > **Add data source**.
   - Select PostgreSQL.
   - Configure: Host `postgres:5432`, Database `heartbeat_db`, User `kafkapostgres`, Password `12345`, UID `felo4ssnstji8c`.
   - Save and test the connection.
7. **Import the Dashboard**:
   - Go to **Create** > **Import**.
   - Upload `grafana_dashboard.json` (provided earlier) or paste its JSON content.
   - Select `HeartbeatDB` (UID `felo4ssnstji8c`) as the data source and import.
8. **Interact with the Dashboard**:
   - View the "Kafka Dashboard".
   - Monitor "Heart Rate Spikes" for aggregated heart rates and "Record Count" for total records.

## Stopping the System
- Stop services: `docker-compose down`.
- To remove volumes (data): `docker-compose down -v`.