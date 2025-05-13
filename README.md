# Real-Time Heartbeat Monitoring System

## Overview
This project implements a real-time heartbeat monitoring system using Kafka for streaming, PostgreSQL for storage, and Grafana for visualization. It focuses on tracking heart rate spikes and total record counts, deployed via Docker.

## Prerequisites
- Docker and Docker Compose
- Git
- Internet access for pulling Docker images

## Setup Guide
1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd heartbeat-monitoring-system
   ```
2. **Set Up Docker Environment**:
   - Ensure Docker is running.
   - Create a `docker-compose.yml` file (see project files or documentation for configuration).
3. **Start Services**:
   ```bash
   docker-compose up --build -d
   ```
   This starts Zookeeper, Kafka, PostgreSQL, producer, consumer, and Grafana.
4. **Verify Services**:
   - Check container status: `docker-compose ps`.
   - View logs: `docker logs <container-name>` (e.g., `fresh-kafka-producer-1`).
5. **Access Grafana**:
   - Open `http://localhost:3000` in a browser.
   - Default login: `admin/admin`.
6. **Configure Data Source**:
   - In Grafana, add a PostgreSQL data source named `HeartbeatDB` (UID `felo4ssnstji8c`).
   - Settings: Host `postgres:5432`, Database `heartbeat_db`, User `kafkapostgres`, Password `12345`.
7. **Import Dashboard**:
   - Go to **Create** > **Import**.
   - Upload `grafana_dashboard.json` (provided earlier) or paste its JSON.
   - Select `HeartbeatDB` as the data source.

## Documentation
- **Project Overview**: See `project_overview.md` for system components and data flow.
- **User Guide**: See `user_guide.md` for step-by-step instructions to run the project.
- **Test Cases**: See `test_cases.md` for manual test plans.
- **Performance Metrics**: See `performance_metrics.md` for latency and throughput data.

## Troubleshooting
- **No Data in Grafana**: Check PostgreSQL data (`SELECT * FROM heartbeat_data LIMIT 10;`), Kafka logs, and consumer logs.
- **Service Fails**: Restart with `docker-compose restart <service-name>`.
- **Network Issues**: Ensure all containers are on the same Docker network (`heartbeat-network`).