# Kafka Heartbeat Data Pipeline

This project demonstrates a real-time data pipeline using Apache Kafka, Python, and PostgreSQL. It simulates a stream of customer heartbeat data, ingests it into Kafka, consumes and processes it, and finally stores it in a relational database.

The core components are:

- **Synthetic Data Generator (`generator.py`)**: A Python script that generates random heartbeat data.
- **Kafka Producer (`producer.py`)**: Handles the logic for sending data to a Kafka topic.
- **Kafka Consumer (`consumer.py`)**: Reads data from the Kafka topic, performs basic processing, and writes it to PostgreSQL.
- **Main Orchestration Script (`main.py`)**: Orchestrates the data generator and producer.
- **Utility Functions (`utility.py`)**: Contains shared helper functions used across other Python scripts.
- **Apache Kafka**: The distributed event streaming platform acting as the central message bus.
- **Apache ZooKeeper**:Coordinates the Kafka brokers.
- **PostgreSQL Database**: Stores the processed heartbeat data.

The project is organized into the following directories and files:

```
kafka-heartbeat-project/
├── docker/
│   └── docker-compose.yml    # Defines Kafka, Zookeeper, and PostgreSQL services
├── src/
│   ├── generator.py          # Generates synthetic heartbeat data
│   ├── producer.py           # Handles sending data to Kafka
│   ├── consumer.py           # Reads from Kafka, processes, and writes to DB
│   ├── main.py               # Orchestrates the data generation and production
│   └── utility.py            # Contains shared helper functions
├── sql/
│   └── create_table.sql      # SQL script to create the database table
├── .gitignore                # Specifies intentionally untracked files that Git should ignore
└── README.md                 # Project documentation (this file)
```

_Architecture Diagram: (We will add a diagram here later to visualize the flow)_

Before you begin, ensure you have the following installed on your system:

- **Docker**: [Get Docker](https://www.docker.com/)
- **Docker Compose**: (Usually comes bundled with Docker Desktop, otherwise follow [Compose installation instructions](https://docs.docker.com/compose/install/))
- **Python 3.6+**: [Python Downloads](https://www.python.org/downloads/)
- **venv module** (usually included with Python 3.3+): For creating virtual environments

---

## Getting Started

Follow these steps to set up the project locally and start the core infrastructure.

### 1. Clone the Repository

First, clone this repository to your local machine:

```bash
git clone  # Replace  with the actual URL
cd kafka-heartbeat-project
```

### 2. Set up a Python Virtual Environment

It's highly recommended to use a virtual environment to manage project dependencies.

```bash
python -m venv .venv
```

Activate the virtual environment:

- On macOS and Linux:
    ```bash
    source .venv/bin/activate
    ```
- On Windows:
    ```bash
    .venv\Scripts\activate
    ```

Your terminal prompt should now indicate that you are in the virtual environment (e.g., `(.venv) kafka-heartbeat-project$`).

### 3. Install Python Dependencies

We will install the necessary Python libraries for Kafka and PostgreSQL interaction later when we develop the Python scripts. For now, just ensure your virtual environment is active.

### 4. Set up and Start Infrastructure with Docker Compose

We have created the `docker/docker-compose.yml` file which defines the services needed for this project (Kafka, ZooKeeper, and PostgreSQL). Navigate to the docker directory and start the containers using Docker Compose:

```bash
cd docker
docker-compose up -d
```

- `docker-compose up`: Builds (if necessary) and starts the services defined in `docker-compose.yml`.
- `-d`: Runs the containers in detached mode (in the background).

This command will download the necessary Docker images and start the containers for Kafka, ZooKeeper, and PostgreSQL. This may take some time the first time you run it.

You can check the status of the running containers with:

```bash
docker-compose ps
```

You should see output indicating that the zookeeper, kafka, and postgres services are running.

### 5. Stop the Infrastructure

When you are finished working, you can stop the containers:

```bash
cd docker # Make sure you are in the docker directory
docker-compose down
```

This will stop and remove the containers, networks, and volumes created by `up`.

---

Continue to the next sections of the README for developing the data generator, producer, consumer, and database schema.

---
