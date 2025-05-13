import logging
import time
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaConnectionError
import psycopg2

# Configure logging for utility
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []  # Clear any inherited handlers
file_handler = logging.FileHandler("logs/utility.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())

def create_kafka_producer(bootstrap_servers):
    """
    Creates a Kafka Producer with retry logic.

    Args:
        bootstrap_servers (str): Comma-separated list of Kafka brokers.

    Returns:
        KafkaProducer: Configured producer, or None if connection fails.
    """
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Creating Kafka Producer (attempt {attempt}/{retries})")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                value_serializer=lambda x: json.dumps(x).encode("utf-8")
            )
            logger.info("Kafka Producer created")
            return producer
        except (NoBrokersAvailable, KafkaConnectionError) as e:
            logger.warning(f"Failed to connect to Kafka: {e}. Retrying in 5s...")
            time.sleep(5)
    logger.error("Failed to create Kafka Producer after retries")
    return None

def create_kafka_consumer(bootstrap_servers, topic, group_id):
    """
    Creates a Kafka Consumer with retry logic.

    Args:
        bootstrap_servers (str): Comma-separated list of Kafka brokers.
        topic (str): Kafka topic to consume.
        group_id (str): Consumer group ID.

    Returns:
        KafkaConsumer: Configured consumer, or None if connection fails.
    """
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Creating Kafka Consumer (attempt {attempt}/{retries})")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers.split(","),
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            logger.info("Kafka Consumer created")
            return consumer
        except (NoBrokersAvailable, KafkaConnectionError) as e:
            logger.warning(f"Failed to connect to Kafka: {e}. Retrying in 5s...")
            time.sleep(5)
    logger.error("Failed to create Kafka Consumer after retries")
    return None

def create_db_connection(db_host, db_name, db_user, db_password, db_port):
    """
    Creates a PostgreSQL connection with retry logic for Dockerized database.

    Args:
        db_host (str): Database host (e.g., 'postgres' in Docker).
        db_name (str): Database name.
        db_user (str): Database user.
        db_password (str): Database password.
        db_port (str): Database port (e.g., '5432' inside Docker).

    Returns:
        psycopg2.connection: Database connection, or None if connection fails.
    """
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Connecting to PostgreSQL (attempt {attempt}/{retries})")
            conn = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_password,
                port=db_port
            )
            logger.info("PostgreSQL connection established")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"Failed to connect to PostgreSQL: {e}. Retrying in 5s...")
            time.sleep(5)
    logger.error("Failed to connect to PostgreSQL after retries")
    return None