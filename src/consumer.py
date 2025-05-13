import logging
import os
import sys
import time
import psycopg2
from datetime import datetime
from kafka.errors import KafkaError
from utility import create_kafka_consumer, create_db_connection

# Configure logging for consumer
logger = logging.getLogger("consumer")
logger.setLevel(logging.INFO)
logger.handlers = []  # Clear any inherited handlers
file_handler = logging.FileHandler("logs/consumer.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())

def process_message(message, db_conn):
    """
    Processes a Kafka message and inserts it into the database.

    Args:
        message: Kafka message object.
        db_conn: PostgreSQL connection.
    """
    try:
        data = message.value
        customer_id = data.get("customer_id")
        timestamp_str = data.get("timestamp")
        heart_rate = data.get("heart_rate")
        
        if not all([customer_id, timestamp_str, heart_rate is not None]):
            logger.warning(f"Invalid message at offset {message.offset}: {data}")
            return
        
        # Validate heart rate
        if not (10 <= heart_rate <= 200):
            logger.warning(f"Invalid heart_rate {heart_rate} at offset {message.offset}")
            return
        
        # Parse timestamp
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except ValueError as e:
            logger.warning(f"Invalid timestamp at offset {message.offset}: {e}")
            return
        
        # Insert into database
        sql = """
            INSERT INTO heartbeat_data (customer_id, time_stamp, heart_rate)
            VALUES (%s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE
            SET time_stamp = EXCLUDED.time_stamp, heart_rate = EXCLUDED.heart_rate;
        """
        with db_conn.cursor() as cursor:
            cursor.execute(sql, (customer_id, timestamp, heart_rate))
        db_conn.commit()
        logger.info(f"Inserted data for customer {customer_id}")
    except psycopg2.Error as e:
        logger.error(f"Database error for customer {customer_id}: {e}")
        db_conn.rollback()
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)

def run_consumer():
    """Runs the consumer to process Kafka messages and store in PostgreSQL."""
    # Load environment variables
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "heartbeat_data")
    group_id = os.environ.get("KAFKA_CONSUMER_GROUP_ID", "heartbeat_group")
    db_host = os.environ.get("POSTGRES_HOST", "postgres")
    db_name = os.environ.get("POSTGRES_DB", "heartbeat_db")
    db_user = os.environ.get("POSTGRES_USER", "kafkapostgres")
    db_password = os.environ.get("POSTGRES_PASSWORD", "12345")
    db_port = os.environ.get("POSTGRES_PORT", "5432")
    
    if not all([bootstrap_servers, topic, db_host, db_name, db_user, db_password]):
        logger.error("Missing required environment variables")
        sys.exit(1)
    
    logger.info(f"Starting consumer with bootstrap_servers={bootstrap_servers}, topic={topic}")
    
    # Create connections
    consumer = create_kafka_consumer(bootstrap_servers, topic, group_id)
    db_conn = create_db_connection(db_host, db_name, db_user, db_password, db_port)
    
    if not consumer or not db_conn:
        logger.error("Failed to initialize consumer or database")
        if consumer:
            consumer.close()
        if db_conn and not db_conn.closed:
            db_conn.close()
        sys.exit(1)
    
    try:
        logger.info("Starting consumer loop")
        for message in consumer:
            logger.info(f"Received message for customer {message.value.get('customer_id')}")
            process_message(message, db_conn)
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka Consumer closed")
        if db_conn and not db_conn.closed:
            db_conn.close()
            logger.info("PostgreSQL connection closed")
    
    logger.info("Consumer shutdown complete")
    sys.exit(0)

if __name__ == "__main__":
    run_consumer()