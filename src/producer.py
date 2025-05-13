import logging
import os
import sys
import json
import time
from datetime import datetime, timezone
from kafka.errors import KafkaError
from generator import generate_heartbeat_data
from utility import create_kafka_producer

# Configure logging for producer
logger = logging.getLogger("producer")
logger.setLevel(logging.INFO)
logger.handlers = []  # Clear any inherited handlers
file_handler = logging.FileHandler("logs/producer.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())

def run_producer():
    """Runs the producer to send heartbeat data to Kafka."""
    # Load environment variables
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "heartbeat_data")
    time_limit = os.environ.get("TIME_LIMIT", "continuous")
    
    if not all([bootstrap_servers, topic]):
        logger.error("Missing KAFKA_BOOTSTRAP_SERVERS or KAFKA_TOPIC")
        sys.exit(1)
    
    logger.info(f"Starting producer with bootstrap_servers={bootstrap_servers}, topic={topic}")
    
    # Parse time limit
    time_limit_seconds = None
    if time_limit.lower() != "continuous":
        try:
            time_limit_seconds = int(time_limit)
            if time_limit_seconds <= 0:
                raise ValueError
            logger.info(f"Running for {time_limit_seconds} seconds")
        except ValueError:
            logger.error("TIME_LIMIT must be 'continuous' or a positive integer")
            sys.exit(1)
    
    # Create producer
    producer = create_kafka_producer(bootstrap_servers)
    if not producer:
        logger.error("Failed to create Kafka Producer. Check Kafka broker connection.")
        sys.exit(1)
    
    # Generate and send data
    generator = generate_heartbeat_data()
    start_time = datetime.now(timezone.utc)
    count = 0
    
    try:
        for data in generator:
            if time_limit_seconds and (datetime.now(timezone.utc) - start_time).total_seconds() >= time_limit_seconds:
                logger.info(f"Time limit reached after sending {count} messages")
                break
            try:
                producer.send(topic, value=data)
                logger.debug(f"Sent data for customer {data['customer_id']}")
                count += 1
                time.sleep(0.1)  # Control rate
            except KafkaError as e:
                logger.error(f"Failed to send data: {e}")
        producer.flush()
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        producer.close()
        logger.info("Kafka Producer closed")
    
    logger.info(f"Producer sent {count} messages")
    sys.exit(0)

if __name__ == "__main__":
    run_producer()