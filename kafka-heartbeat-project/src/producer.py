import time
import logging
import os
import json
from datetime import datetime, timezone
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Define the logs directory path
# Ensure the 'logs' directory exists in the project root before running scripts.
LOGS_DIR = "AMALITECH_LABS\kafka-heartbeat-project\logs"

# Configuring logging for the producer here allows standalone testing.
logging.basicConfig(
    level=logging.INFO, # Set the minimum logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', # Define log message format
    handlers=[
        logging.StreamHandler(), # Log to console
        # Ensure the LOGS_DIR exists before the FileHandler is created
        logging.FileHandler(os.path.join(LOGS_DIR, "producer.log")) # Log to a file inside the logs directory
    ]
)

# Get a logger for this module
logger = logging.getLogger(__name__)

def create_kafka_producer(bootstrap_servers):
    """
    Creates and configures a Kafka Producer instance.

    Args:
        bootstrap_servers (str or list): List of 'host[:port]' strings
                                         that the producer should contact to
                                         bootstrap initial cluster metadata.

    Returns:
        KafkaProducer: Configured Kafka Producer instance.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            # Serialize data to JSON format
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            # Optional: Configure retries and timeout
            retries=5,
            request_timeout_ms=10000 # Timeout for sending requests
        )
        logger.info(f"Kafka Producer created successfully for brokers: {bootstrap_servers}")
        return producer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        return None

# Callback function for successful delivery
def on_send_success(metadata):
    """Logs successful message delivery."""
    logger.debug(f"Message delivered to topic {metadata.topic} [partition {metadata.partition}] at offset {metadata.offset}")

# Callback function for delivery errors
def on_send_error(excp):
    """Logs message delivery errors."""
    logger.error('Error sending message', exc_info=excp)


def send_heartbeat_data(producer, topic, data):
    """
    Sends a single heartbeat data point to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka Producer instance.
        topic (str): The Kafka topic to send data to.
        data (dict): The heartbeat data point (customer_id, timestamp, heart_rate).
    """
    if producer is None:
        logger.error("Producer is not initialized. Cannot send data.")
        return

    try:
        # Send the data asynchronously
        future = producer.send(topic, value=data)

        # Add callback for successful delivery
        future.add_callback(on_send_success)
        # Add callback for delivery errors
        future.add_errback(on_send_error)

        # Note: The logger.debug here indicates the message was added to the
        # producer's internal buffer, not necessarily delivered to Kafka yet.
        # The callbacks handle the actual delivery outcome.
        logger.debug(f"Queued data point for topic {topic}: {data}")

    except KafkaError as e:
        # This catches immediate errors when queuing the message
        logger.error(f"Failed to queue data for topic {topic}: {e}")


if __name__ == "__main__":
    # This block is for testing the producer function in isolation.
    # It generates dummy data to send to Kafka.

    logger.info("Running producer.py in standalone test mode.")

    # Define Kafka broker and topic for testing
    # In a real scenario, these would come from environment variables or config
    TEST_KAFKA_BROKER = "localhost:9092"
    TEST_KAFKA_TOPIC = "test_heartbeats" # Using a different topic for testing

    # Create a producer instance
    test_producer = create_kafka_producer(TEST_KAFKA_BROKER)

    if test_producer:
        try:
            logger.info(f"Sending test messages to topic: {TEST_KAFKA_TOPIC}")
            # Send a few dummy messages
            for i in range(5):
                dummy_data = {
                    "customer_id": f"TEST-CUST-{i}",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "heart_rate": random.randint(50, 110)
                }
                send_heartbeat_data(test_producer, TEST_KAFKA_TOPIC, dummy_data)
                time.sleep(1) # Small delay between test messages

            # It's important to call flush() to ensure all buffered messages are sent
            # and wait for callbacks to complete before exiting.
            logger.info("Flushing producer to ensure all messages are sent.")
            test_producer.flush()
            logger.info("Finished sending test messages.")

        except Exception as e:
            logger.error(f"An error occurred during standalone producer test: {e}")
        finally:
            # Close the producer connection
            if test_producer: # Check if producer was successfully created
                test_producer.close()
                logger.info("Kafka Producer closed.")
    else:
        logger.error("Could not create producer. Check Kafka broker connection and Docker Compose setup.")
