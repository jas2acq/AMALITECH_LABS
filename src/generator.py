import logging
import random
import uuid
from datetime import datetime, timezone

# Configure logging for generator
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []  # Clear any inherited handlers
file_handler = logging.FileHandler("logs/generator.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())

def generate_heartbeat_data():
    """
    Generates synthetic heartbeat data for random customers.

    Yields:
        dict: Data point with customer_id, timestamp, and heart_rate.
    """
    logger.info("Starting heartbeat data generation")
    while True:
        # Generate unique customer ID
        customer_id = f"CUST-{str(uuid.uuid4())[:8]}"
        # Normal heart rate range: 60-100 BPM
        heart_rate = random.randint(60, 100)
        # 5% chance of anomaly (high/low spike)
        if random.random() < 0.05:
            spike = random.randint(20, 50) * random.choice([1, -1])
            heart_rate = max(10, heart_rate + spike)  # Ensure minimum 10 BPM
        # Current timestamp in ISO format
        timestamp = datetime.now(timezone.utc).isoformat()
        
        data_point = {
            "customer_id": customer_id,
            "timestamp": timestamp,
            "heart_rate": heart_rate
        }
        logger.debug(f"Generated: {data_point}")
        yield data_point