import time
import uuid
import random
import logging # Import the logging module
import json # Import json to format log messages
import os 
from datetime import datetime, timezone, timedelta
from utility import *


# Define the logs directory path
# Ensure the 'logs' directory exists in the project root before running scripts.
LOGS_DIR = "AMALITECH_LABS\kafka-heartbeat-project\logs"


# Configuring logging for standalone testing.
logging.basicConfig(
    level=logging.INFO, # Set the minimum logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', # Define log message format
    handlers=[
        logging.StreamHandler(), # Log to console
        logging.FileHandler(os.path.join(LOGS_DIR, "generator.log")) # Log to a file inside the logs directory
    ]
)

# Get a logger for this module
logger = logging.getLogger(__name__)


def generate_heartbeat_data():
    """
    Generates synthetic heartbeat data for an unbounded number of customers.
    Yields data points continuously, generating a new customer_id for each.
    """
    logger.info("Starting continuous heartbeat data generation for customers.")
    while True:
        # Generate a unique customer ID for each data point
        customer_id = f"CUST-{str(uuid.uuid4())[:8]}"

        # Realistic heart rate range = 60-100 BPM
        base_heart_rate = random.randint(60, 100)

        # Initialize heart_rate with the base value BEFORE the spike logic
        heart_rate = base_heart_rate

        # Introduce occasional higher/lower spikes for anomaly simulation
        if random.random() < 0.05: # 5% chance of a spike
             spike_amount = random.randint(20, 50)
             heart_rate += spike_amount if random.random() < 0.5 else -spike_amount
             # Ensure heart rate doesn't go below a minimum plausible value
             heart_rate = max(10, heart_rate)


        # Get current timestamp
        timestamp = datetime.now(timezone.utc).isoformat()

        # Create the data point
        data_point = {
            "customer_id": customer_id,
            "timestamp": timestamp,
            "heart_rate": heart_rate
        }

        # Choose your prefered log level here
        
        # Log the generated data point using debug level
        # logger.debug(f"Generated data point: {data_point}")

        # Log the generated data point using info level
        logger.info(f"Generated data point for {customer_id}")

        yield data_point

        # Adding a small delay between data points
        time.sleep(0.1)


if __name__ == "__main__":
    # This block is for testing the generator function in isolation with a time limit.
    logger.info("Running generator.py in standalone test mode with time limit.")

    generation_duration_seconds = 7 # Set a short duration for standalone testing

    logger.info(f"Generating heartbeat data for {generation_duration_seconds} seconds...")

    start_time = datetime.now(timezone.utc) # Use timezone-aware UTC for start time
    end_time = start_time + timedelta(seconds=generation_duration_seconds)

    try:
        # Continuously generate and print data points until the time limit is reached
        for heartbeat in generate_heartbeat_data():
            print(heartbeat) # Print for standalone test visibility

            # Check if the time limit has been reached
            if datetime.now(timezone.utc) >= end_time: # Use timezone-aware UTC for check
                logger.info(f"Generation duration of {generation_duration_seconds} seconds reached. Stopping.")
                break # Exit the loop

            # Simulate real-time generation speed
            time.sleep(0.1) # Adjust sleep time as needed for test speed

    except KeyboardInterrupt:
        logger.info("Standalone data generation test stopped manually.")
    except Exception as e:
        logger.error(f"An error occurred during standalone test generation: {e}")

    logger.info("Standalone generator test finished.")


