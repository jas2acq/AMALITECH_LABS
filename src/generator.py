import csv
import time
import uuid
import names
import random
from datetime import datetime
import sys
import os
import logging 

# --- Logging Setup ---
# Define the directory and file path for the log file INSIDE the container.
# This path should be within the volume mounted from your host's local 'log' directory.
# Ensure the log directory exists inside the container.
CONTAINER_LOG_DIR = "/app/log" # Directory where logs will be mounted inside the container
LOG_FILE_PATH = os.path.join(CONTAINER_LOG_DIR, "generator.log") # Full path to the log file




# Configure the basic logger.
# Messages with level INFO or higher will be written to the specified file.
# The format includes timestamp, log level, and the message.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=LOG_FILE_PATH, # Specify the log file path
                    filemode='a') # Append to the log file

# Adding a handler to also output logs to the console (stdout/stderr) for visibility
# via 'docker compose logs'. This is very helpful for debugging.
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO) # Set level for console output
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s') # Same format as file
console_handler.setFormatter(formatter)
# Prevent adding handlers multiple times if the script is somehow reloaded
if not logging.getLogger().handlers:
    logging.getLogger().addHandler(console_handler)


# --- Product Catalog ---
# Fixed catalog of products with their names and unit prices.
product_catalog = {
    "P101": ("Smartphone", 799.99),
    "P102": ("Laptop", 1200.00),
    "P103": ("Digital Camera", 450.00),
    "P104": ("Men's Sneakers", 120.00),
    "P105": ("Women's Handbag", 75.00),
    "P106": ("Gaming Console", 350.00),
    "P107": ("Bluetooth Speaker", 90.00),
    "P108": ("Office Chair", 200.00),
    "P109": ("Wireless Earbuds", 85.00),
    "P110": ("Fitness Tracker", 150.00)
}

def generate_customer():
    """
    Generates and yields details for a new, unique customer dynamically.

    This is a generator function that runs indefinitely, producing a dictionary
    for one customer with unique ID, random name, and random age in each iteration.

    Yields:
        dict: A dictionary containing the generated details for a single customer.
              Includes 'customer_id', 'first_name', 'last_name', and 'age'.
    """
    # Use a while True loop to create an infinite generator.
    while True:
        # The 'yield' keyword makes this a generator function.
        yield {
            # Generate a unique customer ID with "CUST-" prefix
            "customer_id": "CUST-" + str(uuid.uuid4())[:6],
            # Generate a random first name
            "first_name": names.get_first_name(),
            # Generate a random last name
            "last_name": names.get_last_name(),
            # Generate a random integer age
            "age": random.randint(18, 40)
        }

def generate_order():
    """
    Generates and yields order details dynamically, including total quantity & total price.

    This generator produces a dictionary for a single order with a unique ID, timestamp,
    calculated total quantity and price, and a list of products purchased in that order.

    Yields:
        dict: A dictionary containing the generated details for a single order.
              Includes 'order_id', 'time_stamp', 'Total quantity', 'Total Price',
              and a list of 'Products' dictionaries.
    """
    # Use a while True loop to create an infinite generator.
    while True:
        num_products = random.randint(1, 5) # Random number of products per purchase
        total_quantity = 0
        total_price = 0
        products = [] # Store product details for this order

        for _ in range(num_products):
            # Randomly select a product from the catalog
            product_id, (product_name, price) = random.choice(list(product_catalog.items()))
            quantity = random.randint(1, 5) # Random quantity for this product
            total_item_price = round(quantity * price, 2) # Calculate total price for this item

            total_quantity += quantity
            total_price += total_item_price

            # Append details for this item to the products list
            products.append({
                "product_id": product_id,
                "product_name": product_name,
                "quantity": quantity,
                "unit_price": price,
                "Total Item Price": total_item_price
            })

        # Yield the complete order details
        yield {
            # Generate a unique order ID with "ORDER-" prefix
            "order_id": "ORDER-" + str(uuid.uuid4())[:6],
            # Generate the current timestamp in a specific format
            "time_stamp": datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
            # Include calculated total quantity and price for the order
            "Total quantity": total_quantity,
            "Total Price": round(total_price, 2),
            # Include the list of products purchased
            "Products": products
        }

def generate_purchase():
    """
    Pairs customer and order data, formats it, writes to a CSV file,
    and yields the filename dynamically upon successful write.

    This generator combines data from generate_customer and generate_order,
    writes each complete purchase record (flattened with product details)
    into a separate CSV file within the mounted data directory, and yields
    the path of the successfully created file. Includes error handling for file writing.

    Yields:
        str: The full path to the CSV file that was successfully generated and written.
             (Only yields on successful write).
    """
    customer_gen = generate_customer() # Get the customer generator
    order_gen = generate_order()       # Get the order generator

    # Use a while True loop to keep generating and writing purchases indefinitely.
    while True:
        customer = next(customer_gen) # Get the next customer from the generator
        order = next(order_gen)       # Get the next order from the generator

        # Define the output directory INSIDE the container.
        # This should be the path mounted from your host's ./data directory.
        output_dir = "/opt/spark/data"

        # Construct the filename using order ID and timestamp, within the output directory.
        filename = f"{output_dir}/purchase_{order['order_id']}_{order['time_stamp']}.csv"

        try:
            # Open the file for writing in text mode ('w') with newline='' for proper CSV handling.
            with open(filename, mode="w", newline="") as file:
                writer = csv.writer(file) # Create a CSV writer object

                # Write the header row to the CSV file.
                writer.writerow(["customer_id", "first_name", "last_name", "age",
                                 "order_id", "time_stamp", "Total_quantity", "Total_Price",
                                 "product_id", "Product_Name", "quantity", "Unit_Price", "Total_Item_Price"])

                # Iterate over each product in the order and write a row to the CSV.
                # This flattens the order data so each row represents a purchased item
                # with associated customer and order details.
                for product in order["Products"]:
                    writer.writerow([
                        customer["customer_id"], customer["first_name"], customer["last_name"],
                        customer["age"], order["order_id"], order["time_stamp"], order["Total quantity"],
                        order["Total Price"], product["product_id"], product["product_name"],
                        product["quantity"], product["unit_price"], product["Total Item Price"]
                    ])
            
            # Log that the file was successfully generated.
            logging.info(f"Generated: {filename}")

            # Yield the filename dynamically upon successful file write.
            yield filename

        except Exception as e:
            # Logging any errors that occur during file writing.
            # Using logging.error to indicate a problem.
            logging.error(f"Error writing file {filename}: {e}", exc_info=True) # exc_info=True logs traceback
            # Don't yield the filename if the write failed.
            raise e 


# --- Main execution logic ---
if __name__ == "__main__":
    
    time_limit_seconds = None
    delay_seconds = 2

    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg == "continuous":
            time_limit_seconds = None
            logging.info("Running continuously (no time limit)...")
        else:
            try:
                time_limit_seconds = int(arg)
                if time_limit_seconds <= 0:
                    logging.warning(f"Invalid time limit '{arg}'. Please provide a positive integer or 'continuous'. Defaulting to continuous.")
                    time_limit_seconds = None
                else:
                    logging.info(f"Running for {time_limit_seconds} seconds...")
            except ValueError:
                logging.warning(f"Invalid time limit argument '{arg}'. Please provide a positive integer or 'continuous'. Defaulting to continuous.")
                time_limit_seconds = None
    else:
        logging.info("No time limit argument provided. Defaulting to continuous...")
        time_limit_seconds = None

    start_time = time.time()
    purchase_generator = generate_purchase()
    purchase_count = 0

    while True:
        if time_limit_seconds is not None and (time.time() - start_time) > time_limit_seconds:
            logging.info(f"Time limit ({time_limit_seconds} seconds) reached. Stopping after generating {purchase_count} purchases.")
            break

        try:
            # Get the next purchase. If generate_purchase encounters a file writing error,
            # it will re-raise the exception, which will be caught by the
            # general except Exception block below.
            next(purchase_generator)
            purchase_count += 1

            # Apply the delay
            time.sleep(delay_seconds)

        except StopIteration:
             # Should not happen with while True in generator, but good to have.
             logging.info("Generator exhausted unexpectedly. Stopping.")
             break

        except KeyboardInterrupt:
            # Handle user interruption (e.g., Ctrl+C).
            logging.info("\nScript interrupted by user. Stopping.")
            break

        # --- General Exception Handler ---
        # This will catch exceptions re-raised by generate_purchase (file writing errors)
        # and any other unexpected errors in the main loop.
        except Exception as e:
            logging.error(f"An unexpected error occurred during purchase generation: {e}. Stopping.", exc_info=True)
            break # Stop the main loop on error

    logging.info("Script finished.")