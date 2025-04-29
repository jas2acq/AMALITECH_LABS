import csv
import time
import uuid
import names
import random
from datetime import datetime

# Product catalog with fixed prices
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
    """Yields customer details dynamically."""
    while True:
        yield {
            "Customer ID": "CUST-" + str(uuid.uuid4())[:6],
            "First Name": names.get_first_name(),
            "Last Name": names.get_last_name(),
            "Age": random.randint(18, 40)
        }

def generate_order():
    """Yields order details dynamically, including total quantity & total price."""
    while True:
        num_products = random.randint(1, 5)  # Random number of products per purchase
        total_quantity = 0
        total_price = 0
        products = []  # Store product details

        for _ in range(num_products):
            product_id, (product_name, price) = random.choice(list(product_catalog.items()))
            quantity = random.randint(1, 5)
            total_item_price = round(quantity * price, 2)

            total_quantity += quantity
            total_price += total_item_price

            products.append({
                "Product ID": product_id,
                "Product Name": product_name,
                "Quantity": quantity,
                "Unit Price": price,
                "Total Item Price": total_item_price
            })

        yield {
            "Order ID": "ORDER-" + str(uuid.uuid4())[:6],
            "Timestamp": datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
            "Total Quantity": total_quantity,
            "Total Price": round(total_price, 2),
            "Products": products
        }

def generate_purchase():
    """Pairs customer and order data, yielding complete purchases dynamically."""
    customer_gen = generate_customer()
    order_gen = generate_order()

    while True:  # Keep yielding purchases dynamically
        customer = next(customer_gen)
        order = next(order_gen)

        filename = f"AMALITECH_LABS\Stream_pyspark\data\purchase_{order['Order ID']}_{order['Timestamp']}.csv"

        with open(filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Customer ID", "First Name", "Last Name", "Age", 
                            "Order ID", "Timestamp", "Total Quantity", "Total Price", 
                            "Product ID", "Product Name", "Quantity", "Unit Price", "Total Item Price"])  # Header

            for product in order["Products"]:
                writer.writerow([
                    customer["Customer ID"], customer["First Name"], customer["Last Name"], 
                    customer["Age"], order["Order ID"], order["Timestamp"], order["Total Quantity"], 
                    order["Total Price"], product["Product ID"], product["Product Name"], 
                    product["Quantity"], product["Unit Price"], product["Total Item Price"]
                ])

        yield filename  # Yield file name dynamically

# Call `generate_purchase()` only with the desired number of purchases
for purchase_file in zip(range(5), generate_purchase()):  # Generate 5 purchases on demand
    print(f"Generated: {purchase_file[1]}")
    time.sleep(4)  # Simulate delay between transactions