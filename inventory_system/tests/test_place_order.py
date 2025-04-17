import os
from utility import *
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def test_place_order():
    db_connection = connect_db()
    if not db_connection:
        return

    try:
        cursor = db_connection.cursor()

        # --- Sample Order Data ---
        customer_id = 1  # Replace with an existing customer ID
        order_date = datetime.now()

        order_items = [
            {'product_id': 'P0001', 'quantity': 2, 'price': 19.99},
            {'product_id': 'P0002', 'quantity': 1, 'price': 49.99},
        ]
        # --- End Sample Order Data ---

        # --- Read SQL queries from files ---
        sql_dir = "sql_queries"
        with open(f"../deliverables/sql_queries/insert_order.sql", 'r') as f:
            insert_order_query = f.read()

        with open(f"../deliverables/sql_queries/insert_order_details.sql", 'r') as f:
            insert_order_details_query = f.read()

        # --- Insert into orders table ---
        cursor.execute(insert_order_query, (customer_id, order_date, 0.00)) # Initial total amount can be 0
        order_id = get_last_insert_id(cursor)
        print(f"New order placed with ID: {order_id}")

        # --- Insert into order_details table ---
        for item in order_items:
            product_id = item['product_id']
            quantity = item['quantity']
            price = item['price']

            # Insert into order_details
            cursor.execute(insert_order_details_query, (order_id, product_id, quantity, price))

        db_connection.commit()
        print("Order details added. Triggers will handle stock deduction and total amount update.")

    except Exception as e:
        db_connection.rollback()
        print(f"An error occurred during order placement: {e}")
    finally:
        close_connection(db_connection)

if __name__ == "__main__":
    test_place_order()