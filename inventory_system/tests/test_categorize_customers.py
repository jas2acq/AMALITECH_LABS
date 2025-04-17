import os
from utility import *
from dotenv import load_dotenv

load_dotenv()

def test_categorize_customers():
    db_connection = connect_db()
    if not db_connection:
        return

    try:
        categorize_customers_script_path = "../deliverables/replenishment_system/categorise_customers.sql"
        execute_sql_script(db_connection, categorize_customers_script_path)
        print("\n--- Customer Categorization Test ---")
        print(f"Successfully executed SQL script: {categorize_customers_script_path}")

    except Exception as e:
        print(f"An error occurred during customer categorization: {e}")
    finally:
        close_connection(db_connection)

if __name__ == "__main__":
    test_categorize_customers()