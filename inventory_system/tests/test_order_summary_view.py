import os
from utility import *
from dotenv import load_dotenv
# load_dotenv() # No need to load environment variables now

def test_order_summary_view():
    db_connection = connect_db()
    if not db_connection:
        return

    try:
        schema_path = r"AMALITECH_LABS\inventory_system\deliverables\database_schema\schema.sql"
        view_script_path = r"AMALITECH_LABS\inventory_system\deliverables\views\order_summary_view.sql"

        # Execute schema to ensure tables exist
        execute_sql_script(db_connection, schema_path)

        # Execute the view script to create the view
        execute_sql_script(db_connection, view_script_path)

        cursor = db_connection.cursor()

        # Try to query the order_summary_view to check if it was created
        query = "SELECT customer_name, order_id FROM order_summary_view LIMIT 1"  # Query a small amount of data
        cursor.execute(query)
        results = cursor.fetchall()
        print("\n--- Order Summary View Creation Test ---")
        print("Order Summary View created successfully and is queryable.")
        if results:
            print("Sample results:", results)
        else:
            print("No data in the view (this is expected if no orders exist).")

    except Exception as e:
        print(f"An error occurred during view creation or querying: {e}")
    finally:
        close_connection(db_connection)

if __name__ == "__main__":
    test_order_summary_view()