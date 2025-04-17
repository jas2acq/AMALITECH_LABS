import os
from utility import *
from dotenv import load_dotenv

def test_low_stock_products_view():
    db_connection = connect_db()
    if not db_connection:
        return

    try:
        schema_path = "../deliverables/database_schema/schema.sql"
        low_stock_view_path = "../deliverables/views/low_stock_products_view.sql"

        # Execute schema to ensure tables exist
        execute_sql_script(db_connection, schema_path)

        # Read the low_stock_products_view script content
        with open(low_stock_view_path, 'r') as f:
            low_stock_view_sql = f.read()

        cursor = execute_query(db_connection, low_stock_view_sql)
        cursor.close()
        db_connection.commit()
        print(f"Successfully executed SQL script: {low_stock_view_path}")

        cursor = db_connection.cursor()
        query = "SELECT product_name, stock_quantity, reorder_level FROM low_stock_products_view WHERE stock_quantity <= reorder_level LIMIT 1"
        cursor.execute(query)
        results = cursor.fetchall()
        print("\n--- Low Stock Products View Creation Test ---")
        print("Low Stock Products View created successfully and is queryable.")
        if results:
            print("Sample results:", results)
        else:
            print("No low stock products found (this is expected if all products have sufficient stock).")

    except Exception as e:
        print(f"An error occurred during low stock products view creation or querying: {e}")
    finally:
        close_connection(db_connection)

if __name__ == "__main__":
    test_low_stock_products_view()