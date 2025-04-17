import os
from utility import *
from dotenv import load_dotenv

def test_create_schema():
    db_connection = connect_db()
    if not db_connection:
        return

    try:
        schema_path = "../deliverables/database_schema/schema.sql"
        execute_sql_script(db_connection, schema_path)
        print("\n--- Schema Creation Test ---")
        print("Database schema created successfully.")

    except Exception as e:
        print(f"An error occurred during schema creation: {e}")
    finally:
        close_connection(db_connection)

if __name__ == "__main__":
    test_create_schema()