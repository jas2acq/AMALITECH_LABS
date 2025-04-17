import os
from utility import *
from dotenv import load_dotenv

def test_order_summary_report():
    db_connection = connect_db()
    if not db_connection:
        return

    try:
        schema_path = r"AMALITECH_LABS\inventory_system\deliverables\database_schema\schema.sql"
        report_script_path = r"AMALITECH_LABS\inventory_system\deliverables\report_summaries\order_summary_report.sql"

        # Execute schema to ensure tables exist
        execute_sql_script(db_connection, schema_path)

        # Execute the order_summary_report script
        execute_sql_script(db_connection, report_script_path)

        print("\n--- Order Summary Report Execution Test ---")
        print("Order Summary Report script executed successfully.")

    except Exception as e:
        print(f"An error occurred during order summary report execution: {e}")
    finally:
        close_connection(db_connection)

if __name__ == "__main__":
    test_order_summary_report()