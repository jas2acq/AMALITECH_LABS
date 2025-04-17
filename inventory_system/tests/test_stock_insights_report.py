import os
from utility import *
from dotenv import load_dotenv

def test_stock_insights_report():
    db_connection = connect_db()
    if not db_connection:
        return

    try:
        schema_path = "../deliverables/database_schema/schema.sql"
        report_script_path = "../deliverables/report_summaries/stock_insights_report.sql"

        # Execute schema to ensure tables exist
        execute_sql_script(db_connection, schema_path)

        # Read the stock_insights_report script content
        with open(report_script_path, 'r') as f:
            report_sql = f.read()

        cursor = execute_query(db_connection, report_sql)
        results = cursor.fetchall()  # Fetch the results here
        cursor.close()
        db_connection.commit()
        print(f"Successfully executed SQL script: {report_script_path}")

        print("\n--- Stock Insights Report Execution Test ---")
        print("Stock Insights Report script executed successfully.")


    except Exception as e:
        print(f"An error occurred during stock insights report execution: {e}")
    finally:
        close_connection(db_connection)

if __name__ == "__main__":
    test_stock_insights_report()