import os
from utility import *
from dotenv import load_dotenv

load_dotenv()

def test_replenish_stock(product_id, quantity, remarks="Manual Restock"):
    db_connection = connect_db()
    if not db_connection:
        return

    try:
        cursor = db_connection.cursor()

        # Call the replenish_stock stored procedure
        call_procedure_query = "CALL replenish_stock(%s, %s, %s)"
        cursor.execute(call_procedure_query, (product_id, quantity, remarks))

        db_connection.commit()
        print(f"Successfully called replenish_stock procedure for product '{product_id}'. Added {quantity} units.")

    except Exception as e:
        db_connection.rollback()
        print(f"An error occurred during stock replenishment: {e}")
    finally:
        close_connection(db_connection)

if __name__ == "__main__":
    # Example usage: Replace with the actual product ID and quantity you want to replenish
    product_to_replenish = 1  # Assuming product_id in the procedure is INT
    replenishment_quantity = 50
    replenishment_remarks = "Received new shipment"

    test_replenish_stock(product_to_replenish, replenishment_quantity, replenishment_remarks)