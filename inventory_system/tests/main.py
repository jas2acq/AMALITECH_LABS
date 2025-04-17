import os
from utility import *
from dotenv import load_dotenv
#from tests.test_schema import test_create_schema
from test_stock_insights_report import test_stock_insights_report
from test_place_order import test_place_order
from test_categorize_customers import test_categorize_customers
from test_replenish_stock import test_replenish_stock
# from test_order_summary_view import test_order_summary_view
# from test_low_stock_products_view import test_low_stock_products_view

load_dotenv()

def main():
    print("Inventory and Order Management Automation Script")
    print("---------------------------------------------")

    #print("\n--- Phase 1: Database Schema Implementation ---")
    #test_create_schema()

    print("\n--- Phase 3: Monitoring and Reporting ---")
    test_stock_insights_report()

    print("\n--- Phase 2: Order Placement and Inventory Management ---")
    test_place_order()

    print("\n--- Phase 3: Customer Categorization ---")
    test_categorize_customers()

    print("\n--- Phase 4: Stock Replenishment ---")
    # Example of replenishing stock for a specific product
    product_to_replenish = 1
    replenishment_quantity = 25
    replenishment_remarks = "Scheduled Restock"
    test_replenish_stock(product_to_replenish, replenishment_quantity, replenishment_remarks)

    # print("\n--- Phase 5: Creating Views ---")
    # test_order_summary_view()
    # test_low_stock_products_view()

    print("\n--- Automation Script Completed ---")

if __name__ == "__main__":
    main()