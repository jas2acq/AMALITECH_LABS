# Inventory and Order Management System

## Overview

This project implements a database-driven system to manage inventory and orders for an e-commerce company. It efficiently handles product information, customer data, and order processing while ensuring that stock levels are properly updated when orders are placed. The system also tracks all inventory changes and streamlines processes related to stock replenishment.

The goal is to provide insights into customer purchase behaviors, monitor stock levels, and ensure that products are always available for sale, minimizing manual work and ensuring accuracy in inventory tracking and order management.

## Problem Statement

This system addresses the need for an efficient way to manage inventory and orders for an e-commerce company. It aims to solve challenges related to tracking product information, managing customer data, processing orders, updating stock levels, and monitoring inventory changes. The system also provides insights into customer spending habits and helps streamline stock replenishment processes.

## Key Features

* **Database Schema:** A well-designed database schema to store information about products, customers, orders, order details, and inventory logs.
* **Order Placement:** Efficient processing of new customer orders, including deducting stock and calculating total amounts.
* **Inventory Management:** Real-time tracking of stock levels and automatic updates upon order placement.
* **Inventory Logging:** Comprehensive logging of all inventory changes (orders, restocks, adjustments) for auditing.
* **Stock Replenishment:** Mechanisms to identify low stock products and facilitate the replenishment process.
* **Business Insights:** Generation of summaries and reports on orders and stock levels.
* **Customer Insights:** Categorization of customers based on spending habits and implementation of bulk discounts.
* **Automated Processes:** Automation of key tasks like stock updates, order total calculations, and customer categorization.
* **Data Access Simplification:** Use of database views to provide simplified access to summarized order and stock information.

## Getting Started

### Prerequisites

* **MySQL Server:** You need a running MySQL server instance to host the database.
* **Python 3:** Python 3 is required to run any potential scripts interacting with the database.
* **MySQL Connector/Python:** This library is used to connect Python applications to the MySQL database.

### Installation

1.  **Clone the repository** (if applicable) or create the project directory.
2.  **Install Dependencies:** Navigate to the project directory in your terminal and install the required Python packages using pip:

    ```bash
    pip install -r requirements.txt
    ```

    The `requirements.txt` file for this project includes:

    ```txt
    dotenv==0.9.9
    mysql-connector-python==9.3.0
    mysqlclient==2.2.7
    python-dotenv==1.1.0
    ```

    While `mysqlclient` is listed, the code primarily uses `mysql-connector-python`.

3.  **Database Setup:**
    * Connect to your MySQL server using a MySQL client.
    * Create a new database for the inventory system (if you haven't already).
    * Execute the SQL scripts located in the `deliverables/database_schema` directory (specifically `schema.sql`) to create the necessary tables and define the schema.
