import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()

def connect_db():
    """Connects to the MySQL database using environment variables."""
    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_DATABASE")
        )
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None

def execute_sql_script(connection, script_path):
    """Executes a SQL script from the given file path, handling delimiters."""
    if not connection:
        print("No database connection available.")
        return

    cursor = connection.cursor()
    try:
        with open(script_path, 'r') as f:
            sql_content = f.read()

        statements = []
        current_statement = ""
        delimiter = ";"

        for line in sql_content.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.upper().startswith("DELIMITER"):
                parts = line.split()
                if len(parts) > 1:
                    delimiter = parts[1]
                continue
            current_statement += line + "\n"
            if current_statement.strip().endswith(delimiter):
                statements.append(current_statement.strip()[:-len(delimiter)].strip())
                current_statement = ""

        if current_statement.strip():  # Handle any remaining statement without a delimiter
            statements.append(current_statement.strip())

        for statement in statements:
            if statement:
                cursor.execute(statement)
                if cursor.with_rows:
                    cursor.fetchall()

        connection.commit()
        print(f"Successfully executed SQL script: {script_path}")

    except FileNotFoundError:
        print(f"Error: SQL script not found at {script_path}")
        raise
    except mysql.connector.Error as err:
        print(f"MySQL error during script execution ({script_path}): {err}")
        connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def fetch_one(cursor):
    """Fetches one result from the cursor."""
    return cursor.fetchone()

def fetch_all(cursor):
    """Fetches all results from the cursor."""
    return cursor.fetchall()

def execute_query(connection, query, params=None):
    """Executes a SQL query and returns the cursor."""
    cursor = connection.cursor()
    cursor.execute(query, params)
    return cursor

def get_last_insert_id(cursor):
    """Returns the ID of the last inserted row."""
    return cursor.lastrowid

def close_connection(connection):
    """Closes the database connection."""
    if connection and connection.is_connected():
        connection.cursor().close()
        connection.close()
        print("MySQL connection is closed.")