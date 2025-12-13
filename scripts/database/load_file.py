# ==================================================================================
# SCRIPT: Load CSV Files into MySQL Source Tables
# ==================================================================================
# Purpose:
#   - Load structured data from CSV files into MySQL source tables.
#   - These tables serve as dimension or lookup tables for the data pipeline.
#
# Source:
#   - CSV files located in the 'data/' directory:
#     * customers.csv
#     * payment_method.csv
#
# Target MySQL Tables: customers, payment_methods
#
# Notes:
#   - Make sure the MySQL server is running and accessible.
#   - Ensure table schemas in MySQL match the structure of the CSV files.
#   - Enable 'local_infile' on both server and client sides.
# ==================================================================================

import sys
from pathlib import Path

import mysql.connector
from mysql.connector import errorcode

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from scripts.utils import get_mysql_config


def connect_database(user, password, host, database, port=3306, allow_local_infile=True):
    """Connect to MySQL database."""
    try:
        conn = mysql.connector.connect(
            user=user,
            password=password,
            host=host,
            database=database,
            port=port,
            allow_local_infile=allow_local_infile
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username and password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(err)
        sys.exit(1)
    else:
        print("Connect to database successfully!")
        return conn
    
def load_data_to_table(cursor, table_name, csv_path, columns=None):
    """Load CSV files to tables.
    
    Args:
        cursor: MySQL cursor
        table_name: Target table name
        csv_path: Path to CSV file
        columns: Optional list of column names (in CSV order). If None, assumes CSV header matches table columns exactly.
    """
    csv_path_str = csv_path.replace("\\", "/")

    if columns:
        # Specify columns explicitly for safety
        columns_str = "(" + ", ".join(columns) + ")"
        load_query = f"""
        LOAD DATA LOCAL INFILE '{csv_path_str}'
        INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS
        {columns_str};
        """
    else:
        # Assume CSV columns match table columns exactly (risky if order differs)
        load_query = f"""
        LOAD DATA LOCAL INFILE '{csv_path_str}'
        INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS;
        """

    try:
        cursor.execute(load_query)
    except mysql.connector.Error as err:
        print(f"Error loading data to {table_name}: {err.msg}")
    else:
        print(f"Data loaded into {table_name} from {csv_path}")

def main():
    # Configuration to connect to MySQL database
    mysql_config = get_mysql_config()

    conn = connect_database(**mysql_config)
    cursor = conn.cursor()

    # Load customers.csv: id,name,phone_number,tier,updated_at -> customers table
    # Column names match exactly, but specify them explicitly for clarity
    print("Loading customers data...")
    load_data_to_table(
        cursor, 
        'customers', 
        str(BASE_DIR / 'data' / 'customers.csv'),
        columns=['id', 'name', 'phone_number', 'tier', 'updated_at']
    )
    conn.commit()
    print(f"✓ Loaded customers data")
    
    # Load payment_method.csv: id,method_name,bank -> payment_method table
    # Column names match exactly, but specify them explicitly for clarity
    print("\nLoading payment methods data...")
    load_data_to_table(
        cursor, 
        'payment_method', 
        str(BASE_DIR / 'data' / 'payment_method.csv'),
        columns=['id', 'method_name', 'bank']
    )
    conn.commit()
    print(f"✓ Loaded payment methods data")

    print("\n✅ All data loaded successfully!")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()