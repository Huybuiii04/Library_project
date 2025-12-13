import mysql.connector
from mysql.connector import Error
import logging
import os
from pathlib import Path

# Define BASE_DIR - root project directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# MySQL configuration from environment variables or defaults
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'mysql'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'admin'),
    'password': os.getenv('MYSQL_PASSWORD', 'admin'),
    'database': os.getenv('MYSQL_DATABASE', 'project_db')
}

def get_mysql_connection():
    """Create and return a MySQL connection."""
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        if connection.is_connected():
            logging.info("Successfully connected to MySQL database")
            return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        raise
    return None

def create_books_table():
    """Create the books table if it doesn't exist."""
    connection = None
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS books (
            id INT AUTO_INCREMENT PRIMARY KEY,
            work_key VARCHAR(50) UNIQUE,
            title TEXT,
            author_name TEXT,
            author_key TEXT,
            first_publish_year INT,
            isbn TEXT,
            language TEXT,
            cover_id VARCHAR(50),
            unit_price DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_work_key (work_key),
            INDEX idx_author_name (author_name(255)),
            INDEX idx_first_publish_year (first_publish_year),
            INDEX idx_unit_price (unit_price)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        logging.info("Books table created or already exists")
        
    except Error as e:
        logging.error(f"Error creating table: {e}")
        raise
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def bulk_insert_books(books_data):
    """Bulk insert books into MySQL database."""
    if not books_data:
        return 0
    
    connection = None
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        
        insert_query = """
        INSERT INTO books (
            work_key, title, author_name, author_key, 
            first_publish_year, isbn, language, cover_id, unit_price
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            author_name = VALUES(author_name),
            first_publish_year = VALUES(first_publish_year),
            unit_price = VALUES(unit_price)
        """
        
        cursor.executemany(insert_query, books_data)
        connection.commit()
        
        rows_affected = cursor.rowcount
        logging.info(f"Successfully inserted/updated {rows_affected} books")
        return rows_affected
        
    except Error as e:
        logging.error(f"Error inserting books: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def get_books_count():
    """Get total count of books in database."""
    connection = None
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM books")
        count = cursor.fetchone()[0]
        return count
    except Error as e:
        logging.error(f"Error getting books count: {e}")
        return 0
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
