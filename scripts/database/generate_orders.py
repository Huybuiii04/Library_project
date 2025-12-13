# ==============================================
# SCRIPT: Generate data to MySQL database
# ==============================================
# Purpose:
#   - Simulate real-time coffee orders
# ==============================================

import random
import sys
import time
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager

import mysql.connector
from faker import Faker

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from scripts.utils import get_mysql_config

fake = Faker()

# Configuration to connect to MySQL database
MYSQL_CONFIG = get_mysql_config()


@contextmanager
def get_conn_cursor():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)
    try:
        yield conn, cursor
    finally:
        cursor.close()
        conn.close()


def get_books(cursor):
    """Get random books from database to use as products."""
    cursor.execute("""
        SELECT id, title, unit_price 
        FROM books 
        WHERE title IS NOT NULL AND unit_price IS NOT NULL
        LIMIT 1500
    """)
    return cursor.fetchall()


def create_order(cursor, order_id, timestamp, customer_id,  
                 store_id, payment_method_id, num_products):
    
    cursor.execute(
        """
        INSERT INTO orders (id, timestamp, customer_id, store_id, payment_method_id, num_products)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (order_id, timestamp, customer_id, store_id, payment_method_id, num_products)
    )


def prepare_order_items(books):
    """Prepare order items from available books."""
    # Random 1-5 items: 1 (50%), 2 (25%), 3 (15%), 4 (7%), 5 (3%)
    num_items = random.choices([1, 2, 3, 4, 5], weights=[0.50, 0.25, 0.15, 0.07, 0.03])[0]
    selected = random.sample(books, min(num_items, len(books)))
    order_items = []

    for book in selected:
        # Random quantity 1-5: 1 (60%), 2 (20%), 3 (12%), 4 (6%), 5 (2%)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[0.60, 0.20, 0.12, 0.06, 0.02])[0]
        subtotal = book["unit_price"] * quantity
        order_items.append((book["id"], quantity, subtotal))

    return order_items


def main():
    MAX_ORDERS = 25000
    
    with get_conn_cursor() as (conn, cursor):
        books = get_books(cursor)
        
        if not books:
            print("❌ No books found in database. Please run the Open Library crawler first.")
            return

        print(f"✅ Found {len(books)} books in database")
        print(f"🚀 Starting order generation... (Max: {MAX_ORDERS:,} orders)\n")

        order_count = 0
        while order_count < MAX_ORDERS:
            order_id = fake.uuid4()
            timestamp = datetime.now()
            customer_id = random.randint(1, 1000000)
            store_id = random.randint(1, 1000)
            payment_method_id = random.randint(1, 12)  # 12 payment methods in database
            order_items = prepare_order_items(books)
            num_products = len(order_items)

            try:
                create_order(cursor, order_id, timestamp, customer_id, store_id, payment_method_id, num_products)
                for product_id, quantity, subtotal in order_items:
                    cursor.execute(
                        "INSERT INTO order_details (order_id, product_id, quantity, subtotal, is_suggestion) VALUES (%s, %s, %s, %s, %s)",
                        (order_id, product_id, quantity, subtotal, False)
                    )
                conn.commit()
                order_count += 1
                
                # Progress indicator
                if order_count % 100 == 0:
                    progress = (order_count / MAX_ORDERS) * 100
                    print(f"[{order_count:,}/{MAX_ORDERS:,}] ({progress:.1f}%) ✓ Created order {order_id[:8]}... with {num_products} item(s)")
                elif order_count % 10 == 0:
                    print(f"[{order_count:,}] ✓ Created order {order_id[:8]}... with {num_products} item(s)")
            except Exception as insert_err:
                conn.rollback()
                print(f"❌ Failed to insert order {order_id}: {insert_err}")

            time.sleep(0.1)  # Generate 10 orders per second
        
        print(f"\n✅ Completed! Generated {order_count:,} orders successfully.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✋ Stopped order generation")
    except Exception as e:
        print(f"\n❌ Error: {e}")
