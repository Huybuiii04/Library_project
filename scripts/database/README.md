# Database Setup Guide

## Prerequisites
```bash
pip install mysql-connector-python faker
```

## Setup Steps

### 1. Start MySQL container
```cmd
docker compose up -d mysql
```

### 2. Wait for MySQL to be ready (check logs)
```cmd
docker logs mysql-db
```

### 3. Load CSV data into MySQL
```cmd
python scripts\database\load_file.py
```

This will load:
- `data/customers.csv` → `customers` table (1M rows)
- `data/payment_method.csv` → `payment_method` table (13 rows)

### 4. Generate real-time orders (optional)
```cmd
python scripts\database\generate_orders.py
```

This will continuously generate orders with order details.
Press `Ctrl+C` to stop.

## Database Schema

```
CUSTOMERS                    ORDERS                     PAYMENT_METHOD
├─ id (PK)                   ├─ id (PK)                 ├─ id (PK)
├─ name                      ├─ timestamp               ├─ method_name
├─ phone_number              ├─ customer_id (FK) ──→ CUSTOMERS.id
├─ tier                      ├─ store_id                ├─ bank
└─ updated_at                ├─ payment_method_id (FK)─→ PAYMENT_METHOD.id
                             └─ num_products
                                      │
                                      ↓
                             ORDER_DETAILS
                             ├─ id (PK)
                             ├─ order_id (FK) ──→ ORDERS.id
                             ├─ product_id
                             ├─ quantity
                             ├─ subtotal
                             └─ is_suggestion
```

## Verify Data

```sql
-- Check loaded data
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM payment_method;

-- Check generated orders
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM order_details;

-- Sample query
SELECT o.id, o.timestamp, c.name, pm.method_name, o.num_products
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN payment_method pm ON o.payment_method_id = pm.id
LIMIT 10;
```
