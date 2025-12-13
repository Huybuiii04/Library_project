-- Sample initialization script for MySQL

-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    phone_number VARCHAR(50),
    tier VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_tier (tier)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Payment methods lookup table
CREATE TABLE IF NOT EXISTS payment_method (
    id INT PRIMARY KEY,
    method_name VARCHAR(100) NOT NULL,
    bank VARCHAR(100),
    INDEX idx_method_name (method_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(36) PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    customer_id INT NOT NULL,
    store_id INT,
    payment_method_id INT,
    num_products INT,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    FOREIGN KEY (payment_method_id) REFERENCES payment_method(id) ON DELETE SET NULL,
    INDEX idx_customer_id (customer_id),
    INDEX idx_timestamp (timestamp),
    INDEX idx_payment_method_id (payment_method_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Order details table
CREATE TABLE IF NOT EXISTS order_details (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_id VARCHAR(36) NOT NULL,
    product_id INT,
    quantity INT NOT NULL,
    subtotal DECIMAL(10, 2),
    is_suggestion BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    INDEX idx_order_id (order_id),
    INDEX idx_product_id (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



