-- MySQL CDC Initialization Script
-- Create test tables and configure CDC user

-- Create test table for users
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO users (name, email, age) VALUES 
('John Doe', 'john@example.com', 30),
('Jane Smith', 'jane@example.com', 25),
('Bob Johnson', 'bob@example.com', 35);

-- Create another test table for orders
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INTEGER,
    product_name VARCHAR(200) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Insert sample orders
INSERT INTO orders (user_id, product_name, quantity, price) VALUES
(1, 'Laptop', 1, 999.99),
(2, 'Mouse', 2, 29.99),
(1, 'Keyboard', 1, 79.99);

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(100),
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert sample products
INSERT INTO products (name, description, price, category, stock_quantity) VALUES
('Laptop Pro', 'High-performance laptop', 1299.99, 'Electronics', 50),
('Wireless Mouse', 'Ergonomic wireless mouse', 29.99, 'Electronics', 200),
('Mechanical Keyboard', 'RGB mechanical keyboard', 79.99, 'Electronics', 100);

-- Grant CDC permissions to the user
-- The cdc_user was created during container initialization
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';

-- Grant specific permissions on the cdc_source database
GRANT ALL PRIVILEGES ON cdc_source.* TO 'cdc_user'@'%';

-- Flush privileges
FLUSH PRIVILEGES;

-- Show binary log status (for debugging)
SHOW MASTER STATUS; 