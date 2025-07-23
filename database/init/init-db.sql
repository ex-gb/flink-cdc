-- Create test table
CREATE TABLE public.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO public.users (name, email, age) VALUES 
('John Doe', 'john@example.com', 30),
('Jane Smith', 'jane@example.com', 25),
('Bob Johnson', 'bob@example.com', 35);

-- Create another test table for orders
CREATE TABLE public.orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES public.users(id),
    product_name VARCHAR(200) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample orders
INSERT INTO public.orders (user_id, product_name, quantity, price) VALUES
(1, 'Laptop', 1, 999.99),
(2, 'Mouse', 2, 29.99),
(1, 'Keyboard', 1, 79.99);

-- Grant necessary permissions for CDC
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;

-- Create publication for logical replication
CREATE PUBLICATION cdc_publication FOR ALL TABLES; 