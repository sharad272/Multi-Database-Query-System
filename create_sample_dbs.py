import sqlite3
import os

# Create databases directory if it doesn't exist
os.makedirs('databases', exist_ok=True)

# Create sales database
sales_db_path = 'databases/sales.db'
sales_conn = sqlite3.connect(sales_db_path)
sales_cursor = sales_conn.cursor()

# Create sales tables
sales_cursor.execute('''
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date TEXT,
    total_amount REAL
)
''')

sales_cursor.execute('''
CREATE TABLE IF NOT EXISTS order_items (
    item_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price REAL,
    FOREIGN KEY (order_id) REFERENCES orders (order_id)
)
''')

# Add sample data
sales_cursor.execute("INSERT INTO orders VALUES (1, 101, '2023-10-01', 150.50)")
sales_cursor.execute("INSERT INTO orders VALUES (2, 102, '2023-10-02', 75.25)")
sales_cursor.execute("INSERT INTO orders VALUES (3, 101, '2023-10-03', 200.00)")

sales_cursor.execute("INSERT INTO order_items VALUES (1, 1, 1001, 2, 75.25)")
sales_cursor.execute("INSERT INTO order_items VALUES (2, 2, 1002, 1, 75.25)")
sales_cursor.execute("INSERT INTO order_items VALUES (3, 3, 1001, 1, 75.25)")
sales_cursor.execute("INSERT INTO order_items VALUES (4, 3, 1003, 1, 124.75)")

sales_conn.commit()
sales_conn.close()
print(f"Created sales database at {sales_db_path}")

# Create customers database
customers_db_path = 'databases/customers.db'
customers_conn = sqlite3.connect(customers_db_path)
customers_cursor = customers_conn.cursor()

# Create customers tables
customers_cursor.execute('''
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT
)
''')

# Add sample data
customers_cursor.execute("INSERT INTO customers VALUES (101, 'John', 'Doe', 'john.doe@example.com', '555-1234', '123 Main St', 'New York', 'NY', '10001')")
customers_cursor.execute("INSERT INTO customers VALUES (102, 'Jane', 'Smith', 'jane.smith@example.com', '555-5678', '456 Park Ave', 'Boston', 'MA', '02108')")
customers_cursor.execute("INSERT INTO customers VALUES (103, 'Bob', 'Johnson', 'bob.johnson@example.com', '555-9012', '789 Broadway', 'Chicago', 'IL', '60601')")

customers_conn.commit()
customers_conn.close()
print(f"Created customers database at {customers_db_path}")

# Create inventory database
inventory_db_path = 'databases/inventory.db'
inventory_conn = sqlite3.connect(inventory_db_path)
inventory_cursor = inventory_conn.cursor()

# Create inventory tables
inventory_cursor.execute('''
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    name TEXT,
    description TEXT,
    price REAL,
    category TEXT
)
''')

inventory_cursor.execute('''
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id INTEGER PRIMARY KEY,
    product_id INTEGER,
    quantity INTEGER,
    location TEXT,
    FOREIGN KEY (product_id) REFERENCES products (product_id)
)
''')

# Add sample data
inventory_cursor.execute("INSERT INTO products VALUES (1001, 'Laptop', 'High-end laptop', 999.99, 'Electronics')")
inventory_cursor.execute("INSERT INTO products VALUES (1002, 'Smartphone', 'Latest model', 599.99, 'Electronics')")
inventory_cursor.execute("INSERT INTO products VALUES (1003, 'Headphones', 'Noise-cancelling', 199.99, 'Audio')")

inventory_cursor.execute("INSERT INTO inventory VALUES (1, 1001, 50, 'Warehouse A')")
inventory_cursor.execute("INSERT INTO inventory VALUES (2, 1002, 100, 'Warehouse B')")
inventory_cursor.execute("INSERT INTO inventory VALUES (3, 1003, 75, 'Warehouse A')")

inventory_conn.commit()
inventory_conn.close()
print(f"Created inventory database at {inventory_db_path}")

print("All sample databases created successfully!") 
 