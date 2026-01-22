import sqlite3

# Create SQLite database
conn = sqlite3.connect("orders.db")
cursor = conn.cursor()

# Create orders table
cursor.execute("""
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    amount REAL
)
""")

# Insert sample data
# Customer 1 buys twice, customer 2 buys once, customer 3 buys nothing
data = [
    (101, 1, 500.0),
    (102, 1, 300.0),
    (103, 2, 1200.0),
    (104, 4, 150.0)
]

cursor.executemany(
    "INSERT INTO orders VALUES (?, ?, ?)",
    data
)

conn.commit()
conn.close()

print("Database orders.db created successfully!")
