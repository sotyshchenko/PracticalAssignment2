import mysql.connector
import uuid
from faker import Faker
from dotenv import load_dotenv
import random
from datetime import datetime, timedelta
import os

# Load environment variables
load_dotenv()

# Connection settings
HOST = os.getenv('host')
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DATABASE = os.getenv('database')


# Connect to the MySQL database
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='GoodOmens2018',
    database='opt_db'
)

cursor = connection.cursor()
fake = Faker()

# Insert 10000 rows into opt_clients
print("Inserting into opt_clients...")
client_insert_query = """
    INSERT INTO opt_clients (id, name, surname, email, phone, address, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
clients_data = [
    (str(uuid.uuid4()), fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(), fake.address(), random.choice(['active', 'inactive']))
    for _ in range(10000)
]
cursor.executemany(client_insert_query, clients_data)
connection.commit()
print("Inserted into opt_clients.")

# Insert 20000 rows into opt_products
print("Inserting into opt_products...")
product_insert_query = """
   INSERT INTO opt_products (product_name, product_category, description, product_price)
    VALUES (%s, %s, %s, %s)
"""
categories = ['Category1', 'Category2', 'Category3', 'Category4', 'Category5']
products_data = [
    (fake.word(), random.choice(categories), fake.text(), round(random.uniform(5.00, 500.00), 2))
    for _ in range(20000)
]
cursor.executemany(product_insert_query, products_data)
connection.commit()
print("Inserted into opt_products.")

# Insert 20000 rows into opt_orders
print("Inserting into opt_orders...")
order_insert_query = """
    INSERT INTO opt_orders (order_date, client_id, product_id)
    VALUES (%s, %s, %s)
"""
order_date_start = datetime.now() - timedelta(days=365 * 5)
orders_data = [
    (order_date_start + timedelta(days=random.randint(0, 365 * 5)), random.choice(clients_data)[0], random.randint(1, 1000))
    for _ in range(20000)
]
# Use chunks to avoid memory issues
chunk_size = 10000
for i in range(0, len(orders_data), chunk_size):
    cursor.executemany(order_insert_query, orders_data[i:i + chunk_size])
    connection.commit()
    print(f"Inserted {i + chunk_size} rows into opt_orders...")

print("Inserted into opt_orders.")

#Close the cursor and connection
cursor.close()
connection.close()
