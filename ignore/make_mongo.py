import sqlite3
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import AutoReconnect
from pymongo.server_api import ServerApi
import time

# SQLite database file
sqlite_db = "uil.db"

# MongoDB connection URI
uri = "mongodb+srv://blainecowen:DKlhhcyhp9LwI5Ap@cluster0.lmx49.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(
    uri, server_api=ServerApi("1"), connectTimeoutMS=30000, socketTimeoutMS=30000
)

# Connect to SQLite
sqlite_conn = sqlite3.connect(sqlite_db)

# List of tables to migrate
tables = ["results"]  # Replace with your actual table names
for table in tables:
    # Read data from SQLite table into a DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {table}", sqlite_conn)

    # Convert DataFrame to a list of dictionaries
    data = df.to_dict(orient="records")

    # Insert data into MongoDB with retry logic
    db = client["uil_history"]  # Replace with your MongoDB database name
    collection = db[table]

    retries = 5
    chunk_size = 1000  # Adjust chunk size as needed
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        for attempt in range(retries):
            try:
                collection.insert_many(chunk)
                print(f"Data chunk from {table} inserted successfully.")
                break
            except AutoReconnect as e:
                print(f"AutoReconnect error: {e}. Retrying {attempt + 1}/{retries}...")
                time.sleep(5)  # Wait for 5 seconds before retrying
        else:
            print(f"Failed to insert data chunk from {table} after {retries} attempts.")

# Close SQLite connection
sqlite_conn.close()

print("Data migration completed.")
