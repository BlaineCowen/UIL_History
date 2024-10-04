import sqlite3

# Connect to the database
conn = sqlite3.connect("uil.db")

# Create a cursor
cur = conn.cursor()

# Run the VACUUM command
cur.execute("VACUUM")

# Close the connection
conn.close()
