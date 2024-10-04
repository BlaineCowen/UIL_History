import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect("uil_backup.db")

results = pd.read_sql_query("SELECT * FROM results", conn)
pml = pd.read_sql_query("SELECT * FROM pml", conn)

conn.close()

# Connect to the database
conn = sqlite3.connect("uil.db")

# add results to uil.db
results.to_sql("results", conn, if_exists="replace", index=False)


# add pml to uil.db
pml.to_sql("pml", conn, if_exists="replace", index=False)

# Close the connection
conn.close()
