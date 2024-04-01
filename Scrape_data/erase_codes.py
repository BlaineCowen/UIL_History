import sqlite3
import pandas as pd

conn = sqlite3.connect("uil.db")
df = pd.read_sql_query("SELECT * FROM results", conn)

df["code_1"] = None
df["code_2"] = None
df["code_3"] = None

# update the database
df.to_sql("results", conn, if_exists="replace", index=False)
conn.close()
