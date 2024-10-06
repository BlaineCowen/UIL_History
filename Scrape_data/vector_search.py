import pandas as pd
import sqlite3

conn = sqlite3.connect("uil.db")
unique_df = pd.read_sql_query("SELECT * FROM unique_entries", conn)
results_df = pd.read_sql_query("SELECT * FROM results", conn)


conn.close()
