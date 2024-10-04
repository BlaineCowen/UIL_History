import pandas as pd
import sqlite3

old_db = sqlite3.connect('uil_old.db')
new_db = sqlite3.connect('uil.db')

old_df = pd.read_sql_query("SELECT * FROM results", old_db)
new_df = pd.read_sql_query("SELECT * FROM results", new_db)

# find columns difference
old_columns = old_df.columns
new_columns = new_df.columns
print("Columns difference:")
print("Old columns:", old_columns)
print("New columns:", new_columns)
print("Columns in old but not in new:", set(old_columns) - set(new_columns))
print("Columns in new but not in old:", set(new_columns) - set(old_columns))



