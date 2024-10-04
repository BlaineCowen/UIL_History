import pandas as pd
import sqlite3

old_db = sqlite3.connect('uil_old.db')
new_db = sqlite3.connect('uil.db')

old_df = pd.read_sql_query("SELECT * FROM results", old_db)
new_df = pd.read_sql_query("SELECT * FROM results", new_db)

# combine the two dataframes
combined_df = pd.concat([old_df, new_df], ignore_index=True)

combined_df.fillna('N/A', inplace=True)

# save the combined dataframe to a new database
combined_db = sqlite3.connect('combined.db')
combined_df.to_sql('results', combined_db, index=False)

new_pml = pd.read_sql_query("SELECT * FROM pml", new_db)

# save the new pml to the combined database
new_pml.to_sql('pml', combined_db, index=False)

