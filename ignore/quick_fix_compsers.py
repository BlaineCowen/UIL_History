import sqlite3
import pandas as pd
import sqlite3
import pandas as pd


conn = sqlite3.connect("uil_backup.db")
backup_df = pd.read_sql_query("SELECT * FROM results", conn)

cols_to_keep = [
    "entry_number",
    "composer_1",
    "composer_2",
    "composer_3",
]

backup_df = backup_df[cols_to_keep]

conn.close()


conn = sqlite3.connect("uil.db")
df_to_fix = pd.read_sql_query("SELECT * FROM results", conn)
conn.close()

df_to_fix = df_to_fix.merge(backup_df, on="entry_number", how="left")
df_to_fix["composer_1"] = df_to_fix["composer_1_y"]

df_to_fix["composer_2"] = df_to_fix["composer_2_y"]

df_to_fix["composer_3"] = df_to_fix["composer_3_y"]

df_to_fix.drop(
    [
        "composer_1_x",
        "composer_1_y",
        "composer_2_x",
        "composer_2_y",
        "composer_3_x",
        "composer_3_y",
    ],
    axis=1,
    inplace=True,
)

# erase codes
df_to_fix["code_1"] = ""
df_to_fix["code_2"] = ""
df_to_fix["code_3"] = ""


# update the database
conn = sqlite3.connect("uil.db")
df_to_fix.to_sql("results", conn, if_exists="replace", index=False)

# check if unique_entries is in db, delete if it is
try:
    conn.execute("DROP TABLE unique_entries")
except:
    pass
conn.close()
