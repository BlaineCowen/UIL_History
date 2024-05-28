import pandas as pd
import sqlite3

# Load the data
conn = sqlite3.connect("uil.db")
df = pd.read_sql_query("SELECT * FROM results", conn)

# drop table "events"
conn.execute("DROP TABLE IF EXISTS events")

# Fix the data
# erase the re ###-
# erase the re ###-
df["event"] = df["event"].str.replace(r"\d{3}-", "", regex=True)
# change to lowercase and erase space
df["event"] = df["event"].str.lower().str.replace(" ", "")

# put a space before chorus, ochrestra or band
df["event"] = df["event"].str.replace("chorus", " chorus")
df["event"] = df["event"].str.replace("orchestra", " orchestra")
df["event"] = df["event"].str.replace("band", " band")


# Save the data
df.to_sql("results", conn, if_exists="replace", index=False)
conn.close()
