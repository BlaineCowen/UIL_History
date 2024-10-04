import pandas as pd
import sqlite3

pml_csv = pd.read_csv("pml.csv")

conn = sqlite3.connect("uil.db")
df_to_fix = pd.read_sql_query("SELECT * FROM pml", conn)

# makesure all codes are strings
df_to_fix["code"] = df_to_fix["code"].astype(str)
pml_csv["code"] = pml_csv["code"].astype(str)

# if code ends in .0, remove the .0
df_to_fix["code"] = df_to_fix["code"].str.replace(r"\.0$", "", regex=True)
pml_csv["code"] = pml_csv["code"].str.replace(r"\.0$", "", regex=True)

# only use event_name, title, composer, arranger, specification on pml.csv
pml_csv = pml_csv[["code", "title", "composer", "arranger", "specification"]]

# replace all the columns with the new data
df_to_fix = df_to_fix.drop(columns=["title", "composer", "arranger", "specification"])
df_to_fix = df_to_fix.merge(pml_csv, on="code", how="left")


# update the database
df_to_fix.to_sql("pml", conn, if_exists="replace", index=False)
conn.close()
