import pandas as pd
from pymongo import MongoClient
import streamlit as st
import sqlite3


# MongoDB connection URI
uri = st.secrets["mongo_uri"]

#  connect to local db
sqlite_db = "uil.db"
sqlite_conn = sqlite3.connect(sqlite_db)
df = pd.read_sql_query("SELECT * FROM results", sqlite_conn)


# get rid of the 0:00:00 in the date
def fix_date(date):
    try:
        new_date = date.split(" ")[0]
    except:
        new_date = date

    return new_date


df["contest_date"] = df["contest_date"].apply(fix_date)

# replace the old table in db
sqlite_conn.execute("DROP TABLE results")
df.to_sql("results", sqlite_conn, index=False)

# close connection
