import pandas as pd
import sqlite3

results_df = pd.read_sql_query("SELECT * FROM results", sqlite3.connect("combined.db"))

pml_df = pd.read_sql_query("SELECT * FROM pml", sqlite3.connect("combined.db"))

# match results code_1 with pml code, show results.event and pml.event_name
results_df["code_2"] = results_df["code_2"].str.replace(" ", "")
pml_df["code"] = pml_df["code"].str.replace(" ", "")

merged_df = pd.merge(results_df, pml_df, left_on="code_2", right_on="code", how="left")
# only get where code is not none
merged_df = merged_df[merged_df["code"].notnull()]
merged_df = merged_df[merged_df["code"].str.lower() != "none"]


# make sure both events are *band* events or both chorus or both orchestra
error_df = merged_df[
    merged_df["event"].str.contains("chorus")
    != (
        merged_df["event_name"].str.lower().str.contains("chorus")
        | merged_df["event_name"].str.lower().str.contains("madrigal")
    )
]

print(error_df[["event", "event_name"]].head(50))
