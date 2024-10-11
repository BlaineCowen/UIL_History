from rapidfuzz import process, fuzz
import pandas as pd
import sqlite3
from tqdm import tqdm

conn = sqlite3.connect("uil.db")
grouped_titles = pd.read_sql_query("SELECT * FROM arranger_grouped_entries", conn)

# Split the dataframe into two parts: one with missing codes and one with existing codes
df_missing = grouped_titles[grouped_titles["code"] == ""]
df_existing = grouped_titles[grouped_titles["code"] != ""]

# Concatenate the title and composer columns for better matching in the existing dataframe
df_existing["combined"] = df_existing["title"] + " " + df_existing["composer"]

# Optional: reset index of df_existing to ensure consistent indexing
df_existing = df_existing.reset_index(drop=True)


# Function to find the closest match for a missing entry
def find_closest_match(row, df_existing):
    row_combined = f"{row['title']} {row['composer']}"
    min_score = 84

    # Use process.extractOne to find the closest match
    match, score, index = process.extractOne(
        query=row_combined, choices=df_existing["combined"], scorer=fuzz.ratio
    )

    # Check if a valid index is found and score is above the threshold
    if index is not None and score >= min_score:
        return df_existing.iloc[index]["code"]
    return "none"


# Apply the closest match function to the missing codes DataFrame with a progress bar
tqdm.pandas()  # Enables the progress bar for pandas apply
df_missing["code"] = df_missing.progress_apply(
    lambda row: find_closest_match(row, df_existing), axis=1
)

# Combine the updated missing codes with the original existing codes DataFrame
grouped_titles_updated = pd.concat([df_existing, df_missing])

# Save the updated DataFrame back to the database
grouped_titles_updated.to_sql("grouped_entries", conn, if_exists="replace", index=False)

conn.close()
