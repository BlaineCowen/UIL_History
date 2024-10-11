import pandas as pd
import sqlite3
from fuzzywuzzy import fuzz

start_time = pd.Timestamp.now()
conn = sqlite3.connect("uil.db")

results_df = pd.read_sql_query("SELECT * FROM results", conn)

conn.close()

# fill na in all title, composer, arranger, and event columns
results_df["title_1"] = results_df["title_1"].fillna("")
results_df["title_2"] = results_df["title_2"].fillna("")
results_df["title_3"] = results_df["title_3"].fillna("")
results_df["composer_1"] = results_df["composer_1"].fillna("")
results_df["composer_2"] = results_df["composer_2"].fillna("")
results_df["composer_3"] = results_df["composer_3"].fillna("")
results_df["event"] = results_df["event"].fillna("")

for i in range(1, 4):
    # make song simple lower
    results_df[f"title_simple_{i}"] = results_df[f"title_{i}"].str.lower()

    # remove anything in parenthesis
    results_df[f"title_simple_{i}"] = results_df[f"title_simple_{i}"].str.replace(
        r"\(.*?\)", "", regex=True
    )
    # remove anything not a letter
    results_df[f"title_simple_{i}"] = results_df[f"title_simple_{i}"].str.replace(
        r"[^a-zA-Z]", "", regex=True
    )
    # get rid of spaces
    results_df[f"title_simple_{i}"] = results_df[f"title_simple_{i}"].str.replace(
        " ", ""
    )
    # if from is in the title, remove from and anything after
    results_df[f"title_simple_{i}"] = (
        results_df[f"title_simple_{i}"].str.split("from").str[0]
    )

    # erase anything inside of parenthesis and the parenthesis
    results_df[f"composer_{i}"] = (
        results_df[f"composer_{i}"]
        .str.lower()
        .str.replace(r"\(.*?\)", "", regex=True)
        .str.replace("  ", " ", regex=False)
        .str.replace("comp:", "", regex=False)
        .str.replace("arr:", "", regex=False)
    )

    results_df[f"composer_{i}"] = results_df[f"composer_{i}"].str.strip()

    # erase annything containing arr and anything after
    results_df[f"composer_match_{i}"] = (
        results_df[f"composer_{i}"].str.split(" arr ").str[0]
    )
    results_df[f"composer_match_{i}"] = (
        results_df[f"composer_{i}"].str.split("arr.").str[0]
    )
    results_df[f"composer_match_{i}"] = (
        results_df[f"composer_{i}"].str.split("/").str[0]
    )

    # get rid of "  "
    results_df[f"composer_match_{i}"] = results_df[f"composer_match_{i}"].str.replace(
        "  ", " "
    )

    results_df[f"composer_match_{i}"] = (
        results_df[f"composer_{i}"].str.split(" ").str[-1]
    )

    # remove anything not a letter
    results_df[f"composer_match_{i}"] = results_df[f"composer_match_{i}"].str.replace(
        r"[^a-zA-Z]", "", regex=True
    )

    # get rid of spaces
    results_df[f"composer_match_{i}"] = results_df[f"composer_match_{i}"].str.replace(
        " ", ""
    )

    arranger = results_df[f"composer_{i}"].str.split(" arr ").str[-1]
    arranger = arranger.str.split("arr").str[-1]
    arranger = arranger.str.split("/").str[-1]
    arranger = arranger.str.replace("  ", " ")
    arranger = arranger.str.strip()
    arranger = arranger.str.split(" ").str[-1]
    arranger = arranger.str.replace(r"[^a-zA-Z]", "", regex=True)
    results_df[f"arranger_{i}"] = arranger
    # Create a new column by combining composer and arranger for the i-th round
    results_df[f"composer+arranger_{i}"] = (
        results_df[f"composer_match_{i}"] + results_df[f"arranger_{i}"]
    )

    # Use .loc with vectorized logic to check if arranger is in composer_match
    results_df.loc[
        results_df[f"arranger_{i}"].isin(results_df[f"composer_match_{i}"]),
        f"composer+arranger_{i}",
    ] = results_df[f"composer_match_{i}"]

grouped_df = (
    results_df[["event", "title_simple_1", "composer+arranger_1"]]
    .groupby(["event", "title_simple_1", "composer+arranger_1"])
    .size()
    .reset_index(name="count")
)
# rename columns
grouped_df.columns = ["event", "title", "composer", "count"]

# concat with 2 and 3
grouped_df_2 = (
    results_df[["event", "title_simple_2", "composer+arranger_2"]]
    .groupby(["event", "title_simple_2", "composer+arranger_2"])
    .size()
    .reset_index(name="count")
)
# rename columns
grouped_df_2.columns = ["event", "title", "composer", "count"]

grouped_df_3 = (
    results_df[["event", "title_simple_3", "composer+arranger_3"]]
    .groupby(["event", "title_simple_3", "composer+arranger_3"])
    .size()
    .reset_index(name="count")
)
# rename columns
grouped_df_3.columns = ["event", "title", "composer", "count"]

grouped_df = pd.concat([grouped_df, grouped_df_2, grouped_df_3])
# group by event, title, composer
grouped_df = grouped_df.groupby(["event", "title", "composer"]).sum().reset_index()

grouped_df = grouped_df.sort_values(by="count", ascending=False)


# save to db as table grouped_entries
conn = sqlite3.connect("uil.db")
grouped_df.to_sql("arranger_grouped_entries", conn, if_exists="replace", index=False)
conn.close()
