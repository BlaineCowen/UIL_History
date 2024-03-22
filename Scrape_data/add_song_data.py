from numpy.lib.arraysetops import unique
import pandas as pd
import numpy as np
import difflib
import sqlite3
from multiprocessing import Pool
import re
from fuzzywuzzy import process
from fix_everything import fix_everything


def add_song_concat(pml):
    # lowercase everything and remove spaces
    pml = pml.apply(lambda x: x.astype(str).str.lower())
    pml = pml.apply(lambda x: x.astype(str).str.replace(" ", ""))
    pml = pml.apply(lambda x: x.astype(str).str.replace(r"[^\w\s]", ""))
    # change all columns to lower case and replace spaces with _
    pml.columns = pml.columns.str.lower().str.replace(" ", "_")

    # make code only the last after -
    pml["code"] = pml["code"].str.split("-").str[-1]

    # add a song_search column to the pml
    pml["song_search"] = (
        pml["title"].str.lower().replace(" ", "").replace(r"[^a-zA-Z]", "", regex=True)
    )

    # get rid of from if in title and anything after
    pml["song_search"] = pml["song_search"].str.split("from").str[0]

    pml["song_simple"] = (
        pml["title"]
        .str.lower()
        .replace(r"\(.*?\)", "", regex=True)
        .replace(r"[^a-zA-Z]", "", regex=True)
    )

    pml["composer_search"] = (
        pml["composer"]
        .str.lower()
        .replace(" ", "")
        .replace(r"[^a-zA-Z]", "", regex=True)
    )

    # only keep letters and numbers
    pml["song_search"] = (
        pml["song_search"].replace(r"[^\w\s]", "").replace(r"[^a-zA-Z]", "", regex=True)
    )
    pml["composer_search"] = (
        pml["composer_search"]
        .replace(r"[^\w\s]", "")
        .replace(r"[^a-zA-Z]", "", regex=True)
    )

    pml["specification"] = (
        pml["specification"]
        .str.lower()
        .replace(" ", "")
        .replace(r"[^a-zA-Z]", "", regex=True)
    )

    pml["song_search_with_specification"] = pml["song_search"] + pml["specification"]

    # make a compoer/arranger column
    pml["composer"] = (
        pml["composer"].str.lower().replace(r"[^a-zA-Z0-9]", "", regex=True)
    )

    # if composer is anonortrad, make it ""
    pml["composer"] = (
        pml["composer"]
        .replace("anonortrad", "")
        .replace("trad", "")
        .replace("traditional", "")
    )

    # if composer is now blank, make arranger the composer
    pml["arranger"] = pml[pml["composer"] == ""]["arranger"]

    pml["arranger"] = (
        pml["arranger"].str.lower().replace(r"[^a-zA-Z0-9]", "", regex=True)
    )
    pml["composer_arranger"] = pml["composer"] + pml["arranger"]
    pml["composer_arranger"] = pml["composer_arranger"].replace(r"[^\w\s]", "")
    pml["composer_arranger"] = pml["composer_arranger"].replace(" ", "")

    pml["event_name"] = (
        pml["event_name"].str.lower().replace(r"[^a-zA-Z]", "", regex=True)
    )

    pml.to_sql("pml", sqlite3.connect("uil.db"), if_exists="replace")

    return pml


pml = add_song_concat(pd.read_csv("Scrape_data/pml.csv", encoding="utf-8"))


def definite_match(col, row, n, pml=pml):

    try:
        title_col = col.index(f"title_{n}")
        original_title = row[title_col]
        # title = re.sub(r"[^a-zA-Z]", "", row[title_col]).lower()
        title = row[title_col].strip().lower()

        # if title contains 5 numbers in a row, return them as code
        if re.search(r"\d{5}", title):
            code = re.search(r"\d{5}", title).group(0)
            return code

        # get rid of everything no a character
        title = re.sub(r"[^a-zA-Z]", "", title)

        # if  title contains "from" get rid of from and anything after
        if "from" in title:
            title = title.split("from")[0]

        composer_col = col.index(f"composer_{n}")
        composer = row[composer_col].strip()
        composer.replace(")", "").replace("(", "")
        # check if composer has space
        if " " in composer:
            composer_last = composer.split(" ")[-1]
            composer_last = re.sub(r"[^a-zA-Z]", "", composer_last).lower()

        if "/" in composer:
            composer = composer.split("/")[0]
            composer_last = composer.split("/")[-1]

        else:
            composer_last = composer.lower()

        composer = re.sub(r"[^a-zA-Z]", "", row[composer_col]).lower()
        composer = composer.replace("anonortrad", "")
        composer = composer.replace("trad", "")

    except Exception as e:
        print("Error", e)
        return None

    # check if title just has a number already. it will be any 5 digit number in the title
    if re.search(r"\d{5}", title):
        # extract the number
        match = re.search(r"\d{5}", title)
        code = match.group(0)

        return code

    event_name = row[col.index("event")].lower().replace(" ", "")
    # only keep text after -
    event_name = event_name.split("-")[1]
    # get rid of anything not a letter
    event_name = re.sub(r"[^a-zA-Z]", "", event_name)

    # if contains band ==band
    if "band" in event_name:
        event_name = "band"

    def fuzzy_search(title, composer, composer_last, event_name, pml):
        # Fetch all rows from the table
        df = pml

        # Filter the DataFrame based on composer, arranger, and event_name
        df = df[
            (
                (df["composer_search"].str.contains(composer, regex=False))
                | (df["arranger"].str.contains(composer, regex=False))
                | (df["composer_search"].str.contains(composer_last, regex=False))
                | (df["arranger"].str.contains(composer_last, regex=False))
            )
            & (df["event_name"] == event_name)
        ]

        # Perform fuzzy matching on the filtered DataFrame
        choices = (
            df["song_search"].tolist()
            + df["song_search_with_specification"].tolist()
            + df["song_simple"].tolist()
        )

        # check if there is title
        if not title:
            return None
        best_match = process.extractOne(title, choices, score_cutoff=80)

        # Check if a match was found
        if best_match is not None:
            # Get the row with the best match
            result = df[
                (df["song_search"] == best_match[0])
                | (df["song_search_with_specification"] == best_match[0])
                | (df["song_simple"] == best_match[0])
            ]
        else:
            result = None
            return result

        return result["code"].iloc[0]

    # Call the function
    result = fuzzy_search(title, composer, composer_last, event_name, pml)

    if result is not None:
        return result
    else:
        return None


def process_row_exact(row_with_columns):

    # if entry number mod 100 == 0, print entry number
    if row_with_columns[1][row_with_columns[0].index("entry_number")] % 100 == 0:
        print(row_with_columns[1][row_with_columns[0].index("entry_number")])

    columns, row = row_with_columns
    try:
        for n in range(1, 4):
            code_index = columns.index(f"code_{n}")
            # if the code is already there, skip
            if row[code_index]:
                continue

            code = definite_match(columns, row, n)
            row[code_index] = code

    except Exception as e:
        print("Error", e)

    entry_number = row[columns.index("entry_number")]
    # update the results_with_codes table using the entry_number
    try:

        conn = sqlite3.connect("uil.db")
        # Create a cursor
        c = conn.cursor()

        # Extract data from 'row' variable (assuming it's properly defined elsewhere)
        code1_value = row[columns.index("code_1")]
        code2_value = row[columns.index("code_2")]
        code3_value = row[columns.index("code_3")]

        c.execute(
            "UPDATE results_with_codes SET code_1 = ?, code_2 = ?, code_3 = ? WHERE entry_number = ?",
            (code1_value, code2_value, code3_value, str(entry_number)),
        )

        # Commit the changes
        conn.commit()

        # Fetch the updated row for verification
        updated = c.execute(
            "SELECT * FROM results_with_codes WHERE entry_number = ?",
            (entry_number,),
        ).fetchone()

        # Return columns and row (assuming this is necessary)
        return columns, row

    except sqlite3.Error as e:
        # Handle any errors that occur during execution
        print("SQLite error:", e)


if __name__ == "__main__":

    # change results table column names to lower case and replace spaces with _

    num_processes = 6

    pool = Pool(processes=num_processes)

    # connect to db
    conn = sqlite3.connect("uil.db")

    uil_data = pd.read_sql("SELECT * FROM results", conn)
    # add columns for code_1, 2, 3 if already none
    if "code_1" not in uil_data.columns:
        uil_data["code_1"] = None
    if "code_2" not in uil_data.columns:
        uil_data["code_2"] = None
    if "code_3" not in uil_data.columns:
        uil_data["code_3"] = None

    # change all columns to lower case and replace spaces with _
    uil_data.columns = uil_data.columns.str.lower().str.replace(" ", "_")

    # update in db reset index
    uil_data = uil_data.reset_index(drop=True)
    uil_data.to_sql("results", conn, if_exists="replace", index=False)

    # if results_with_codes does n ot exist, create it. It is a copy of results with code_1, 2, 3

    try:
        uil_data = pd.read_sql(
            'SELECT * FROM results_with_codes WHERE "code_1" is NULL OR "code_2" is NULL or "code_3" is NULL',
            sqlite3.connect("uil.db"),
        )
    except:
        # create results_with_codes table
        uil_data.to_sql("results_with_codes", conn, if_exists="replace", index=False)

    # randomize index of uil_data
    # uil_data = uil_data.sample(frac=1)
    # sort by entry number
    uil_data = uil_data.sort_values("entry_number")

    # close connection
    conn.close()

    columns = uil_data.columns.to_list()

    pml = add_song_concat(pd.read_csv("Scrape_data/pml.csv", encoding="utf-8"))

    # create pml table

    # time
    start_time = pd.Timestamp.now()
    rows_with_columns = [(columns, list(row)) for row in uil_data.to_numpy()]
    processed_results = pool.map(process_row_exact, rows_with_columns)

    pool.close()
    pool.join()

    print(pd.Timestamp.now() - start_time)

    # time
    start_time = pd.Timestamp.now()
    fix_everything()
    print(pd.Timestamp.now() - start_time)

    # close the connection
