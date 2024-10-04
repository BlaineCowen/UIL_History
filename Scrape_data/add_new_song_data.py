from numpy.lib.arraysetops import unique
import pandas as pd
import numpy as np
import difflib
import sqlite3
from multiprocessing import Pool
import re
from fuzzywuzzy import process
from fix_everything import fix_everything
from tqdm import tqdm
from add_new_performance_data import main as add_new_performance_data
from multiprocessing import Manager


def fix_pml(pml):
    # all lower
    pml["title"] = pml["title"].str.lower()
    pml["composer"] = pml["composer"].str.lower()

    pml["arranger"] = pml["arranger"].str.lower()
    pml["event_name"] = pml["event_name"].str.lower()

    # create song search column replace anything inside of parenthesis!
    pml["song_search"] = pml["title"].str.replace(r"\(.*?\)", "", regex=True)
    # remove anything not a letter
    pml["song_search"] = pml["song_search"].str.replace(r"[^a-zA-Z]", "", regex=True)

    # create song search with specification column
    pml["song_search_with_specification"] = pml["title"].str.replace(
        r"[^a-zA-Z]", "", regex=True
    )

    # create song simple column
    pml["song_simple"] = pml["title"].str.replace(r"[^a-zA-Z]", "", regex=True)

    return pml


def add_concat_to_results(df):

    # fill na in all title, composer, arranger, and event columns
    df["title_1"] = df["title_1"].fillna("")
    df["title_2"] = df["title_2"].fillna("")
    df["title_3"] = df["title_3"].fillna("")
    df["composer_1"] = df["composer_1"].fillna("")
    df["composer_2"] = df["composer_2"].fillna("")
    df["composer_3"] = df["composer_3"].fillna("")
    df["event"] = df["event"].fillna("")

    for i in range(1, 4):
        # make song simple lower
        df[f"title_simple_{i}"] = df[f"title_{i}"].str.lower()
        # add column potential match if there are 5 digits inside parenthesis
        df[f"potential_match_{i}"] = ""
        # remove anything in parenthesis
        df[f"title_simple_{i}"] = df[f"title_simple_{i}"].str.replace(
            r"\(.*?\)", "", regex=True
        )
        # remove anything not a letter
        df[f"title_simple_{i}"] = df[f"title_simple_{i}"].str.replace(
            r"[^a-zA-Z]", "", regex=True
        )
        # get rid of spaces
        df[f"title_simple_{i}"] = df[f"title_simple_{i}"].str.replace(" ", "")
        # if from is in the title, remove from and anything after
        df[f"title_simple_{i}"] = df[f"title_simple_{i}"].str.split("from").str[0]

        # erase anything inside of parenthesis and the parenthesis
        df[f"composer_{i}"] = (
            df[f"composer_{i}"]
            .str.lower()
            .str.replace(r"\(.*?\)", "", regex=True)
            .str.replace("  ", " ", regex=False)
            .str.replace("comp:", "", regex=False)
            .str.replace("arr:", "", regex=False)
        )

        df[f"composer_{i}"] = df[f"composer_{i}"].str.strip()

        # erase annything containing arr and anything after
        df[f"composer_match_{i}"] = df[f"composer_{i}"].str.split(" arr ").str[0]
        df[f"composer_match_{i}"] = df[f"composer_{i}"].str.split("arr.").str[0]
        df[f"composer_match_{i}"] = df[f"composer_{i}"].str.split("/").str[0]

        # get rid of "  "
        df[f"composer_match_{i}"] = df[f"composer_match_{i}"].str.replace(
            "  ", " ", regex=False
        )

        df[f"arranger_{i}"] = df[f"composer_{i}"].str.split(" arr ").str[-1]
        df[f"arranger_{i}"] = df[f"composer_{i}"].str.split("arr.").str[-1]
        df[f"arranger_{i}"] = df[f"composer_{i}"].str.split("/").str[-1]

        # get rid of "  "
        df[f"arranger_{i}"] = df[f"arranger_{i}"].str.replace("  ", " ", regex=False)

        # take the second word if there is a space
        df[f"composer_match_{i}"] = df[f"composer_match_{i}"].apply(
            lambda x: x.split(" ")[-1].strip() if " " in x else x.strip()
        )
        # take the second word if .
        df[f"composer_match_{i}"] = df[f"composer_match_{i}"].apply(
            lambda x: x.split(".")[-1].strip() if " " in x else x.strip()
        )

        # get the first name just in case
        df[f"composer_first_{i}"] = df[f"composer_match_{i}"].apply(
            lambda x: x.split(" ")[0].strip() if " " in x else x.strip()
        )
        df[f"composer_first_{i}"] = df[f"composer_match_{i}"].apply(
            lambda x: x.split(".")[0].strip() if " " in x else x.strip()
        )

        # get last name of arranger
        df[f"arranger_{i}"] = df[f"arranger_{i}"].apply(
            lambda x: x.split(" ")[-1].strip() if " " in x else x.strip()
        )
        df[f"arranger_{i}"] = df[f"arranger_{i}"].apply(
            lambda x: x.split(".")[-1].strip() if " " in x else x.strip()
        )

        # strip arranger
        df[f"arranger_{i}"] = df[f"arranger_{i}"].str.strip().str.lower()

        # remove anything not a letter
        df[f"composer_match_{i}"] = df[f"composer_match_{i}"].str.replace(
            r"[^a-zA-Z]", "", regex=True
        )

        df[f"composer_last_{i}"] = df[f"composer_last_{i}"].str.replace(
            r"[^a-zA-Z]", "", regex=True
        )

        df[f"composer_no_hyphen_{i}"] = df[f"composer_{i}"].str.replace(
            r"[^a-zA-Z]", "", regex=True
        )

        # change all the new columns to lower
        df[f"composer_match_{i}"] = df[f"composer_match_{i}"].str.lower()
        df[f"composer_last_{i}"] = df[f"composer_last_{i}"].str.lower()
        df[f"composer_first_{i}"] = df[f"composer_first_{i}"].str.lower()
        df[f"composer_no_hyphen_{i}"] = df[f"composer_no_hyphen_{i}"].str.lower()
        df[f"arranger_{i}"] = df[f"arranger_{i}"].str.lower()

    # make event lower
    df["event"] = df["event"].str.lower()

    return df


def fuzzy_search(
    title, composer_first, composer_match, composer_no_hyphen, arranger, event_name, pml
):

    if "orchestra" in event_name:
        event_name = "orchestra"

    if "band" in event_name:
        event_name = "band"

    mask = (pml["event_name"].str.contains(event_name, regex=False)) & (
        (pml["composer_search"].str.contains(composer_first, regex=False))
        | (pml["composer_search"].str.contains(composer_match, regex=False))
        | (pml["composer_no_hyphen"].str.contains(composer_match, regex=False))
        | (pml["composer_no_hyphen"].str.contains(composer_no_hyphen, regex=False))
        | (pml["arranger"].str.contains(arranger, regex=False))
        | (pml["arranger"].str.contains(composer_match, regex=False))
    )

    possible_match_df = pml[mask]
    if len(possible_match_df) == 0:
        return None

    else:
        choices = (
            possible_match_df["song_search"].tolist()
            + possible_match_df["song_search_with_specification"].tolist()
            + possible_match_df["song_simple"].tolist()
        )

        best_match = process.extractOne(title, choices, score_cutoff=80)
        if best_match is not None:
            result = possible_match_df[
                (possible_match_df["song_search"] == best_match[0])
                | (possible_match_df["song_search_with_specification"] == best_match[0])
                | (possible_match_df["song_simple"] == best_match[0])
            ]
            if not result.empty:
                result = result.sort_values(by="grade", ascending=False)
                return result["code"].iloc[0]
    return None


def update_db(code, n, entry_number, lock):
    try:
        lock.acquire()
        conn = sqlite3.connect("combined.db")
        c = conn.cursor()

        c.execute(
            f'UPDATE results SET code_{n} = "{code}" WHERE entry_number = {entry_number}'
        )

        conn.commit()
    except sqlite3.Error as e:
        print("Error", e)
    finally:
        conn.close()
        lock.release()


def update_unique_entries(
    title, composer, composer_last, composer_no_hyphen, arranger, event, lock
):
    try:
        lock.acquire()
        conn = sqlite3.connect("combined.db")

        c = conn.cursor()

        c.execute(
            f'INSERT INTO unique_entries (title, composer, composer_last, composer_no_hyphen, arranger, event) VALUES ("{title}", "{composer}", "{composer_last}", "{composer_no_hyphen}", "{arranger}","{event}")'
        )

        conn.commit()
    except sqlite3.Error as e:
        print("Error", e)
    finally:
        conn.close()
        lock.release()


def process_row_exact(args):

    # Unpack the arguments
    col, row, pml, unique_entries, lock = args

    for i in range(1, 4):
        title = row[col.index(f"title_simple_{i}")]
        composer_first = row[col.index(f"composer_first_{i}")]
        composer_match = row[col.index(f"composer_match_{i}")]
        arranger = row[col.index(f"arranger_{i}")]
        composer_no_hyphen = row[col.index(f"composer_no_hyphen_{i}")]

        event = row[col.index("event")]
        entry_number = row[col.index("entry_number")]

        check_code = row[col.index(f"code_{i}")]
        if check_code.lower() != "none" and check_code != "":
            code = check_code
            continue

        # if title is empty return
        if not title:
            code = None
            continue

        if not composer_match:
            code = None
            continue

        if not event:
            code = None
            continue

        # check if the entry is already in the unique_entries table
        mask = (
            (unique_entries["title"] == title)
            & (
                (unique_entries["composer"] == composer_first)
                | (unique_entries["composer"] == composer_match)
                | (unique_entries["composer_no_hyphen"] == composer_no_hyphen)
            )
            & (unique_entries["event"] == event)
        )

        if not unique_entries[mask].empty:
            code = unique_entries[mask]["code"].iloc[0]
            update_db(code, i, entry_number, lock)
            continue

        else:
            # add to unique_entries
            update_unique_entries(
                title,
                composer_first,
                composer_match,
                composer_no_hyphen,
                arranger,
                event,
                lock,
            )
            # first see if the entry is already in unique entryies
        mask = (
            (unique_entries["title"] == title)
            & (
                (unique_entries["composer"] == composer_first)
                | (unique_entries["composer"] == composer_match)
                | (unique_entries["composer_no_hyphen"] == composer_no_hyphen)
            )
            & (unique_entries["event"] == event)
        )

        if not unique_entries[mask].empty:
            code = unique_entries[mask]["code"].iloc[0]

            update_db(code, i, entry_number, lock)
            continue

        code = fuzzy_search(
            title,
            composer_first,
            composer_match,
            composer_no_hyphen,
            arranger,
            event,
            pml,
        )

        if code is None:
            continue

        # update the results_with_codes table using the entry_number
        update_db(code, i, entry_number, lock)
    return


def main():
    start_time = pd.Timestamp.now()
    # Create a manager
    manager = Manager()

    # Create a lock
    lock = manager.Lock()

    # connect to db
    conn = sqlite3.connect("combined.db")

    uil_data = pd.read_sql("SELECT * FROM results", conn)

    # add composer_last and composer_no_hyphen columns
    for i in range(1, 4):
        uil_data[f"composer_last_{i}"] = ""
        uil_data[f"composer_no_hyphen_{i}"] = ""

    # save db
    uil_data.to_sql("results", conn, if_exists="replace", index=False)

    try:
        uil_data = pd.read_sql(
            'SELECT * FROM results WHERE ("code_1" like "none" or "code_2" like "none" or "code_3" like "none") or ("code_1" is null or "code_2" is null or "code_3" is null) or ("code_1" = "" or "code_2" = "" or "code_3" = "")',
            conn,
        )

    except sqlite3.OperationalError as e:
        print("Error", e)

    # if code is none fill ''
    uil_data["code_1"] = uil_data["code_1"].fillna("")
    uil_data["code_2"] = uil_data["code_2"].fillna("")
    uil_data["code_3"] = uil_data["code_3"].fillna("")

    # get missing length before
    missing_length_before = len(uil_data)

    # close connection
    conn.close()

    uil_data = add_concat_to_results(uil_data)

    columns = uil_data.columns.to_list()

    # sort so 145676 is first entry
    uil_data = uil_data.sort_values("entry_number", ascending=False)

    conn = sqlite3.connect("combined.db")

    pml = pd.read_sql("SELECT * FROM pml", conn)

    # add a composer_search column to pml
    pml["composer_search"] = (
        pml["composer"]
        .str.replace(" ", "")
        .str.lower()
        .str.strip()
        .str.replace(r"[^a-zA-Z]", "", regex=True)
    )

    # add composer_last column to pml
    pml["composer_last"] = (
        pml["composer"]
        .str.split(" ")
        .str[-1]
        .str.replace(r"[^a-zA-Z]", "", regex=True)
        .str.lower()
    )

    # add composer_no_hyphen column to pml
    pml["composer_no_hyphen"] = (
        pml["composer"]
        .str.lower()
        .str.replace("-", "")
        .str.replace(r"[^a-zA-Z]", "", regex=True)
    )

    # Fill NaN values in the 'composer' column with an empty string before creating the mask
    mask = pml["composer"].fillna("").str.contains("/", regex=False)

    pml.loc[mask, "arranger"] = (
        pml.loc[mask, "composer"]
        .fillna("")
        .str.split("/", expand=True)
        .iloc[:, -1]
        .str.split(" ", expand=True)
        .iloc[:, -1]
    )

    pml = fix_pml(pml)

    # create unique_entries table if not exists
    c = conn.cursor()
    # drop unique_entries table
    c.execute("DROP TABLE IF EXISTS unique_entries")
    c.execute(
        """CREATE TABLE IF NOT EXISTS unique_entries (
        code TEXT PRIMARY KEY,
        title TEXT,
        composer TEXT,
        composer_last TEXT,
        composer_no_hyphen TEXT,
        arranger TEXT,
        event TEXT
        )"""
    )

    conn.commit()

    # get unique entries
    unique_entries = pd.read_sql("SELECT * FROM unique_entries", conn)

    args = [
        (row._fields, tuple(row), pml, unique_entries, lock)
        for row in uil_data.itertuples(index=False)
    ]

    print(pd.Timestamp.now() - start_time)

    with Pool(8) as pool, Manager() as manager:
        # Create a shared list to store results
        result_list = manager.list()

        # Define a callback function to update the progress bar
        def update(*a):
            pbar.update()

        # Add pml and lock as arguments to each element in rows_with_columns

        with tqdm(total=len(uil_data)) as pbar:
            for _ in pool.imap_unordered(process_row_exact, args):
                update()

    print(pd.Timestamp.now() - start_time)
    # get missing length after
    conn = sqlite3.connect("combined.db")
    uil_data = pd.read_sql(
        'SELECT * FROM results WHERE ("code_1" like "none" or "code_2" like "none" or "code_3" like "none") or ("code_1" is null or "code_2" is null or "code_3" is null) or ("code_1" = "" or "code_2" = "" or "code_3" = "")',
        conn,
    )

    missing_length_after = len(uil_data)

    conn.close()


if __name__ == "__main__":
    main()
    add_new_performance_data()
    print("done")
