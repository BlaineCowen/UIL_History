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

        df[f"composer_{i}"] = df[f"composer_{i}"].str.lower().str.strip()

        # erase annything containing arr and anything after
        df[f"composer_{i}"] = df[f"composer_{i}"].str.split(" arr ").str[0]
        df[f"composer_{i}"] = df[f"composer_{i}"].str.split("arr.").str[0]
        # strip composer
        df[f"composer_{i}"] = df[f"composer_{i}"].str.strip()

        # if composer has a /
        mask = df[f"composer_{i}"].str.contains("/", regex=False)
        df.loc[mask, f"composer_{i}"] = (
            df.loc[mask, f"composer_{i}"].str.split("/").str[0].str.split(" ").str[-1]
        )

        mask = df[f"composer_{i}"].str.contains("/", regex=False)
        df.loc[mask, f"composer_last_{i}"] = (
            df.loc[mask, f"composer_{i}"].str.split("/").str[-1].str.split(" ").str[-1]
        )

        mask = df[f"composer_{i}"].str.contains("/", regex=False)
        df.loc[~mask, f"composer_last_{i}"] = (
            df.loc[~mask, f"composer_{i}"].str.split(" ").str[-1]
        )

        df[f"composer_no_hyphen_{i}"] = df[f"composer_{i}"].str.split("-").str[-1]

        # remove anything not a letter
        df[f"composer_{i}"] = df[f"composer_{i}"].str.replace(
            r"[^a-zA-Z]", "", regex=True
        )
        df[f"composer_last_{i}"] = df[f"composer_last_{i}"].str.replace(
            r"[^a-zA-Z]", "", regex=True
        )
        df[f"composer_no_hyphen_{i}"] = df[f"composer_no_hyphen_{i}"].str.replace(
            r"[^a-zA-Z]", "", regex=True
        )

    # make event lower
    df["event"] = df["event"].str.lower()

    return df


def fuzzy_search(title, composer, composer_last, composer_no_hyphen, event_name, pml):

    if "orchestra" in event_name:
        event_name = "orchestra"

    if "band" in event_name:
        event_name = "band"

    if "nachtigall" in title:
        print(title)

    mask = (pml["event_name"].str.contains(event_name, regex=False)) & (
        (pml["composer_search"].str.contains(composer, regex=False))
        | (pml["composer_search"].str.contains(composer_last, regex=False))
        | (pml["composer_no_hyphen"].str.contains(composer, regex=False))
        | (pml["composer_no_hyphen"].str.contains(composer_no_hyphen, regex=False))
        | (pml["arranger"].str.contains(composer, regex=False))
        | (pml["arranger"].str.contains(composer_last, regex=False))
    )

    possible_match_df = pml[mask]
    if len(possible_match_df) == 0:
        return None

    if len(possible_match_df) == 1:
        return possible_match_df["code"].iloc[0]

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
        conn = sqlite3.connect("uil.db")
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


def process_row_exact(args):

    # Unpack the arguments
    col, row, pml, unique_entries, lock = args

    for i in range(1, 4):
        title = row[col.index(f"title_simple_{i}")]
        composer = row[col.index(f"composer_{i}")]
        composer_last = row[col.index(f"composer_last_{i}")]
        composer_no_hyphen = row[col.index(f"composer_no_hyphen_{i}")]
        event = row[col.index("event")]
        entry_number = row[col.index("entry_number")]

        # if title is empty return
        if not title:
            code = None
            continue

        if not composer and not composer_last:
            code = None
            continue

        if not event:
            code = None
            continue

        # first see if the entry is already in unique entryies
        mask = (
            (unique_entries["title"] == title)
            & (
                (unique_entries["composer"] == composer)
                | (unique_entries["composer"] == composer_last)
                | (unique_entries["composer_no_hyphen"] == composer_no_hyphen)
            )
            & (unique_entries["event"] == event)
        )

        if not unique_entries[mask].empty:
            code = unique_entries[mask]["code"].iloc[0]

            update_db(code, i, entry_number, lock)
            continue

        code = fuzzy_search(
            title, composer, composer_last, composer_no_hyphen, event, pml
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
    conn = sqlite3.connect("uil.db")

    try:
        uil_data = pd.read_sql(
            'SELECT * FROM results WHERE ("code_1" like "none" and "code_2" like "none" and "code_3" like "none") or ("code_1" is null and "code_2" is null and "code_3" is null) or ("code_1" = "" and "code_2" = "" and "code_3" = "") & "entry_id" is null',
            conn,
        )

    except sqlite3.OperationalError as e:
        print("Error", e)

    # if code is none fill ''
    uil_data["code_1"] = uil_data["code_1"].fillna("")
    uil_data["code_2"] = uil_data["code_2"].fillna("")
    uil_data["code_3"] = uil_data["code_3"].fillna("")

    # close connection
    conn.close()

    uil_data = add_concat_to_results(uil_data)

    columns = uil_data.columns.to_list()

    # sort so 145676 is first entry
    uil_data = uil_data.sort_values("entry_number", ascending=False)

    conn = sqlite3.connect("uil.db")

    pml = pd.read_sql("SELECT * FROM pml", conn)

    # create pml table

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

    conn.close()


if __name__ == "__main__":
    main()
    add_new_performance_data()
    print("done")
