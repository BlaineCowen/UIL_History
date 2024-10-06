from numpy.lib.arraysetops import unique
import pandas as pd
import numpy as np
import difflib
import sqlite3
from multiprocessing import Pool
import re
from fuzzywuzzy import process
from scrape_data.fix_pml import fix_everything
from tqdm import tqdm
from add_performance_data import main as add_performance_data
from multiprocessing import Manager


def add_song_concat(pml):
    # lowercase everything and remove spaces
    pml = pml.apply(lambda x: x.astype(str).str.lower())
    pml = pml.apply(lambda x: x.astype(str).str.replace(" ", ""))
    pml = pml.apply(lambda x: x.astype(str).str.replace(r"[^\w\s]", ""))
    # change all columns to lower case and replace spaces with _
    pml.columns = pml.columns.str.lower().str.replace(" ", "_", regex=True)

    # make code only the last after -
    pml["code"] = pml["code"].str.split("-").str[-1]
    # remove any non letter or number characters
    pml["code"] = pml["code"].replace(r"[^\w\s]", "", regex=True)

    # CODE SHOULD ONLY BE 5 CHARACTERS
    pml["code"] = pml["code"].apply(lambda x: x[:-1] if len(x) == 6 else x)

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

    # strip composer
    pml["composer"] = pml["composer"].str.strip()

    # if composer has a " " in it, make the last word the composer
    pml["composer_last"] = pml["composer"].str.split(" ").str[-1]

    pml["composer_no_hyphen"] = pml["composer"].str.split("-").str[-1]

    # make a compoer/arranger column
    pml["composer"] = (
        pml["composer"].str.lower().replace(r"[^a-zA-Z0-9]", "", regex=True)
    )

    pml["composer_last"] = (
        pml["composer_last"].str.lower().replace(r"[^a-zA-Z0-9]", "", regex=True)
    )

    pml["composer_no_hyphen"] = (
        pml["composer_no_hyphen"].str.lower().replace(r"[^a-zA-Z0-9]", "", regex=True)
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

    # fill na with empty string
    pml = pml.fillna("")
    # replace nan with empty string
    pml = pml.replace(np.nan, "", regex=True)

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


# creat function that wil group titles 1, 2, 3
def group_titles(df):

    grouped_df1 = df.groupby(
        [
            "title_simple_1",
            "composer_1",
            "composer_last_1",
            "composer_no_hyphen_1",
            "event",
        ],
        as_index=False,
    ).size()
    grouped_df2 = df.groupby(
        [
            "title_simple_2",
            "composer_2",
            "composer_last_2",
            "composer_no_hyphen_2",
            "event",
        ],
        as_index=False,
    ).size()
    grouped_df3 = df.groupby(
        [
            "title_simple_3",
            "composer_3",
            "composer_last_3",
            "composer_no_hyphen_3",
            "event",
        ],
        as_index=False,
    ).size()

    # rename columns to  no numbers
    grouped_df1.columns = [
        "title",
        "composer",
        "composer_last",
        "composer_no_hyphen",
        "event",
        "count",
    ]
    grouped_df2.columns = [
        "title",
        "composer",
        "composer_last",
        "composer_no_hyphen",
        "event",
        "count",
    ]
    grouped_df3.columns = [
        "title",
        "composer",
        "composer_last",
        "composer_no_hyphen",
        "event",
        "count",
    ]

    # combine
    grouped_df = pd.concat([grouped_df1, grouped_df2, grouped_df3])

    # group by title, composer, composer_last, event
    grouped_df = grouped_df.groupby(
        ["title", "composer", "composer_last", "composer_no_hyphen", "event"],
        as_index=False,
    ).sum()

    # for evernt, split after -, take last, remove spaces, and only keep letters
    grouped_df["event"] = grouped_df["event"].str.split("-").str[-1]
    grouped_df["event"] = grouped_df["event"].str.replace(" ", "")
    grouped_df["event"] = grouped_df["event"].str.replace(r"[^a-zA-Z]", "", regex=True)

    # give each one a unique id
    grouped_df["entry_id"] = range(1, len(grouped_df) + 1)

    # change to str
    grouped_df["entry_id"] = grouped_df["entry_id"].astype(str)

    df["event"] = df["event"].str.split("-").str[-1]
    df["event"] = df["event"].str.replace(" ", "")
    df["event"] = df["event"].str.replace(r"[^a-zA-Z]", "", regex=True)

    df = df.merge(
        grouped_df,
        how="left",
        left_on=[
            "title_simple_1",
            "composer_1",
            "composer_last_1",
            "composer_no_hyphen_1",
            "event",
        ],
        right_on=["title", "composer", "composer_last", "composer_no_hyphen", "event"],
    )
    df["entry_id_1"] = df["entry_id"]
    df = df.drop(
        columns=["title", "composer", "composer_last", "composer_no_hyphen", "entry_id"]
    )

    df = df.merge(
        grouped_df,
        how="left",
        left_on=[
            "title_simple_2",
            "composer_2",
            "composer_last_2",
            "composer_no_hyphen_2",
            "event",
        ],
        right_on=["title", "composer", "composer_last", "composer_no_hyphen", "event"],
    )
    df["entry_id_2"] = df["entry_id"]
    df = df.drop(
        columns=["title", "composer", "composer_last", "composer_no_hyphen", "entry_id"]
    )

    df = df.merge(
        grouped_df,
        how="left",
        left_on=[
            "title_simple_3",
            "composer_3",
            "composer_last_3",
            "composer_no_hyphen_3",
            "event",
        ],
        right_on=["title", "composer", "composer_last", "composer_no_hyphen", "event"],
    )
    df["entry_id_3"] = df["entry_id"]
    df = df.drop(
        columns=[
            "title",
            "composer",
            "composer_last",
            "composer_no_hyphen",
            "entry_id",
            "count_x",
            "count_y",
            "count",
        ]
    )

    grouped_df["code"] = None

    # update results table
    conn = sqlite3.connect("uil.db")
    df.to_sql("results", conn, if_exists="replace", index=False)
    conn.close()

    return grouped_df


def fuzzy_search(title, composer, composer_last, composer_no_hyphen, event_name, pml):

    if "orchestra" in event_name:
        event_name = "orchestra"

    if "band" in event_name:
        event_name = "band"

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


def process_row_exact(args):

    # Unpack the arguments
    col, row, pml, lock = args

    code_index = col.index("code")

    title = row[col.index("title")]
    composer = row[col.index("composer")]
    composer_last = row[col.index("composer_last")]
    event_name = row[col.index("event")]
    composer_no_hyphen = row[col.index("composer_no_hyphen")]

    # if title is empty return
    if not title:
        return None

    if not composer and not composer_last:
        return None

    if not event_name:
        return None

    check_unique = pd.read_sql(
        f'SELECT * FROM unique_entries WHERE entry_id = {row[col.index("entry_id")]}',
        sqlite3.connect("uil.db"),
    )

    if check_unique["code"].iloc[0] == "" or check_unique["code"].iloc[0] is None:
        code = fuzzy_search(
            title, composer, composer_last, composer_no_hyphen, event_name, pml
        )

    else:
        code = check_unique["code"].iloc[0]

    if code is None:
        return None

    # update the results_with_codes table using the entry_number
    try:

        lock.acquire()

        try:
            conn = sqlite3.connect("uil.db")
            # Create a cursor
            c = conn.cursor()

            # set current row in unique_entries to have the code
            c.execute(
                f'UPDATE unique_entries SET code = "{code}" WHERE entry_id = {row[col.index("entry_id")]}'
            )

            # also update the results table
            for i in range(1, 4):
                c.execute(
                    f'UPDATE results SET code_{i} = "{code}" WHERE entry_id_{i} = {row[col.index("entry_id")]}'
                )

            # Commit the changes
            conn.commit()

        except sqlite3.Error as e:
            print("Error", e)
        finally:
            conn.close()
        lock.release()
    except Exception as e:
        print("Error", e)


def main():
    start_time = pd.Timestamp.now()
    # Create a manager
    manager = Manager()

    # Create a lock
    lock = manager.Lock()

    # create new pml from csv
    pml = pd.read_csv("scrape_data/pml.csv", encoding="utf-8")

    # replace pml table in db
    conn = sqlite3.connect("uil.db")
    pml.to_sql("pml", conn, if_exists="replace", index=False)
    conn.close()

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

    try:
        uil_data = pd.read_sql(
            'SELECT * FROM results WHERE "code_1" like "none" OR "code_2" like "none" or "code_3" like "none" or "code_1" is null or "code_2" is null or "code_3" is null  or "code_1" = "" or "code_2" = "" or "code_3" = ""',
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

    # save with pandas
    conn = sqlite3.connect("uil.db")
    uil_data.to_sql("results", conn, if_exists="replace", index=False)
    conn.close()

    uil_data = uil_data.sort_values("entry_number", ascending=False)

    pml = add_song_concat(pd.read_csv("scrape_data/pml.csv", encoding="utf-8"))

    # create pml table

    conn = sqlite3.connect("uil.db")

    unique_entries = group_titles(uil_data)
    # add unique codes as table to db
    unique_entries.reset_index(drop=True, inplace=True)
    # try to drop level_0
    try:
        unique_entries.drop("level_0", axis=1, inplace=True)
    except:
        pass
    unique_entries.to_sql(
        "unique_entries",
        sqlite3.connect("uil.db"),
        if_exists="replace",
    )

    # only get unique entries where code is none
    try:
        unique_entries = unique_entries[
            (unique_entries["code"] == "") | (unique_entries["code"].isnull())
        ]
    except:
        pass

    args = [
        (row._fields, tuple(row), pml, lock)
        for row in unique_entries.itertuples(index=False)
    ]

    print(pd.Timestamp.now() - start_time)

    with Pool(8) as pool, Manager() as manager:
        # Create a shared list to store results
        result_list = manager.list()

        # Define a callback function to update the progress bar
        def update(*a):
            pbar.update()

        # Add pml and lock as arguments to each element in rows_with_columns

        with tqdm(total=len(unique_entries)) as pbar:
            for _ in pool.imap_unordered(process_row_exact, args):
                update()

    conn.close()


if __name__ == "__main__":
    main()
    add_performance_data()
    print("done")
