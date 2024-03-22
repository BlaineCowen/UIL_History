import pandas as pd
import os
import sqlite3
from datetime import datetime


def fix_everything():
    # # drop duplicate entry numbers in uil.db
    conn = sqlite3.connect("uil.db")
    pml = pd.read_sql_query("SELECT * FROM pml", conn)
    # pml = pd.read_csv("Scrape_data/pml.csv")

    # replacce all column names with underscores and lower
    pml.columns = pml.columns.str.lower().str.replace(" ", "_")

    pml["code"] = pml["code"].str.split("-").str[-1]
    # make sure code is str
    pml["code"] = pml["code"].astype(str)

    # remove any duplicate code
    pml.drop_duplicates(subset="code", inplace=True)

    # rename publisher_[collection] column with publisher
    pml.rename(columns={"publisher_[collection]": "publisher"}, inplace=True)

    # save
    pml.to_csv("pml.csv", index=False)
    conn = sqlite3.connect("uil.db")

    pml.to_sql("pml", conn, if_exists="replace", index=False)
    # drop results table and replace it with a copy of results_with_codes tables
    conn.execute("DROP TABLE results")
    conn.execute("CREATE TABLE results AS SELECT * FROM results_with_codes")

    df = pd.read_sql_query("SELECT * FROM results", conn)

    # change column names to have no - or .
    df.columns = df.columns.str.replace("-", "_").str.replace(".", "_")

    # change scores to int. if string is not a number, replace with 0
    df["concert_score_1"] = (
        pd.to_numeric(df["concert_score_1"], errors="coerce").fillna(0).astype(int)
    )
    df["concert_score_2"] = (
        pd.to_numeric(df["concert_score_2"], errors="coerce").fillna(0).astype(int)
    )
    df["concert_score_3"] = (
        pd.to_numeric(df["concert_score_3"], errors="coerce").fillna(0).astype(int)
    )
    df["concert_final_score"] = (
        pd.to_numeric(df["concert_final_score"], errors="coerce").fillna(0).astype(int)
    )
    df["sight_reading_score_1"] = (
        pd.to_numeric(df["sight_reading_score_1"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["sight_reading_score_2"] = (
        pd.to_numeric(df["sight_reading_score_2"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["sight_reading_score_3"] = (
        pd.to_numeric(df["sight_reading_score_3"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["sight_reading_final_score"] = (
        pd.to_numeric(df["sight_reading_final_score"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    # make sure code_1, code_2, code_3 are str
    df["code_1"] = df["code_1"].astype(str)
    df["code_2"] = df["code_2"].astype(str)
    df["code_3"] = df["code_3"].astype(str)

    # replace date with datetime in iso format
    df["contest_date"] = pd.to_datetime(df["contest_date"], format="%m/%d/%Y").dt.date

    def fix_non_pml_codes(pml, df):
        # go through df and if code_1,2,3 not in pml, add it to the pml table with song and compoer and event
        pml["code"] = pml["code"].astype(str)
        for i, row in df.iterrows():
            # check if entry number is 189541

            codes = pml["code"].values

            if row["code_1"] not in codes:
                new_row = pd.DataFrame(
                    {
                        "code": [row["code_1"]],
                        "title": [row["title_1"]],
                        "composer": [row["composer_1"]],
                        "event_name": [row["event"]],
                    }
                )
                pml = pd.concat([pml, new_row], ignore_index=True)
            if row["code_2"] not in codes:
                if row["code_2"] not in codes:
                    new_row = pd.DataFrame(
                        {
                            "code": [row["code_2"]],
                            "title": [row["title_2"]],
                            "composer": [row["composer_2"]],
                            "event_name": [row["event"]],
                        }
                    )
                    pml = pd.concat([pml, new_row], ignore_index=True)
            if row["code_3"] not in codes:
                new_row = pd.DataFrame(
                    {
                        "code": [row["code_3"]],
                        "title": [row["title_3"]],
                        "composer": [row["composer_3"]],
                        "event_name": [row["event"]],
                    }
                )
                pml = pd.concat([pml, new_row], ignore_index=True)

        # drop duplicates from pml
        pml.drop_duplicates(subset="code", inplace=True)

        # update pml table
        pml.to_sql("pml", conn, if_exists="replace", index=False)

        # replace table
        df.to_sql("results", conn, if_exists="replace", index=False)

        conn.close()

    fix_non_pml_codes(pml, df)


if __name__ == "__main__":
    print("fixing everything")
    start_time = datetime.now()
    fix_everything()
    print("done")
    print(datetime.now() - start_time)
