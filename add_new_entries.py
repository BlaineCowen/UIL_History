##disable-gpu

import requests
import pandas as pd
import sqlite3
import time
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import sqlite3
import os
import pandas as pd


# loops of csv files in csv_downloads folder
def loop_csv():

    conn = sqlite3.connect("uil.db")

    path = os.path.abspath("./scrape_data/csv_downloads/")

    results_df = pd.read_sql_query("SELECT * FROM results", conn)

    files = os.listdir(path)
    for file in files:
        df1 = pd.read_csv(f"{path}/{file}", header=2, encoding="iso-8859-1")

        # compare the entry numbers of the new csv file with the existing df
        # change df1 columns to match results
        df1.columns = (
            df1.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace(".", "_")
            .str.replace("-", "_")
        )

        # fix date column in this formate 2019-04-03 00:00:00
        if "contest_date" in df1.columns:
            df1["contest_date"] = pd.to_datetime(df1["contest_date"]).dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        # find the results row with matching entry_number
        results_df["entry_number"] = results_df["entry_number"].astype(str)
        df1["entry_number"] = df1["entry_number"].astype(str)

        cols_to_add = results_df.columns.difference(df1.columns)

        for col in cols_to_add:
            df1[col] = None

        for row in df1.itertuples():
            entry_number = row.entry_number
            results_row = results_df[results_df["entry_number"] == entry_number]

            # if there is no matching entry_number in results_df, add the row to results_df
            if results_row.empty:

                row_df = pd.DataFrame([row])
                # drop index
                # Drop the old index column from row_df
                if "Index" in row_df.columns:
                    row_df = row_df.drop(columns=["Index"])

                # Reset the index of row_df and concatenate it with results_df
                # Exclude columns with only empty or NA values from row_df
                row_df = row_df.dropna(how="all", axis=1)

                if row_df.empty:
                    continue

                # Concatenate the DataFrames
                results_df = pd.concat([results_df, row_df], ignore_index=True)
                print(f"added {entry_number}")

            # if there is a matching entry_number in results_df, update the row with just the scores
            else:
                results_df.loc[
                    results_df["entry_number"] == entry_number, "concert_score_1"
                ] = row.concert_score_1
                results_df.loc[
                    results_df["entry_number"] == entry_number, "concert_score_2"
                ] = row.concert_score_2
                results_df.loc[
                    results_df["entry_number"] == entry_number, "concert_score_3"
                ] = row.concert_score_3
                results_df.loc[
                    results_df["entry_number"] == entry_number, "concert_final_score"
                ] = row.concert_final_score
                results_df.loc[
                    results_df["entry_number"] == entry_number, "sight_reading_score_1"
                ] = row.sight_reading_score_1
                results_df.loc[
                    results_df["entry_number"] == entry_number, "sight_reading_score_2"
                ] = row.sight_reading_score_2
                results_df.loc[
                    results_df["entry_number"] == entry_number, "sight_reading_score_3"
                ] = row.sight_reading_score_3
                results_df.loc[
                    results_df["entry_number"] == entry_number,
                    "sight_reading_final_score",
                ] = row.sight_reading_final_score
                results_df.loc[results_df["entry_number"] == entry_number, "award"] = (
                    row.award
                )

                print(f"updated {entry_number}")

    # remove duplicates
    results_df = results_df.drop_duplicates()

    # update uil.db to this new df_concat
    conn = sqlite3.connect("uil.db")
    results_df.to_sql("results", conn, if_exists="replace", index=False)
    # delete duplicate rows using entry_number as unique key
    conn.execute(
        """
        DELETE FROM results
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM results
            GROUP BY entry_number
        )
        """
    )
    conn.close()


# delete csv files in csv_downloads folder
def delete_csv():
    dir_path = os.path.dirname(os.path.realpath(__file__))

    path = os.path.join(dir_path, "scrape_data/csv_downloads")
    files = os.listdir(path)
    for file in files:
        os.remove(os.path.join(path, file))


def load_url(url, timeout):
    ans = requests.head(url, timeout=timeout)
    return ans


def get_uil_results():

    # delete_csv()

    # Get the absolute path of the directory the script is in
    dir_path = os.path.dirname(os.path.realpath(__file__))

    service = Service()
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": os.path.join(
            dir_path, "./scrape_data/csv_downloads"
        ),
    }
    chrome_options.add_experimental_option("prefs", prefs)
    # driver = webdriver.Chrome("F:\Downloads\chromedriver_win32\chromedriver.exe", options=chrome_options)

    driver = webdriver.Chrome(service=service, options=chrome_options)

    # create log of urls if not exists
    try:
        with open("scrape_data/urls.txt", "r") as f:
            urls = f.read().splitlines()
    except:
        with open("scrape_data/urls.txt", "w") as f:
            f.write("")
        urls = []
    empty_in_a_row = 0
    for i in range(6500, 100000):
        if empty_in_a_row < 100:

            url = f"https://www.texasmusicforms.com/csrrptUILpublic.asp?cn={i}&get=go"
            if url in urls:
                print(f"already scraped {i}")
                continue

            driver.get(url)
            try:
                driver.find_element(By.XPATH, '//*[@id="export-csv"]').click()
                print(f"contest found at {i}")
                empty_in_a_row = 0
                with open("scrape_data/urls.txt", "a") as f:
                    f.write(url + "\n")

                time.sleep(1)

            except:
                print(f"no contest at {i}")
                empty_in_a_row += 1
                continue


# call the funtion and record time

if __name__ == "__main__":
    start_time = time.time()
    # get_uil_results()
    loop_csv()
    print("--- %s seconds ---" % (time.time() - start_time))
