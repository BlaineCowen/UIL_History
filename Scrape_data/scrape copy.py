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
    # if all results.csv does not exist, create it
    if not os.path.exists("all_results.csv"):
        # columns Contest Date	Event	Region	School	TEA Code	City	Director	Additional Director	Accompanist	Conference	Classification	Non-Varsity Group	Entry Number	Title 1	Composer 1	Title 2	Composer 2	Title 3	Composer 3	Concert Judge	Concert Judge	Concert Judge	Concert Score 1	Concert Score 2	Concert Score 3	Concert Final Score	Sight Reading Judge	Sight Reading Judge	Sight Reading Judge	Sight Reading Score 1	Sight Reading Score 2	Sight Reading Score 3	Sight Reading Final Score	Award

        df1 = pd.DataFrame(
            columns=[
                "Contest Date",
                "Event",
                "Region",
                "School",
                "TEA Code",
                "City",
                "Director",
                "Additional Director",
                "Accompanist",
                "Conference",
                "Classification",
                "Non-Varsity Group",
                "Entry Number",
                "Title 1",
                "Composer 1",
                "Title 2",
                "Composer 2",
                "Title 3",
                "Composer 3",
                "Concert Judge",
                "Concert Judge",
                "Concert Judge",
                "Concert Score 1",
                "Concert Score 2",
                "Concert Score 3",
                "Concert Final Score",
                "Sight Reading Judge",
                "Sight Reading Judge",
                "Sight Reading Judge",
                "Sight Reading Score 1",
                "Sight Reading Score 2",
                "Sight Reading Score 3",
                "Sight Reading Final Score",
                "Award",
            ]
        )
        df1.to_csv("all_results.csv", index=False)
    # read all_results.csv

    df1 = pd.read_csv("all_results.csv", encoding="iso-8859-1", low_memory=False)
    path = "scrape_data/csv_downloads"

    files = os.listdir(path)
    for file in files:
        df2 = pd.read_csv(f"{path}/{file}", header=2, encoding="iso-8859-1")

        # if first entry number is in df1, skip file
        if df2["Entry Number"].iloc[0] in df1["Entry Number"].values:
            continue

        df1 = pd.concat([df1, df2], ignore_index=True)
        print(len(df1))

    # remove duplicates
    df1 = df1.drop_duplicates()

    df1.to_csv("all_results.csv", index=False)
    # update uil.db to this new df_concat
    conn = sqlite3.connect("uil.db")
    df1.to_sql("results", conn, if_exists="replace", index=False)
    conn.close()


# delete csv files in csv_downloads folder
def delete_csv():
    dir_path = os.path.dirname(os.path.realpath(__file__))

    path = os.path.join(dir_path, "csv_downloads")
    files = os.listdir(path)
    for file in files:
        os.remove(os.path.join(path, file))


def load_url(url, timeout):
    ans = requests.head(url, timeout=timeout)
    return ans


def get_uil_results():

    # Get the absolute path of the directory the script is in
    dir_path = os.path.dirname(os.path.realpath(__file__))

    service = Service()
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": os.path.join(dir_path, "./csv_downloads"),
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
    for i in range(7000, 10000):

        url = f"https://www.texasmusicforms.com/csrrptUILpublic.asp?cn={i}&get=go"

        if url in urls:
            print(f"already scraped {i}")
            continue

        # write url to log

        driver.get(url)
        try:
            driver.find_element(By.XPATH, '//*[@id="export-csv"]').click()
            print(f"contest found at {i}")
            with open("scrape_data/urls.txt", "a") as f:
                f.write(url + "\n")

        except:
            print(f"no contest at {i}")
            continue
            pass

        time.sleep(1)


# call the funtion and record time

if __name__ == "__main__":
    start_time = time.time()
    # get_uil_results()
    loop_csv()
    print("--- %s seconds ---" % (time.time() - start_time))
