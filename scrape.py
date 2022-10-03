##disable-gpu

from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
import sqlite3
import re
import time
import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.common import exceptions
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import unicodedata
from selenium.webdriver.chrome.options import Options
import sqlite3
import os
import pandas as pd

# delete csv files in csv_downloads folder
def delete_csv():
    path = 'csv_downloads'
    files = os.listdir(path)
    for file in files:
        os.remove(os.path.join(path, file))

delete_csv()


chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--log-level=3')
# # change webdriver path to csv_downloads
prefs = {"download.default_directory": r"F:\Documents\projects\UIL_RESULTS_REWRITE\csv_downloads\\"}
chrome_options.add_experimental_option("prefs", prefs)
# driver = webdriver.Chrome("F:\Downloads\chromedriver_win32\chromedriver.exe", options=chrome_options)

driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

out = []
CONNECTIONS = 100
TIMEOUT = 5

def load_url(url, timeout):
    ans = requests.head(url, timeout=timeout)
    return ans


def get_uil_results():
    data = pd.DataFrame()
    url = 'https://www.texasmusicforms.com/csrrptUILpublic.asp'
    driver.get(url)
    year = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[1]/select'))
    year_options = year.options

    # loop over years
    for i in range(len(year_options)):
        # skip first option
        if i == 0:
            continue
        year = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[1]/select'))
        try:
            if i >= len(year_options):
                continue
            year.select_by_index(i)
        except exceptions.NoSuchElementException:
            continue
        year_num = year.options[i].text
        print(year_num)
        region = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[2]/select'))

        region_options = region.options
        # loop over regions
        for j in range(len(region_options) - 2):
            if j > 0:
                try:
                    year = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[1]/select'))   
                except exceptions.NoSuchElementException:
                    print("No such element ", i)
                    driver.get(url)
                    j -= 1
                    continue
                year.select_by_index(i)
                year_num = year.options[i].text
                print(year_num, 'region', j)
                region = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[2]/select'))             
            region.select_by_index(j + 1)
            region_num = region.options[j + 1].text
            event = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[3]/select'))
            event.select_by_visible_text('Chorus')
            time.sleep(.5)
            try:
                contest = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form[2]/div/select'))
            except exceptions.NoSuchElementException:
                print("No contest ", year_num, region_num)
                driver.get(url)
                continue
            contest_options = contest.options
            # loop over contests
            for k in range(len(contest_options)-1):
                year = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[1]/select'))
                year.select_by_index(i)
                region = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[2]/select'))             

                region.select_by_index(j + 1)
                event = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[3]/select'))

                event.select_by_visible_text('Chorus')
                contest = Select(driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form[2]/div/select'))          
                contest.select_by_index(k + 1)
                try:
                    results = driver.find_element(By.XPATH, '//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form[2]/button[1]/b').click()
                except:
                    print("No results button ", year_num, region_num)
                    driver.get(url)
                    continue

                # click export button
                try:
                    export = driver.find_element(By.XPATH, '//*[@id="export-csv"]').click()
                except exceptions.NoSuchElementException:
                    driver.get(url)
                    continue
                # press back button
                driver.get(url)



# call the funtion and record time

if __name__ == '__main__':
    start_time = time.time()
    get_uil_results()
    print("--- %s seconds ---" % (time.time() - start_time))
