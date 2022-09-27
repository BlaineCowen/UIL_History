import logging, sys
import requests
from bs4 import BeautifulSoup
from requests import session
from pprint import pprint
import concurrent.futures as cf
from requests_futures.sessions import FuturesSession
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import sqlite3
import pandas as pd
import time
import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.common import exceptions
from webdriver_manager.chrome import ChromeDriverManager
import asyncio
import aiohttp
import time

# driver = webdriver.Chrome(ChromeDriverManager().install())

conn = sqlite3.connect('trading_card_info.db')
c = conn.cursor()


def get_sets():  
    set_list = []  
    set_url_list = []
    url = 'https://www.sportscardspro.com/'
    header = 'brand/baseball-cards'
    result = requests.get(url + header)
    soup = BeautifulSoup(result.text, 'html.parser')
    table = soup.find(class_="home-box all")
    i = 0
    for set in range(10):
        set = table.find('a')
        set_name = set.text
        header = 'console/'
        set_url = url + header + set['href']
        set_list.append(set_name)
        set_url_list.append(set_url)
        i += 1
    return set_list, set_url_list

def sales_month(string):
    num = int(''.join(filter(str.isdigit, string)))
    date = string.split()
    if date[-1] == 'year':
        return num/12
    elif date[-1] == 'month':
        return num
    elif date[-1] == 'week':
        return num*4.3
    elif date[-1] == 'day':
        return num*30.4
    else: 
        print('error sales per month ', string)

def sports_get_card_info():
    c.execute("""SELECT * FROM cardID WHERE card_type = 'baseball'""")
    card_list = c.fetchmany(100)
    for row in card_list:
        set = str(row[3]).replace(' ', '-')
        cardName = row[1].replace(' ', '-').replace('[', '').replace(']', '').replace('.', '').replace('?', '').replace('/', '').replace(':', '').replace('!', '').replace('--','-')
        cardNumb = row[2]
        #checks in cardnumb is a string
        if isinstance(cardNumb, str):
            card_text = cardName + '-' + cardNumb + '-' + set
        else:
            card_text = cardName + '-' + set
        url = row[5]
        # url = 'https://www.pricecharting.com/game/baseball-cards-1986-quaker-oats/willie-mcgee-1'
        print(url)
        try: 
            result = requests.get(url)
        except:
            print('error' + url)
            return
        soup = BeautifulSoup(result.text, 'html.parser')
        table = soup.find(id="price_data")
        try:
            ungraded_price = table.find(id="used_price")
        except:
            print('table error')
            continue
        ungraded_price = ungraded_price.find('span').text.strip().replace('$', '').replace(',', '')
        if ungraded_price == 'N/A':
            ungraded_price = 0
        else:
            ungraded_price = float(ungraded_price)
        psa7 = table.find(id="complete_price")
        psa7 = psa7.find('span').text.strip().replace('$', '').replace(',', '')
        if psa7 == 'N/A':
            psa7 = 0
        else: 
            psa7 = float(psa7)
        psa8 = table.find(id="new_price")
        psa8 = psa8.find('span').text.strip().replace('$', '').replace(',', '')
        if psa8 == 'N/A':
            psa8 = 0
        else:
            psa8 = float(psa8)
        psa9 = table.find(id="graded_price")
        psa9 = psa9.find('span').text.strip().replace('$', '').replace(',', '')
        if psa9 == 'N/A':
            psa9 = 0
        else:
            psa9 = float(psa9)
        bgs95 = table.find(id="box_only_price")
        bgs95 = bgs95.find('span').text.strip().replace('$', '').replace(',', '')
        if bgs95 == 'N/A':
            bgs95 = 0
        else:
            bgs95 = float(bgs95)
        psa10 = table.find(id="manual_only_price")
        psa10 = psa10.find('span').text.strip().replace('$', '').replace(',', '')
        if psa10 == 'N/A':
            psa10 = 0
        else:
            psa10 = float(psa10)
        volume_table = table.find(class_="sales_volume")
        vol_list = []
        for col in volume_table.find_all('a'):
            sale_vol = col.text.strip()
            sale_vol = sales_month(sale_vol)
            vol_list.append(sale_vol)
        ungraded_sales = vol_list[0]
        psa7_sales = vol_list[1]
        psa8_sales = vol_list[2]
        psa9_sales = vol_list[3]
        bgs95_sales = vol_list[4]
        psa10_sales = vol_list[5]
        total_sales = sum(vol_list)
        price_data = [ungraded_price, psa7, psa8, psa9, bgs95, psa10, str(ungraded_sales) + 'a week',
                        str(psa7_sales) + 'a week', str(psa8_sales) + 'a week', str(psa9_sales) + 'a week',
                        str(bgs95_sales) + 'a week', str(psa10_sales) + 'a week']

        c.execute("""INSERT INTO cardPriceData VALUES(?, datetime('now'),?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
        (row[0], ungraded_price, psa7, psa8, psa9, bgs95, psa10, card_text, ungraded_sales, psa7_sales, psa8_sales, psa9_sales, bgs95_sales, psa10_sales, total_sales)) 
        conn.commit()       

    conn.close()


# def scroll_down():
#     SCROLL_PAUSE_TIME = 2

#     # Get scroll height
#     last_height = driver.execute_script("return document.body.scrollHeight")

#     while True:
#         # Scroll down to bottom
#         driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

#         # Wait to load page
#         time.sleep(SCROLL_PAUSE_TIME)

#         # Calculate new scroll height and compare with last scroll height
#         new_height = driver.execute_script("return document.body.scrollHeight")
#         if new_height == last_height:
#             break
#         last_height = new_height

def load_more():
    path = '//*[@id="games_table"]/tfoot/tr/td/form'
    button = driver.find_element_by_xpath(path)
    try:
        while button.is_displayed():
            a = driver.find_elements_by_class_name('used_price')
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            if '$' not in a[-1].text:
                    print('card is shit')
                    break
    except exceptions.StaleElementReferenceException as e:
        print(e)
        return
        



    # while button.is_displayed():
    #     driver.execute_script("window.scrollTo(0, document.body.scrollHeight")
    #     # check to see if the button if false
    #     if not button.is_displayed():
    #         print('load more button is not displayed')
    #         break


# def test_req():
#     def load_more():
#         path = '//*[@id="games_table"]/tfoot/tr/td/form'
#         button = driver.find_element_by_xpath(path)
#         try:
#             while button.is_displayed():
#                 a = driver.find_elements_by_class_name('used_price')
#                 driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#                 if '$' not in a[-1].text:
#                         print('card is shit')
#                         break
#         except exceptions.StaleElementReferenceException as e:
#             print(e)
#             return
#     url = 'https://www.sportscardspro.com/console/baseball-cards-2020-topps'
#     driver.get(url)
#     #use scroll_down on the selenium driver
#     load_more()
#     soup = BeautifulSoup(driver.page_source, 'html.parser')
#     driver.quit
#     table = soup.find('tbody')
#     for row in table.find_all('tr'):
#         name = row.find('a').text.strip()
#         print(name)

async def get(url, session):
    try:
        async with session.get(url=url) as response:
            resp = await response.page_source()
            print("Successfully got url {} with resp of length {}.".format(url))
            return resp

    except Exception as e:
        print("Unable to get url {} due to {}.".format(url, e.__class__))


async def main(urls):
    async with aiohttp.ClientSession() as session:
        ret = await asyncio.gather(*[get(url, session) for url in urls])
    print("Finalized all. Return is a list of len {} outputs.".format(len(ret)))

def async_card_data():
    c.execute("""SELECT * FROM cardID WHERE card_type = 'baseball'""")
    card_list = c.fetchmany(100)
    urls = []
    for i in range(len(card_list)):
        urls.append(card_list[i][5])
    asyncio.run(main(urls))

    # for future in as_completed(futures):
    #     row = card_list[i]
    #     resp = future.result()
    #     soup = BeautifulSoup(resp.text, 'html.parser')
    #     set = str(row[3]).replace(' ', '-')
    #     cardName = row[1].replace(' ', '-').replace('[', '').replace(']', '').replace('.', '').replace('?', '').replace('/', '').replace(':', '').replace('!', '').replace('--','-')
    #     cardNumb = row[2]
    #     #checks in cardnumb is a string
    #     if isinstance(cardNumb, str):
    #         card_text = cardName + '-' + cardNumb + '-' + set
    #     else:
    #         card_text = cardName + '-' + set
    #     url = row[5]
    #     # url = 'https://www.pricecharting.com/game/baseball-cards-1986-quaker-oats/willie-mcgee-1'
    #     print(url)
    #     try: 
    #         result = requests.get(url)
    #     except:
    #         print('error' + url)
    #         return
    #     soup = BeautifulSoup(result.text, 'html.parser')
    #     table = soup.find(id="price_data")
    #     try:
    #         ungraded_price = table.find(id="used_price")
    #     except:
    #         print('table error')
    #         continue
    #     ungraded_price = ungraded_price.find('span').text.strip().replace('$', '').replace(',', '')
    #     if ungraded_price == 'N/A':
    #         ungraded_price = 0
    #     else:
    #         ungraded_price = float(ungraded_price)
    #     psa7 = table.find(id="complete_price")
    #     psa7 = psa7.find('span').text.strip().replace('$', '').replace(',', '')
    #     if psa7 == 'N/A':
    #         psa7 = 0
    #     else: 
    #         psa7 = float(psa7)
    #     psa8 = table.find(id="new_price")
    #     psa8 = psa8.find('span').text.strip().replace('$', '').replace(',', '')
    #     if psa8 == 'N/A':
    #         psa8 = 0
    #     else:
    #         psa8 = float(psa8)
    #     psa9 = table.find(id="graded_price")
    #     psa9 = psa9.find('span').text.strip().replace('$', '').replace(',', '')
    #     if psa9 == 'N/A':
    #         psa9 = 0
    #     else:
    #         psa9 = float(psa9)
    #     bgs95 = table.find(id="box_only_price")
    #     bgs95 = bgs95.find('span').text.strip().replace('$', '').replace(',', '')
    #     if bgs95 == 'N/A':
    #         bgs95 = 0
    #     else:
    #         bgs95 = float(bgs95)
    #     psa10 = table.find(id="manual_only_price")
    #     psa10 = psa10.find('span').text.strip().replace('$', '').replace(',', '')
    #     if psa10 == 'N/A':
    #         psa10 = 0
    #     else:
    #         psa10 = float(psa10)
    #     volume_table = table.find(class_="sales_volume")
    #     vol_list = []
    #     for col in volume_table.find_all('a'):
    #         sale_vol = col.text.strip()
    #         sale_vol = sales_month(sale_vol)
    #         vol_list.append(sale_vol)
    #     ungraded_sales = vol_list[0]
    #     psa7_sales = vol_list[1]
    #     psa8_sales = vol_list[2]
    #     psa9_sales = vol_list[3]
    #     bgs95_sales = vol_list[4]
    #     psa10_sales = vol_list[5]
    #     total_sales = sum(vol_list)
    #     price_data = [ungraded_price, psa7, psa8, psa9, bgs95, psa10, str(ungraded_sales) + 'a month',
    #                     str(psa7_sales) + 'a month', str(psa8_sales) + 'a month', str(psa9_sales) + 'a month',
    #                     str(bgs95_sales) + 'a month', str(psa10_sales) + 'a month']

    #     c.execute("""INSERT INTO card_price_test VALUES(?, datetime('now'),?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
    #     (row[0], ungraded_price, psa7, psa8, psa9, bgs95, psa10, card_text, ungraded_sales, psa7_sales, psa8_sales, psa9_sales, bgs95_sales, psa10_sales, total_sales)) 
    #     conn.commit()
    #     i += 1       

    # conn.close()





    

if __name__ == "__main__":
    start = datetime.datetime.now()
    async_card_data()
    end = datetime.datetime.now()
    total_time = end - start
    print(f'time = {total_time}')
