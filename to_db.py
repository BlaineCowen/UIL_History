from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
import sqlite3
import re
from concurrent.futures import ThreadPoolExecutor
import time
import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.common import exceptions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
import unicodedata




out = []
CONNECTIONS = 100
TIMEOUT = 5

def load_url(url, timeout):
    ans = requests.head(url, timeout=timeout)
    return ans


def get_uil_results():
    driver = webdriver.Chrome(ChromeDriverManager().install())
    data = pd.DataFrame()
    url = 'https://www.texasmusicforms.com/csrrptUILpublic.asp'
    driver.get(url)
    year = Select(driver.find_element_by_xpath('//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[1]/select'))
    year.select_by_visible_text('2021')
    region = Select(driver.find_element_by_xpath('//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[2]/select'))
    region.select_by_visible_text('Region 1')
    event = Select(driver.find_element_by_xpath('//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form/div[3]/select'))
    event.select_by_visible_text('Chorus')
    contest = Select(driver.find_element_by_xpath('//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form[2]/div/select'))
    contest.select_by_index(1)
    results = driver.find_element_by_xpath('//*[@id="content-wrapper-div"]/div/div/div/div/div/div[1]/div/div/form[2]/button[1]/b').click()
    load = Select(driver.find_element_by_xpath('//*[@id="DataTables_Table_0_length"]/label/select'))
    load.select_by_visible_text('All')
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    table = soup.find('table', id='DataTables_Table_0')
    table = table.find('tbody')
    for row in table.find_all('tr'):
        school = row.find_all('td')[0]
        school = unicodedata.normalize('NFKD', school.text).encode('ascii', 'ignore').decode('utf-8').strip()
        school = school.split(' ')[2:]
        print(school)

        school = ' '.join(school).strip()
        print(school)
        chorus = row.find_all('td')[0].text.split('-', 1)[1:]
        chorus = ' '.join(chorus).strip().replace("\n", " ")
        chorus = chorus.split(' ', 1)[0:1]
        chorus = ' '.join(chorus).strip().replace("\n", " ")
        conf = row.find_all('td')[1].text.split()[0]
        score_1 = row.find_all('td')[2].text.strip()
        score_2 = row.find_all('td')[3].text.strip()
        score_3 = row.find_all('td')[4].text.strip()
        score_final = row.find_all('td')[5].text.strip()
        sr_1 = row.find_all('td')[6].text.strip()
        sr_2 = row.find_all('td')[7].text.strip()
        sr_3 = row.find_all('td')[8].text.strip()
        sr_final = row.find_all('td')[9].text.strip()
        songs = unicodedata.normalize('NFKD', row.find_all('td')[11].text.strip()).encode('ascii', 'ignore').decode('utf-8').strip()
        songs = re.split('\(.*?\)', songs)
        songs = [x.strip(' ') for x in songs]
        songs = list(filter(None, songs))
        if len(songs) != 3:
            print("ERRROORRRRRRR", len(songs), "in row", row)
        data = data.append({'school': school, 'chorus': chorus, 'conf': conf, 'song1': songs[0], 
        'song2': songs[1], 'song3': songs[2], 'score_1': score_1, 'score_2': score_2, 'score_3': score_3, 'score_final': score_final, 'sr_1': sr_1, 'sr_2': sr_2, 'sr_3': sr_3, 'sr_final': sr_final, }, ignore_index=True)
    data.to_csv('uil_results.csv', encoding='utf-8')
    return data

def parse_songs():
    possible_voicing = ['SA', 'SSA', 'SSAA', 'SAA', 'SSSAAA', 'SAB', 'SSAB'
    'SATB', 'SSAATTBB', 'TB', 'TTB', 'TTBB', 'TTBBB', 'TTBBBB', 'TTBBBBB', '2 part', '2-part'
    '2-Part' '2 Part', '3-part',' 3 Part', 'SAT', 'sing version 2', 'unison', 'SA ONLY']
    perf_option = ['a cappella', 'accompaniment', 'solo', 'soloist', 'accomp',
    'opt', 'accomp opt', 'w/']
    data = pd.read_csv('uil_results.csv', encoding='utf-8')

    data.insert(0, 'song1', '')
    data.insert(0, 'song2', '')
    data.insert(0, 'song3', '')
    for i in range(len(data)):
        songs = data['songs'][i]
        songs = unicodedata.normalize('NFKD', songs).encode('ascii', 'ignore').decode('utf-8').strip()
        songs = re.split('\(.*?\)', songs)
        songs = [x.strip(' ') for x in songs]
        songs = list(filter(None, songs))
        if len(songs) != 3:
            print("ERRROORRRRRRR", len(songs), "in row", i)
        data['song1'][i] = songs[0]
        data['song2'][i] = songs[1]
        data['song3'][i] = songs[2]

    data.to_csv('uil_results2.csv')




# this function will take the card_list and turn it into a database

# this function will take a row from the database and return the values

conn = sqlite3.connect('trading_card_info.db')
c = conn.cursor()
# print count the number of rows in table

set_list = []
set_url_list = []
card_list = []




def get_sets():
    
    url = 'https://www.sportscardspro.com/'
    header = 'category/baseball-cards'
    result = requests.get(url + header)
    soup = BeautifulSoup(result.text, 'html.parser')
    table = soup.find(class_="home-box all")
    for set in table.find_all('li'):
        set = set.find('a')
        set_name = set.text
        header = 'console/'
        set_url = url + header + set['href']
        print(set_url)
        set_list.append(set_name)
        set_url_list.append(set_url)
    return set_list, set_url_list


def load_more():
    path = '//*[@id="games_table"]/tfoot/tr/td/form'
    try: button = driver.find_element_by_xpath(path)
    except exceptions.NoSuchElementException:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        print('no more to load')
        return
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
# print count the number of rows in table

# this function will replace the url in cardID with the url from the card_list



def get_card_names():
    # get_sets()
    missing_sets = get_missing_sets()
    card_list = pd.DataFrame()
    for i in range(len(set_list)):
        if set_list[i] in missing_sets:
            url = set_url_list[i]
            set_name = set_list[i]
    # this loop will pull the card names and append them to card_list
            print(set_name)
            try: 

                driver.get(url)
                #use scroll_down on the selenium driver
                load_more()
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                driver.quit
                table = soup.find('tbody')
                # while the rest of the function is running, pull up the next url
                
                for item in table.find_all('tr'):
                    item_info = item.find('td').text.strip()
                    item_name = item_info.split('#')[0]
                    url = item.find('a')
                    url = 'https://www.pricecharting.com' + url['href']
                    try: 
                        item_num = item_info.split('#')[1]
                    except IndexError:
                        item_num = "N/A"
                    card_list = card_list.append({'name': item_name, 'num': item_num, 'set': set_name, 'card_type': 'baseball',  'url': url}, ignore_index=True)
                    c.execute("""INSERT OR REPLACE INTO cardIDtest (card_name, card_number, set_name, card_type, url) VALUES (?, ?, ?, ?, ?)""", (item_name, item_num, set_name, 'baseball', url))
            except Exception as e:
                print("Oops!", e,  e.__class__, "occurred.")
                print('error' + url)
                continue
    print(card_list)
# get_card_names()


def sales_week(string):
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
        print('error sales per week ', string)


def sports_get_card_info():
    c.execute("""SELECT * FROM cardID WHERE card_type = 'baseball'""")
    card_list = c.fetchall()
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
            sale_vol = sales_week(sale_vol)
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
# sports_get_card_info()



def pkmn_get_card_info():
    c.execute("""SELECT * FROM cardID WHERE card_type = 'pokemon'""")
    card_list = c.fetchall()
    for row in card_list:
        print(row)
        set = row[3].replace(' ', '-')
        cardName = row[1].replace(' ', '-').replace('[', '').replace(']', '').replace('.', '').replace('?', '').replace('/', '').replace(':', '').replace('!', '').replace('--','-')
        cardNumb = row[2]
        #checks in cardnumb is a string
        if isinstance(cardNumb, str):
            card_text = cardName + '-' + cardNumb + '-' + set
        else:
            card_text = cardName + '-' + set
        url = row[5]
        print(url)
        try: 
            result = requests.get(url)
        except:
            print('error' + url)
            return
        soup = BeautifulSoup(result.text, 'html.parser')
        table = soup.find(id="price_data")
        ungraded_price = table.find(id="used_price")
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
        price_data = [ungraded_price, psa7, psa8, psa9, bgs95, psa10]
        print(price_data)

        c.execute("""INSERT INTO cardPriceData VALUES(?, datetime('now'),?,?,?,?,?,?,?)""", (row[0], ungraded_price, psa7, psa8, psa9, bgs95, psa10, card_text))
        conn.commit()    
    
    conn.close()

# get_card_info()

def get_missing_sets():
    all_sets = get_sets()[0]
    missing_sets = []
    for i in all_sets:
        c.execute("""SELECT * FROM cardIDtest WHERE set_name = ?""", (i,))
        card_list = c.fetchall()
        if len(card_list) == 0:
            missing_sets.append(i)
    print(missing_sets)
    return missing_sets
    # for i in all_sets:
    #     if i not in card_list:
    #         missing_sets.append(i)
    #         print(i)    
    #         # save missing sets as text file
    # with open('missing_sets.txt', 'w') as f:
    #     for item in missing_sets:
    #         f.write("%s\n" % item)




if __name__ == '__main__':
    s = requests.Session()
    start = datetime.datetime.now()
    get_uil_results()
    # parse_songs()
    conn.commit()
    conn.close()
    finish = datetime.datetime.now() - start
    print(finish)
