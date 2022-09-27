import asyncio
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
import datetime
import aiohttp
import lxml.html
import sqlite3
import time

conn = sqlite3.connect('trading_card_info.db')
c = conn.cursor()

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

def process_page(html):
    try:
       soup = BeautifulSoup(html, 'html.parser')

    except:
        price_data = 'error parsing page'
    table = soup.find(id="price_data")
    try:
        ungraded_price = table.find(id="used_price")
    except:
        print('table error')
        return
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
    price_data = [ungraded_price, psa7, psa8, psa9, bgs95, psa10, ungraded_sales,
                 psa7_sales, psa8_sales, psa9_sales, bgs95_sales, psa10_sales, total_sales]
   
    
    return price_data


async def fetch_page(url, session):
    '''Meant for IO-bound workload'''
    async with session.get(url, timeout = 20) as res:
        return await res.text()



async def process(url, session, pool):
    html = await fetch_page(url, session)
    return await asyncio.wrap_future(pool.submit(process_page, html))



async def dispatch(urls):
    pool = ProcessPoolExecutor()
    async with aiohttp.ClientSession() as session:
        try:
            coros = (process(url, session, pool) for url in urls)
            return await asyncio.gather(*coros)
        except asyncio.CancelledError:
            print('timeout')
            return


def main():
    urls = []
    c.execute("""SELECT * FROM cardID WHERE card_type = 'baseball'""")
    card_list = c.fetchall()
    for i in range(len(card_list)):
        urls.append(card_list[i][5])
    i = 0
    price_data = asyncio.get_event_loop().run_until_complete(dispatch(urls))
    for row in card_list:
        if isinstance(price_data[i], str):
            print('error', row[0])
            continue
        set = str(row[3]).replace(' ', '-')
        cardNumb = row[2]
        cardName = row[1] + " " + cardNumb + " " + row[3]
        cardName = cardName.replace(' ', '-').replace('[', '').replace(']', '').replace('.', '').replace('?', '').replace('/', '').replace(':', '').replace('!', '').replace('--','-')
        id = row[0]
        if isinstance(cardNumb, str):
            card_text = cardName + '-' + cardNumb + '-' + set
        else:
            card_text = cardName + '-' + set
        url = row[5]
        print(url)
        c.execute("""INSERT INTO card_price_test VALUES(?, datetime('now'),?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", 
            (id, price_data[i][0], price_data[i][1], price_data[i][2], price_data[i][3], price_data[i][4], price_data[i][5], cardName,
             price_data[i][6], price_data[i][7], price_data[i][8], price_data[i][9], price_data[i][10], price_data[i][11], price_data[i][12]))
        i += 1
        conn.commit()
    conn.close()

if __name__ == '__main__':
    start = datetime.datetime.now()
    main()
    end = datetime.datetime.now()
    total_time = end - start
    print(f'time = {total_time}')