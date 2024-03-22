import pandas as pd
import os
import sqlite3

df1 = pd.DataFrame()


# loops of csv files in csv_downloads folder
def loop_csv():
    path = 'Scrape_data/csv_downloads'
    files = os.listdir(path)
    i = 0
    for file in files:
        if i == 0:
            df2 = pd.read_csv(
                f"{path}/{file}", header=2, encoding='iso-8859-1')
            df_concat = pd.concat([df1, df2], ignore_index=True)
            print(len(df_concat))

            i += 1
        else:
            try:
                df2 = pd.read_csv(
                    f"{path}/{file}", header=2, encoding='iso-8859-1')
                df_concat = pd.concat([df_concat, df2], ignore_index=True)
                print(len(df_concat))
            except:
                pass
            
    df_concat.to_csv('all_results.csv', index=False)
    # update uil.db to this new df_concat
    conn = sqlite3.connect('uil.db')
    df_concat.to_sql('results', conn, if_exists='replace', index=False)
    conn.close()



loop_csv()
