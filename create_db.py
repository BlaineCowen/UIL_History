import pandas as pd
import os
import sqlite3

df1 = pd.DataFrame()


# loops of csv files in csv_downloads folder
def loop_csv():
    path = 'csv_downloads'
    files = os.listdir(path)
    i = 0
    for file in files:
        if i == 0:
            df2 = pd.read_csv(
                f"csv_downloads/{file}", header=2, encoding='iso-8859-1')
            df_concat = pd.concat([df1, df2], ignore_index=True)
            print(len(df_concat))

            i += 1
        else:
            df2 = pd.read_csv(
                f"csv_downloads/{file}", header=2, encoding='iso-8859-1')
            df_concat = pd.concat([df_concat, df2], ignore_index=True)
            print(len(df_concat))
    df_concat.to_csv('all_results2.csv', index=False)


loop_csv()
