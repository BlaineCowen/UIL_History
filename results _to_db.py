import pandas as pd
import os
import sqlite3

df = pd.read_csv('all_results_with_codes.csv')
df.fillna('', inplace=True)
df["all codes"] = df["code 1"] + df["code 2"] + df["code 3"]
df["all codes"] = df["all codes"].str.replace('nan', '')

# results['expected concert mean'] is the average concert score grouped by year
df['expected concert mean'] = df.groupby(
    'Year')['Concert Average'].transform('mean')

df['score above expected'] = df['expected concert mean'] - df['Concert Average']

#  send to sqlite
conn = sqlite3.connect('pml.db')
df.to_sql('results', conn, if_exists='replace', index=False)
# close connection
conn.close()
