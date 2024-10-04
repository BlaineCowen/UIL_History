import pandas as pd
import os
import sqlite3

df = pd.read_csv('all_results.csv', encoding='latin-1')

# # drop duplicate rows
# df.drop_duplicates(inplace=True)

# drop rows with no scores
df.dropna(subset=['Concert Score 1'], inplace=True)


# if any rows have 'DNA' in them, drop them
for col in df.columns:
    df = df[~df[col].astype(str).str.contains('DNA')]
    df = df[~df[col].astype(str).str.contains('DQ')]

# # if a score is 0, change to 1
# df['Concert Score 1'] = df['Concert Score 1'].replace(0, 1)
# df['Concert Score 2'] = df['Concert Score 2'].replace(0, 1)
# df['Concert Score 3'] = df['Concert Score 3'].replace(0, 1)
# df['Sight Reading Score 1'] = df['Sight Reading Score 1'].replace(0, 1)
# df['Sight Reading Score 2'] = df['Sight Reading Score 2'].replace(0, 1)
# df['Sight Reading Score 3'] = df['Sight Reading Score 3'].replace(0, 1)


# create average scores column
df['Concert Average'] = ((df['Concert Score 1'].astype(float) + df['Concert Score 2'].astype(float) + df['Concert Score 3'].astype(float)) / 3).round(2)
df['Sight Reading Average'] = ((df['Sight Reading Score 1'].astype(float) + df['Sight Reading Score 2'].astype(float) + df['Sight Reading Score 3'].astype(float)) / 3).round(2)

# create total scores column
df['Total Score'] = df['Concert Average'] + df['Sight Reading Average']

# create new column for year
df['Year'] = df['Contest Date'].str[-4:]

print(df.head())
df.to_csv('all_results_manip.csv', index=False)
