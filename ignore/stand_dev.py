import pandas as pd
import os
import sqlite3

df = pd.read_csv('all_results_manip.csv', encoding='latin-1')

# create a column that finds the standard deviation of the scores by year
df['Concert Standard Deviation'] = df.groupby('Year')['Concert Average'].transform('std')
df['Sight Reading Standard Deviation'] = df.groupby('Year')['Sight Reading Average'].transform('std')
df['Total Standard Deviation'] = df.groupby('Year')['Total Score'].transform('std')

# create a column that finds the mean of the scores by year
df['Concert Mean'] = df.groupby('Year')['Concert Average'].transform('mean')
df['Sight Reading Mean'] = df.groupby('Year')['Sight Reading Average'].transform('mean')
df['Total Mean'] = df.groupby('Year')['Total Score'].transform('mean')

# create a column that is how many standard deviations away from the mean the score is
df['Concert Standard Deviations Away From Mean'] = ((df['Concert Average'] - df['Concert Mean']) / df['Concert Standard Deviation']).round(2)
df['Sight Reading Standard Deviations Away From Mean'] = ((df['Sight Reading Average'] - df['Sight Reading Mean']) / df['Sight Reading Standard Deviation']).round(2)
df['Total Standard Deviations Away From Mean'] = ((df['Total Score'] - df['Total Mean']) / df['Total Standard Deviation']).round(2)

# save to csv
df.to_csv('all_results_manip_std_dev.csv', index=False)
