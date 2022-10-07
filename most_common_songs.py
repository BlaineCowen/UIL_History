import pandas as pd
import os
import sqlite3

pml_matches = pd.read_csv('pml.csv')

results = pd.read_csv('all_results_with_codes.csv')
results.fillna('', inplace=True)
results["all codes"] = results["code 1"] + \
    results["code 2"] + results["code 3"]

# results['expected concert mean'] is the average concert score grouped by year
results['expected concert mean'] = results.groupby(
    'Year')['Concert Average'].transform('mean')

results['score above expected'] = results['expected concert mean'] - \
    results['Concert Average']

pml_songs_performed = results[[
    'code 1', 'code 2', 'code 3']].stack().value_counts()

# change column name
pml_songs_performed = pml_songs_performed.rename('count')

# convert to dataframe
pml_songs_performed = pml_songs_performed.to_frame()


pml_songs_performed['title'] = ''
pml_songs_performed['composer'] = ''
pml_songs_performed['event'] = ''
pml_songs_performed['specification'] = ''
pml_songs_performed['grade'] = ''

for row in pml_songs_performed.iterrows():
    code = row[0]
    try:
        title = pml_matches[pml_matches['Code'] == code]['Title'].tolist()[0]
        composer = pml_matches[pml_matches['Code'] == code]['Composer'].tolist()[
            0]
        event = pml_matches[pml_matches['Code'] == code]['Event Name'].tolist()[
            0]
        specification = pml_matches[pml_matches['Code'] == code]['Specification'].tolist()[
            0]
        grade = pml_matches[pml_matches['Code'] == code]['Grade'].tolist()[
            0]

        pml_songs_performed.loc[code, 'title'] = title
        pml_songs_performed.loc[code, 'composer'] = composer
        pml_songs_performed.loc[code, 'event'] = event
        pml_songs_performed.loc[code, 'specification'] = specification
        pml_songs_performed.loc[code, 'grade'] = grade

        # from results, get the average std dev above mean for each song
        # get the average std dev above mean if code is in "all codes column"\
        average_std_dev_score = (results[results['all codes'].str.contains(
            code)]['Total Standard Deviation'].mean() * -1) + 1
        pml_songs_performed.loc[code,
                                'average std dev above mean'] = average_std_dev_score

        # ageragy uil score
        average_uil_score = results[results['all codes'].str.contains(
            code)]['Concert Average'].mean()
        pml_songs_performed.loc[code, 'average uil score'] = average_uil_score

        # average score above expected
        average_score_above_expected = results[results['all codes'].str.contains(
            code)]['score above expected'].mean()
        pml_songs_performed.loc[code,
                                'average score above expected'] = average_score_above_expected

    except Exception:
        pass

pml_songs_performed.to_csv('pml_songs_performed.csv')
