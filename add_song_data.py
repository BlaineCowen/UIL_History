
from numpy.lib.arraysetops import unique
import pandas as pd
import numpy as np
import difflib


uil_data = pd.read_csv('all_results.csv', encoding='utf-8')
pml = pd.read_csv('pml.csv', encoding='utf-8')

# if pml_codes.csv doesn't exist, create it
try:
    pml_nums = pd.read_csv('pml_codes.csv')
except:
    pml_nums = pd.DataFrame()
    pml_nums.to_csv('pml_codes.csv', index=False)


# use difflib.get_close_matches to find close matches to song1
# retrun pml['code'] of that row

pml.fillna(' ', inplace=True)
pml['concat'] = pml['Title'] + ' ' + pml['Composer']


def get_close_matches(song1, event, composer):

    # skip if nan
    if pd.isnull(song1):
        return np.nan

    pml_song = pml['concat']
    # pml_song to list
    pml_song = pml_song.tolist()
    pml_song = [str(x) for x in pml_song]

    close_matches = difflib.get_close_matches(song1, pml_song, n=1, cutoff=0.7)

    # if get_close_matches returns nothing, try again without pml['Specification']
    if close_matches == []:
        close_matches = difflib.get_close_matches(
            song1, pml_song, n=1, cutoff=0.7)

    # return pml index of close matches
    try:
        # i = pml.index[pml['Title'] + ' ' + pml['Specification'] + ' ' + pml['Composer'] + ' ' + pml['Event Name'] == close_matches[0]]
        # code = pml.loc[i, 'code'].text
        code = pml[pml['concat'] == close_matches[0]]['Code'].tolist()[0]
    except Exception:
        code = ''
    print(f"song1: {song1} close_matches: {close_matches} code: {code}")
    return code


def add_pml_code():
    try:
        pml_nums = pd.read_csv('pml_codes.csv')
    except:
        pml_nums = pd.DataFrame(columns=['title', 'event', 'code'])
        pml_nums.to_csv('pml_codes.csv', index=False)

    # uil_data['get_match1'] = uil_data['Title 1'].astype(
    #     str) + " " + uil_data['Event'][4:].astype(str) + " " + uil_data['Composer 1'].astype(str)
    # uil_data['get_match2'] = uil_data['Title 2'].astype(str) + \
    #     " " + uil_data['Event'][4:].astype(str) + \
    #     " " + uil_data['Composer 2'].astype(str)
    # uil_data['get_match3'] = uil_data['Title 3'].astype(str) + \
    #     " " + uil_data['Event'][4:].astype(str) + \
    #     " " + uil_data['Composer 3'].astype(str)

    # get rid of the first 4 characters of the event name
    # uil_data['Event'] = uil_data['Event'].str[4:]

    # uil_data['get_match1'] = uil_data['Title 1'] + " " + uil_data['Composer 1']

    # uil_data['get_match2'] = uil_data['Title 2'] + " " + uil_data['Composer 2']

    # uil_data['get_match3'] = uil_data['Title 3'] + " " + uil_data['Composer 3']

    # # make a dataframe with unique entries of uil data columns
    # unique_songs = pd.DataFrame()
    # for col in ['get_match1', 'get_match2', 'get_match3']:
    #     unique_songs['title'] = unique_songs.append(uil_data[col])
    #     # add event name column
    #     unique_songs['event'] = uil_data['Event'].str[4:]
    #     # drop duplicates
    #     unique_songs = unique_songs.drop_duplicates()

    unique_songs = uil_data[['Title 1', 'Composer 1', 'Event']].append(uil_data[[
        'Title 2', 'Composer 2', 'Event']]).append(uil_data[['Title 3', 'Composer 3', 'Event']])

    unique_songs = unique_songs.drop_duplicates()

    print(f"there are {len(unique_songs)} unique songs in uil data")

    pml_nums['title'] = unique_songs['Title 1']
    pml_nums['event'] = unique_songs['Event']
    pml_nums['composer'] = unique_songs['Composer 1']

    # drop duplicates
    pml_nums = pml_nums.drop_duplicates()

    pml_nums = pml_nums.sort_values(by="title", ascending=False)

    # filter pml_nums where pml code is null
    pml_nums = pml_nums[pd.isnull(pml_nums['code'])]

    # save to csv
    pml_nums.to_csv('pml_codes.csv', index=False)

    print(f"there are {len(pml_nums)} unique songs in pml_nums")

    # merge pml_nums_to_find with pml_nums
    pml_nums = pd.merge(pml_nums, pml_nums_to_find, how='outer')

    # drop duplicates
    pml_nums = pml_nums.drop_duplicates()

    # save to csv
    pml_nums.to_csv('pml_codes.csv', index=False)

    print(pml_nums_to_find)

    # append pml_nums_to_find to pml_nums
    pml_nums = pml_nums.drop_duplicates(subset='title', keep='first')
    pml_nums.append(pml_nums_to_find, ignore_index=True)

    # use unique values for pml_nums

    # uil_data['pml_code2'] = uil_data['get_match2'].apply(get_close_matches)
    # uil_data['pml_code3'] = uil_data['get_match3'].apply(get_close_matches)
    pml_nums.to_csv('pml_codes.csv', index=False)


if __name__ == '__main__':
    add_pml_code()
