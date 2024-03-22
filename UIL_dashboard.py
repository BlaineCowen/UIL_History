import pandas as pd
import sqlite3
import re
import streamlit as st


def add_song_concat(df):
    score_subset = [
        "Concert Score 1",
        "Concert Score 2",
        "Concert Score 3",
        "Concert Final Score",
        "Sight Reading Score 1",
        "Sight Reading Score 2",
        "Sight Reading Score 3",
        "Sight Reading Final Score",
    ]

    # drop any rows where all scores are na
    df = df.dropna(subset=score_subset, how="all")

    # force numeric scores to be numeric
    df[score_subset] = df[score_subset].apply(pd.to_numeric, errors="coerce")

    # fill everything else with ""
    cols_not_in_subset = df.columns.difference(score_subset)
    df[cols_not_in_subset] = df[cols_not_in_subset].fillna("")

    df["song_concat"] = df["Title 1"] + " " + df["Title 2"] + " " + df["Title 3"]
    df["composer_concat"] = (
        df["Composer 1"] + " " + df["Composer 2"] + " " + df["Composer 3"]
    )
    # remove any non-alphanumeric characters and spaces
    df["song_concat"] = df["song_concat"].str.replace(r"[^\w\s]", "")
    df["song_concat"] = df["song_concat"].str.replace(" ", "")

    df["composer_concat"] = df["composer_concat"].str.replace(r"[^\w\s]", "")
    df["composer_concat"] = df["composer_concat"].str.replace(r" ", "")

    # make all characters lowercase
    df["song_concat"] = df["song_concat"].str.lower()
    df["composer_concat"] = df["composer_concat"].str.lower()

    # director search
    df["Director_search"] = df["Director"].str.lower()
    df["Additional Director_search"] = df["Additional Director"].str.lower()

    # fill na all

    return df


def clean_data():
    df = pd.read_sql("SELECT * FROM results", sqlite3.connect("uil.db"))

    # remove anywhere where concert score 1 is not a number
    df = add_song_concat(df)

    # fix date
    df["Contest Date"] = pd.to_datetime(df["Contest Date"], errors="coerce")

    # change contest date to just the year
    df["Year"] = df["Contest Date"].dt.year.astype(str)

    # remove anywhere where concert score 1 is dna
    df = df[df["Concert Score 1"] != "DNA"]
    df = df[df["Concert Score 1"].isna() == False]
    df = df[df["Concert Score 1"] != "DQ"]

    df = df[df["Concert Score 2"] != "DNA"]
    df = df[df["Concert Score 2"].isna() == False]
    df = df[df["Concert Score 2"] != "DQ"]

    df = df[df["Concert Score 3"] != "DNA"]
    df = df[df["Concert Score 3"].isna() == False]
    df = df[df["Concert Score 3"] != "DQ"]

    df = df[df["Concert Final Score"] != "DNA"]
    df = df[df["Concert Final Score"].isna() == False]
    df = df[df["Concert Final Score"] != "DQ"]

    df = df[df["Sight Reading Score 1"] != "DNA"]
    df = df[df["Sight Reading Score 1"].isna() == False]
    df = df[df["Sight Reading Score 1"] != "DQ"]

    df = df[df["Sight Reading Score 2"] != "DNA"]
    df = df[df["Sight Reading Score 2"].isna() == False]
    df = df[df["Sight Reading Score 2"] != "DQ"]

    df = df[df["Sight Reading Score 3"] != "DNA"]
    df = df[df["Sight Reading Score 3"].isna() == False]
    df = df[df["Sight Reading Score 3"] != "DQ"]

    df = df[df["Sight Reading Final Score"] != "DNA"]
    df = df[df["Sight Reading Final Score"].isna() == False]
    df = df[df["Sight Reading Final Score"] != "DQ"]

    # change all concert scores to int
    df["Concert Score 1"] = df["Concert Score 1"].astype(float).astype(int)
    df["Concert Score 2"] = df["Concert Score 2"].astype(float).astype(int)
    df["Concert Score 3"] = df["Concert Score 3"].astype(float).astype(int)
    df["Concert Final Score"] = df["Concert Final Score"].astype(float).astype(int)
    df["Sight Reading Score 1"] = df["Sight Reading Score 1"].astype(float).astype(int)
    df["Sight Reading Score 2"] = df["Sight Reading Score 2"].astype(float).astype(int)
    df["Sight Reading Score 3"] = df["Sight Reading Score 3"].astype(float).astype(int)
    df["Sight Reading Final Score"] = (
        df["Sight Reading Final Score"].astype(float).astype(int)
    )

    # add columns called Choice 1, Choice 2, and Choice 3 where title and composer are combined
    df["Choice 1"] = df["Title 1"] + "–" + df["Composer 1"]
    df["Choice 2"] = df["Title 2"] + "–" + df["Composer 2"]
    df["Choice 3"] = df["Title 3"] + "–" + df["Composer 3"]

    df["School Level"] = ""
    df.loc[df["Conference"].str.contains("A", na=False), "School Level"] = "High School"
    df.loc[df["Conference"].str.contains("C", na=False), "School Level"] = (
        "Middle School/JH"
    )

    # change classification to title case
    df["Classification"] = df["Classification"].str.replace("-", " ")
    df["Classification"] = df["Classification"].str.title()
    df.loc[df["Classification"].str.contains("Nv", na=False), "Classification"] = (
        "Non Varsity"
    )
    # if classification begins with "V" it is Varsit
    df.loc[df["Classification"].str.contains(r"^V", na=False), "Classification"] = (
        "Varsity"
    )

    return df


def main():
    st.title("UIL Dashboard")
    st.write(
        "Welcome to the UIL Dashboard. This dashboard is designed to help you track UIL Concert and SR results from the state of Texas."
    )
    st.write("Please select an option from the sidebar to begin.")
    # remove anywhere where concert score 1 is not a number
    df = clean_data()

    song_name_input = st.sidebar.text_input("Enter a song name", "")
    song_name_input = song_name_input.lower()
    song_name_input = re.sub(r"\s+", "", song_name_input)

    composer_name_input = st.sidebar.text_input("Enter a composer name", "")
    composer_name_input = composer_name_input.lower()
    composer_name_input = re.sub(r"\s+", "", composer_name_input)

    school_select = st.sidebar.multiselect(
        "Select a school",
        df["School"].sort_values().unique(),
        default=[],
    )

    director_select = st.sidebar.text_input("Enter a director name", "")
    director_select = director_select.lower()

    if director_select:

        df = df[df["Director_search"].str.contains(director_select, na=False)]

    event_select = st.sidebar.selectbox(
        "Select an event",
        ["Band", "Chorus", "Orchestra"],
        index=None,
    )

    if event_select:
        if "Band" in event_select:
            df = df[df["Event"].str.contains("Band", na=False)]
        if "Chorus" in event_select:
            df = df[df["Event"].str.contains("Chorus", na=False)]
        if "Orchestra" in event_select:
            df = df[df["Event"].str.contains("Orchestra", na=False)]

        # create new sidebar to select sub events
        sub_event_select = st.sidebar.multiselect(
            "Select a sub event",
            df["Event"].sort_values().unique(),
            default=[],
        )

        if sub_event_select:
            df = df[df["Event"].isin(sub_event_select)]

    school_level_select = st.sidebar.selectbox(
        "Select a school level",
        df["School Level"].sort_values().unique(),
        index=None,
    )

    if school_level_select:
        df = df[df["School Level"].str.contains(school_level_select, na=False)]

        conference_select = st.sidebar.multiselect(
            "Select a conference",
            df["Conference"].sort_values().unique(),
            default=[],
        )

        if conference_select:
            df = df[df["Conference"].isin(conference_select)]

    classification_select = st.sidebar.selectbox(
        "Select a classification",
        df["Classification"].sort_values().unique(),
        index=None,
    )

    if classification_select:
        df = df[df["Classification"] == classification_select]

    # only show rows where song name is in song_concat
    filter_df = df[df["song_concat"].str.contains(song_name_input, na=False)]
    # only show rows where composer name is in composer_concat
    filter_df = filter_df[
        filter_df["composer_concat"].str.contains(composer_name_input, na=False)
    ]

    if school_select:
        filter_df = filter_df[filter_df["School"].isin(school_select)]

    filter_df = filter_df[
        [
            "Year",
            "School",
            "Director",
            "Additional Director",
            "Choice 1",
            "Choice 2",
            "Choice 3",
            "Concert Final Score",
            "Sight Reading Final Score",
        ]
    ]

    st.write("Filtered Data")

    st.write(filter_df)

    # write len
    st.write("Number of rows:", len(filter_df))

    st.write(df)

    # write len
    st.write("Number of rows:", len(df))

    st.sidebar.write("This dashboard was created by [Blaine Cowen]")


if __name__ == "__main__":
    main()
