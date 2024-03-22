import pandas as pd
import sqlite3
import re
import streamlit as st


@st.cache_data
def add_song_concat(df):
    score_subset = [
        "concert_score_1",
        "concert_score_2",
        "concert_score_3",
        "concert_final_score",
        "sight_reading_score_1",
        "sight_reading_score_2",
        "sight_reading_score_3",
        "sight_reading_final_score",
    ]

    # drop any rows where all scores are na
    df = df.dropna(subset=score_subset, how="all")

    # force numeric scores to be numeric
    df[score_subset] = df[score_subset].apply(pd.to_numeric, errors="coerce")

    # fill everything else with ""
    cols_not_in_subset = df.columns.difference(score_subset)
    df[cols_not_in_subset] = df[cols_not_in_subset].fillna("")

    df["song_concat"] = df["title_1"] + " " + df["title_2"] + " " + df["title_3"]
    df["composer_concat"] = (
        df["composer_1"] + " " + df["composer_2"] + " " + df["composer_3"]
    )
    # remove any non-alphanumeric characters and spaces
    df["song_concat"] = df["song_concat"].str.replace(r"[^\w\s]", "")
    df["song_concat"] = df["song_concat"].str.replace(" ", "")

    df["composer_concat"] = df["composer_concat"].str.replace(r"[^\w\s]", "")
    df["composer_concat"] = df["composer_concat"].str.replace(r" ", "")

    # make all characters lowercase
    df["song_concat"] = df["song_concat"].str.lower()
    df["composer_concat"] = df["composer_concat"].str.lower()

    # # director search
    # df["director_search"] = df["director"].str.lower()
    # df["additional_director_search"] = df["additional_sirector"].str.lower()

    # fill na all

    return df


def clean_data():
    df = pd.read_sql("SELECT * FROM results", sqlite3.connect("uil.db"))

    # remove anywhere where concert score 1 is not a number
    df = add_song_concat(df)

    # fix date
    df["contest_date"] = pd.to_datetime(df["contest_date"], errors="coerce")

    # change contest_date to just the year
    df["year"] = df["contest_date"].dt.year.astype(str)

    # remove anywhere where concert score 1 is dna
    df = df[df["concert_score_1"] != "DNA"]
    df = df[df["concert_score_1"].isna() == False]
    df = df[df["concert_score_1"] != "DQ"]

    df = df[df["concert_score_2"] != "DNA"]
    df = df[df["concert_score_2"].isna() == False]
    df = df[df["concert_score_2"] != "DQ"]

    df = df[df["concert_score_3"] != "DNA"]
    df = df[df["concert_score_3"].isna() == False]
    df = df[df["concert_score_3"] != "DQ"]

    df = df[df["concert_final_score"] != "DNA"]
    df = df[df["concert_final_score"].isna() == False]
    df = df[df["concert_final_score"] != "DQ"]

    df = df[df["sight_reading_score_1"] != "DNA"]
    df = df[df["sight_reading_score_1"].isna() == False]
    df = df[df["sight_reading_score_1"] != "DQ"]

    df = df[df["sight_reading_score_2"] != "DNA"]
    df = df[df["sight_reading_score_2"].isna() == False]
    df = df[df["sight_reading_score_2"] != "DQ"]

    df = df[df["sight_reading_score_3"] != "DNA"]
    df = df[df["sight_reading_score_3"].isna() == False]
    df = df[df["sight_reading_score_3"] != "DQ"]

    df = df[df["sight_reading_final_score"] != "DNA"]
    df = df[df["sight_reading_final_score"].isna() == False]
    df = df[df["sight_reading_final_score"] != "DQ"]

    # change all concert scores to int
    df["concert_score_1"] = df["concert_score_1"].astype(float).astype(int)
    df["concert_score_2"] = df["concert_score_2"].astype(float).astype(int)
    df["concert_score_3"] = df["concert_score_3"].astype(float).astype(int)
    df["concert_final_score"] = df["concert_final_score"].astype(float).astype(int)
    df["sight_reading_score_1"] = df["sight_reading_score_1"].astype(float).astype(int)
    df["sight_reading_score_2"] = df["sight_reading_score_2"].astype(float).astype(int)
    df["sight_reading_score_3"] = df["sight_reading_score_3"].astype(float).astype(int)
    df["sight_reading_final_score"] = (
        df["sight_reading_final_score"].astype(float).astype(int)
    )

    # add columns called Choice 1, Choice 2, and Choice 3 where title and composer are combined
    df["choice_1"] = df["title_1"] + "–" + df["composer_1"]
    df["choice_2"] = df["title_2"] + "–" + df["composer_2"]
    df["choice_3"] = df["title_3"] + "–" + df["composer_3"]

    df["school_level"] = ""
    df.loc[df["conference"].str.contains("A", na=False), "school_level"] = "High School"
    df.loc[df["conference"].str.contains("C", na=False), "school_level"] = (
        "Middle School/JH"
    )

    # change classification to title case
    df["classification"] = df["classification"].str.replace("-", " ")
    df["classification"] = df["classification"].str.title()
    df.loc[df["classification"].str.contains("Nv", na=False), "classification"] = (
        "Non Varsity"
    )
    # if classification begins with "V" it is Varsit
    df.loc[df["classification"].str.contains(r"^V", na=False), "classification"] = (
        "Varsity"
    )

    return df


def main():
    st.title("UIL Dashboard")
    st.write(
        "Welcome to the UIL Dashboard. This dashboard is designed to help track UIL Concert and SR results from the state of Texas."
    )
    st.write("Please select an option from the sidebar to begin.")
    # remove anywhere where concert_1 is not a number
    df = clean_data()

    event_select = st.sidebar.selectbox(
        "Select an event",
        ["Band", "Chorus", "Orchestra"],
        index=None,
    )

    if event_select:
        if "Band" in event_select:
            df = df[df["event"].str.contains("Band", na=False)]
        if "Chorus" in event_select:
            df = df[df["event"].str.contains("Chorus", na=False)]
        if "Orchestra" in event_select:
            df = df[df["event"].str.contains("Orchestra", na=False)]

        # create new sidebar to select sub events
        sub_event_select = st.sidebar.multiselect(
            "Select a sub event",
            df["event"].sort_values().unique(),
            default=[],
        )

        if sub_event_select:
            df = df[df["Event"].isin(sub_event_select)]

    school_select = st.sidebar.multiselect(
        "Select a school",
        df["school"].sort_values().unique(),
        default=[],
    )

    school_level_select = st.sidebar.selectbox(
        "Select a school level",
        df["school_level"].sort_values().unique(),
        index=None,
    )
    song_name_input = st.sidebar.text_input("Enter a song name", "")
    song_name_input = song_name_input.lower()
    song_name_input = re.sub(r"\s+", "", song_name_input)

    composer_name_input = st.sidebar.text_input("Enter a composer name", "")
    composer_name_input = composer_name_input.lower()
    composer_name_input = re.sub(r"\s+", "", composer_name_input)

    # director_select = st.sidebar.text_input("Enter a director name", "")
    # director_select = director_select.lower()

    # if director_select:

    #     df = df[df["Director_search"].str.contains(director_select, na=False)]

    if school_level_select:
        df = df[df["school_level"].str.contains(school_level_select, na=False)]

        conference_select = st.sidebar.multiselect(
            "Select a conference",
            df["conference"].sort_values().unique(),
            default=[],
        )

        if conference_select:
            df = df[df["conference"].isin(conference_select)]

    classification_select = st.sidebar.selectbox(
        "Select a classification",
        df["classification"].sort_values().unique(),
        index=None,
    )

    if classification_select:
        df = df[df["classification"] == classification_select]

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
            "year",
            "school",
            "director",
            "additional_director",
            "choice_1",
            "choice_2",
            "choice_3",
            "concert_final_score",
            "sight_reading_final_score",
        ]
    ]

    st.write("Filtered Data")

    st.write(filter_df)

    # write len
    st.write("Number of rows:", len(filter_df))

    # st.write(df)

    # # write len
    # st.write("Number of rows:", len(df))

    st.sidebar.write(
        "This dashboard was created by [Blaine Cowen](mailto:blaine.cowen@gmail.com)"
    )


if __name__ == "__main__":
    main()
