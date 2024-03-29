import pandas as pd
import sqlite3
import re
import streamlit as st
import plotly as py
import altair as alt
import plotly.express as px


def get_db(df):
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

    # fix school names
    df["school_search"] = df["school"].str.strip().str.lower()
    df["school_search"] = df["school_search"].str.replace(r"[^\w\s]", "")
    df["school_search"] = df["school_search"].str.replace(r" ", "")

    return df


@st.cache_data
def clean_data():
    df = pd.read_sql("SELECT * FROM results", sqlite3.connect("uil.db"))
    # remove anywhere where concert score 1 is not a number
    df = get_db(df)

    # fix date
    df["contest_date"] = pd.to_datetime(df["contest_date"], errors="coerce")

    # change contest_date to just the year
    df["year"] = df["contest_date"].dt.year.astype(str)

    df = df[df["concert_score_1"] != 0]
    df = df[df["concert_score_2"] != 0]
    df = df[df["concert_score_3"] != 0]
    df = df[df["concert_final_score"] != 0]
    df = df[df["sight_reading_score_1"] != 0]
    df = df[df["sight_reading_score_2"] != 0]
    df = df[df["sight_reading_score_3"] != 0]
    df = df[df["sight_reading_final_score"] != 0]

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


def clean_pml(results_df):
    pml = pd.read_sql("SELECT * FROM pml", sqlite3.connect("uil.db"))
    pml[["arranger", "composer"]] = pml[["arranger", "composer"]].fillna("")

    # only keep rows where event contains band, chorus, or orchestra
    pml = pml[
        pml["event_name"].str.lower().str.contains("band|chorus|orchestra", na=False)
    ]
    # remove any rows where grade is not an int
    pml = pml[pml["grade"].isna() == False]

    # make sure grade is int
    try:
        # Convert "grade" to float, filter out NaN values, then convert to int
        pml["grade"] = pml["grade"].astype(float)
        pml = pml[pml["grade"].notna()]
        pml["grade"] = pml["grade"].astype(int)
    except ValueError:
        pml["grade"] = pml["grade"].str.extract(r"(\d+)", expand=False)
        pml["grade"] = pml["grade"].astype(int)

    pml["song_search"] = pml["title"].str.lower()
    pml["song_search"] = pml["song_search"].str.replace(r"[^\w\s]", "")
    pml["song_search"] = pml["song_search"].str.replace(r" ", "")

    pml["composer_search"] = pml["composer"].str.lower() + pml["arranger"].str.lower()
    pml["composer_search"] = pml["composer_search"].str.replace(r"[^\w\s]", "")

    pml["total_search"] = pml["song_search"] + pml["composer_search"]
    pml["total_search"] = pml["total_search"].str.replace(r"[^\w\s]", "")
    pml["total_search"] = pml["total_search"].str.replace(r" ", "")

    return pml


def main():
    st.title("UIL Dashboard")
    st.write(
        "Welcome to the UIL Dashboard. This dashboard is designed to help track UIL Concert and SR results from the state of Texas."
    )
    st.write("Please select an event to begin.")
    results_df = clean_data()

    tab1, tab2 = st.tabs(["C&SR Results", "PML"])

    with tab1:
        # remove anywhere where concert_1 is not a number
        filter_df = results_df
        event_select = st.selectbox(
            "Select an event",
            ["Band", "Chorus", "Orchestra"],
            index=None,
        )

        if event_select:
            filter_df = filter_df[filter_df["gen_event"] == event_select]

            if event_select == "Chorus":
                # create new to select sub events
                sub_event_select = st.multiselect(
                    "Select a sub event",
                    filter_df[filter_df["event"].str.contains("Chorus")]["event"]
                    .sort_values()
                    .unique(),
                    default=[],
                )

                if sub_event_select:
                    filter_df = filter_df[filter_df["event"].isin(sub_event_select)]

            with st.expander("Filter by schools"):
                # tabs for type select or multiselect

                # school_select = st.multiselect(
                #     "Select a school",
                #     filter_df["school"].sort_values().unique(),
                #     default=[],
                # )

                school_select = st.text_input("Enter a school name", "")
                school_select = school_select.lower()
                school_select = re.sub(r"\s+", "", school_select)

                if school_select:
                    filter_df = filter_df[
                        filter_df["school_search"].str.contains(school_select, na=False)
                    ]

            with st.expander("Filter by Levels"):

                school_level_select = st.selectbox(
                    "Select a school level",
                    filter_df["school_level"].sort_values().unique(),
                    index=None,
                )

                if school_level_select:

                    filter_df = filter_df[
                        filter_df["school_level"].str.contains(
                            school_level_select, na=False
                        )
                    ]

                    conference_select = st.multiselect(
                        "Select a conference",
                        filter_df["conference"].sort_values().unique(),
                        default=[],
                    )
                    if conference_select:
                        filter_df = filter_df[
                            filter_df["conference"].isin(conference_select)
                        ]

                classification_select = st.selectbox(
                    "Select a classification",
                    filter_df["classification"].sort_values().unique(),
                    index=None,
                )

                if classification_select:
                    filter_df = filter_df[
                        filter_df["classification"] == classification_select
                    ]

            with st.expander("Filter by song name and composer"):
                song_name_input = st.text_input("Enter a song name", "")
                song_name_input = song_name_input.lower()
                song_name_input = re.sub(r"\s+", "", song_name_input)

                composer_name_input = st.text_input("Enter a composer name", "")
                composer_name_input = composer_name_input.lower()
                composer_name_input = re.sub(r"\s+", "", composer_name_input)

            # director_select = st.sidebar.text_input("Enter a director name", "")
            # director_select = director_select.lower()

            # if director_select:

            #     filter_df = filter_df[filter_df["Director_search"].str.contains(director_select, na=False)]

            year_select = st.slider("Year Range", 2005, 2024, (2005, 2024))

            if year_select:
                filter_df = filter_df[
                    filter_df["year"].between(str(year_select[0]), str(year_select[1]))
                ]

            if song_name_input or composer_name_input:
                # only show rows where song name is in song_concat
                filter_df = results_df[
                    results_df["song_concat"].str.contains(song_name_input, na=False)
                ]
                # only show rows where composer name is in composer_concat
                filter_df = filter_df[
                    filter_df["composer_concat"].str.contains(
                        composer_name_input, na=False
                    )
                ]

            # testing
            testing_df = filter_df

            filter_df = filter_df[
                [
                    "year",
                    "event",
                    "school",
                    "director",
                    "additional_director",
                    "classification",
                    "choice_1",
                    "choice_2",
                    "choice_3",
                    "concert_final_score",
                    "sight_reading_final_score",
                ]
            ]

            st.write("testing only")
            st.write(testing_df)

            st.write("Filtered Data")

            st.write(filter_df)

            # write len
            st.write("Number of rows:", len(filter_df))

            # make a scores over time
            st.write("Concert Scores Over Time")
            scores_over_time_c = (
                filter_df.groupby("year")["concert_final_score"].mean().sort_index()
            )
            scores_over_time_c2 = (
                results_df[results_df["event"].str.contains(event_select)]
                .groupby("year")["concert_final_score"]
                .mean()
                .sort_index()
            )

            line_chart_c = py.graph_objs.Figure(
                data=[
                    py.graph_objs.Scatter(
                        x=list(range(year_select[0], year_select[1] + 1)),
                        y=scores_over_time_c.values,
                        mode="lines",
                        name="Selected Results",
                    ),
                    py.graph_objs.Scatter(
                        x=list(range(year_select[0], year_select[1] + 1)),
                        y=scores_over_time_c2.values,
                        mode="lines",
                        name="All Results",
                    ),
                ]
            )

            st.plotly_chart(line_chart_c)

            st.write("Sight Reading Scores Over Time")
            scores_over_time_sr = filter_df.groupby("year")[
                "sight_reading_final_score"
            ].mean()

            scores_over_time_sr2 = (
                results_df[results_df["event"].str.contains(event_select)]
                .groupby("year")["sight_reading_final_score"]
                .mean()
                .sort_index()
            )

            line_chart_sr = py.graph_objs.Figure(
                data=[
                    py.graph_objs.Scatter(
                        x=list(range(year_select[0], year_select[1] + 1)),
                        y=scores_over_time_sr.values,
                        mode="lines",
                        name="Selected Results",
                    ),
                    py.graph_objs.Scatter(
                        x=list(range(year_select[0], year_select[1] + 1)),
                        y=scores_over_time_sr2.values,
                        mode="lines",
                        name="All Results",
                    ),
                ]
            )

            st.plotly_chart(line_chart_sr)

            # create a pie chart of the concert scores
            st.write("Concert Scores")
            concert_scores = filter_df["concert_final_score"].value_counts()
            pie_chart_c = py.graph_objs.Figure(
                data=[
                    py.graph_objs.Pie(
                        labels=concert_scores.index,
                        values=concert_scores.values,
                        hole=0.5,
                    )
                ]
            )
            st.plotly_chart(pie_chart_c)

            sight_reading_scores = filter_df["sight_reading_final_score"].value_counts()
            st.write("Sight Reading Scores")
            pie_chart_SR = py.graph_objs.Figure(
                data=[
                    py.graph_objs.Pie(
                        labels=sight_reading_scores.index,
                        values=sight_reading_scores.values,
                        hole=0.5,
                    )
                ]
            )
            st.plotly_chart(pie_chart_SR)

        # # write len
        # st.write("Number of rows:", len(df))

        st.write(
            "This dashboard was created by [Blaine Cowen](mailto:blaine.cowen@gmail.com)"
        )

    with tab2:
        st.write("PML")
        unfiltered_pml = clean_pml(results_df)
        filtered_pml = unfiltered_pml

        grade_select = st.slider(
            "Select a grade",
            0,
            6,
            (0, 6),
        )
        # filter by grade
        if grade_select:
            filtered_pml = filtered_pml[
                (filtered_pml["grade"] >= grade_select[0])
                & (filtered_pml["grade"] <= grade_select[1])
            ]

        song_name_input = st.text_input("Search Titles or Composers", "")
        song_name_input = song_name_input.lower()
        song_name_input = re.sub(r"\s+", "", song_name_input)

        if song_name_input:
            filtered_pml = filtered_pml[
                filtered_pml["total_search"].str.contains(song_name_input, na=False)
            ]

        event_name_select = st.selectbox(
            "Select an event",
            filtered_pml["event_name"].sort_values().unique(),
            index=None,
        )

        if event_name_select:
            filtered_pml = filtered_pml[filtered_pml["event_name"] == event_name_select]

        min_performance_count = st.slider(
            "Minimum Performance Count",
            0,
            100,
            0,
        )

        st.write(filtered_pml)
        st.write(len(filtered_pml))

        graphed_pml = filtered_pml[
            # no nan values
            (filtered_pml["average_concert_score"].notna())
            & (filtered_pml["average_sight_reading_score"].notna())
            # no zeros
            & (filtered_pml["average_concert_score"] != 0)
            & (filtered_pml["average_sight_reading_score"] != 0)
        ]

        if graphed_pml[graphed_pml["performance_count"] > min_performance_count].empty:
            st.write("No data to graph")
            return

        else:

            max_x = graphed_pml[
                graphed_pml["performance_count"] > min_performance_count
            ]["average_concert_score"].max()
            max_y = graphed_pml[
                graphed_pml["performance_count"] > min_performance_count
            ]["average_sight_reading_score"].max()

            bubble_chart_altair = (
                alt.Chart(
                    graphed_pml[
                        graphed_pml["performance_count"] > min_performance_count
                    ]
                )
                .mark_circle()
                .encode(
                    x=alt.X(
                        "average_concert_score",
                        scale=alt.Scale(type="log", domain=(1, max_x)),
                    ),
                    y=alt.Y(
                        "average_sight_reading_score",
                        scale=alt.Scale(type="log", domain=(1, max_y)),
                    ),
                    color=alt.Color("event_name", legend=None),
                    size=alt.Size(
                        "performance_count",
                        legend=None,
                        scale=alt.Scale(range=[2, 3000]),
                    ),
                    tooltip=[
                        "title",
                        "composer",
                        "event_name",
                        "average_concert_score",
                        "average_sight_reading_score",
                    ],
                )
                .interactive()
            )

            st.altair_chart(
                bubble_chart_altair,
                use_container_width=True,
            )

            # make a chart that shows the share of the remaining songs vs all songs in the event


if __name__ == "__main__":
    main()
