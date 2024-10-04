import pandas as pd
import sqlite3
from sklearn.preprocessing import MinMaxScaler


def add_performance_count(pml, df):

    # make sure df code_n is str
    df["code_1"] = df["code_1"].astype(str)
    df["code_2"] = df["code_2"].astype(str)
    df["code_3"] = df["code_3"].astype(str)

    # make sure pml code is str
    pml["code"] = pml["code"].astype(str).str.strip()

    # get rid of last 2 characters if they are .0
    pml["code"] = pml["code"].str.replace(".0", "")

    # only df where concert score is not 0
    df = df[df["concert_final_score"] != 0]

    # get the count of each code
    code_counts = (
        df.groupby("code_1")
        .size()
        .add(df.groupby("code_2").size(), fill_value=0)
        .add(df.groupby("code_3").size(), fill_value=0)
    )

    # get rid of nan
    code_counts = code_counts.dropna()

    code_counts = code_counts.reset_index()

    code_counts.columns = ["code", "performance_count"]

    code_counts["code"] = code_counts["code"].astype(str)
    code_counts["performance_count"] = code_counts["performance_count"].astype(int)

    # remove where code is nan
    code_counts = code_counts.dropna(subset=["code"])

    # change code to string
    code_counts["code"] = code_counts["code"].astype(str)

    # merge code_counts with pml but keep code_counts performance_count
    pml = pml.merge(code_counts, on="code", how="left")

    pml["performance_count"] = pml["performance_count"].fillna(0)

    return pml


def add_average_score(pml, df):
    # Group df by "code" and calculate average score
    # get rid of any where concert score is 0
    df = df[df["concert_final_score"] != 0]
    # also sr
    df = df[df["sight_reading_final_score"] != 0]

    if "average_concert_score" not in df.columns:

        df["average_concert_score"] = df[
            ["concert_score_1", "concert_score_2", "concert_score_3"]
        ].mean(axis=1)
        df["average_sight_reading_score"] = df[
            [
                "sight_reading_score_1",
                "sight_reading_score_2",
                "sight_reading_score_3",
            ]
        ].mean(axis=1)

        def adjust_results_df(df):

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

            df["gen_event"] = ""
            df.loc[df["event"].str.contains("Band", na=False), "gen_event"] = "Band"
            df.loc[df["event"].str.contains("Chorus", na=False), "gen_event"] = "Chorus"
            df.loc[df["event"].str.contains("Orchestra", na=False), "gen_event"] = (
                "Orchestra"
            )

            # drop any rows where all scores are na
            df = df.dropna(subset=score_subset, how="all")

            # force numeric scores to be numeric
            df[score_subset] = df[score_subset].apply(pd.to_numeric, errors="coerce")

            # fill everything else with ""
            cols_not_in_subset = df.columns.difference(score_subset)
            df[cols_not_in_subset] = df[cols_not_in_subset].fillna("")

            df["song_concat"] = (
                df["title_1"] + " " + df["title_2"] + " " + df["title_3"]
            )
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

            df["event"] = df["event"].str.split("-").str.get(1)

            # # director search
            # df["director_search"] = df["director"].str.lower()
            # df["additional_director_search"] = df["additional_sirector"].str.lower()

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
            df["concert_final_score"] = (
                df["concert_final_score"].astype(float).astype(int)
            )
            df["sight_reading_score_1"] = (
                df["sight_reading_score_1"].astype(float).astype(int)
            )
            df["sight_reading_score_2"] = (
                df["sight_reading_score_2"].astype(float).astype(int)
            )
            df["sight_reading_score_3"] = (
                df["sight_reading_score_3"].astype(float).astype(int)
            )
            df["sight_reading_final_score"] = (
                df["sight_reading_final_score"].astype(float).astype(int)
            )

            df["average_concert_score"] = (
                df["concert_score_1"] + df["concert_score_2"] + df["concert_score_3"]
            ) / 3
            # round
            df["average_concert_score"] = df["average_concert_score"].round(2)

            df["average_sight_reading_score"] = (
                df["sight_reading_score_1"]
                + df["sight_reading_score_2"]
                + df["sight_reading_score_3"]
            ) / 3
            # round
            df["average_sight_reading_score"] = df["average_sight_reading_score"].round(
                2
            )

            event_group = df.groupby(["gen_event", "contest_date", "concert_judge_1"])
            # join the scores together
            df = df.merge(
                event_group["average_concert_score"].mean(),
                on=["gen_event", "contest_date", "concert_judge_1"],
                suffixes=("", "_mean"),
            )
            # rename new column
            df = df.rename(
                columns={"average_concert_score_mean": "contest_average_concert_score"}
            )

            return df

        df = adjust_results_df(df)

    # only df where concert score is not 0

    average_scores_1 = df.groupby("code_1")["concert_final_score"].sum()
    average_scores_2 = df.groupby("code_2")["concert_final_score"].sum()
    average_scores_3 = df.groupby("code_3")["concert_final_score"].sum()

    if "delta_score" not in df.columns:
        df["delta_score"] = (
            -df["average_concert_score"] + df["contest_average_concert_score"]
        )

        # if the delta score is nan then set it to 0
        df["delta_score"] = df["delta_score"].fillna(0)

    delta_score_1 = df.groupby("code_1")["delta_score"].sum()
    delta_score_2 = df.groupby("code_2")["delta_score"].sum()
    delta_score_3 = df.groupby("code_3")["delta_score"].sum()

    if "average_concert_score" not in pml.columns:
        pml["average_concert_score"] = 0
    pml.loc[pml["performance_count"] > 0, "average_concert_score"] = (
        pml.loc[pml["performance_count"] > 0, "code"].map(average_scores_1)
        + pml.loc[pml["performance_count"] > 0, "code"].map(average_scores_2)
        + pml.loc[pml["performance_count"] > 0, "code"].map(average_scores_3)
    )

    pml.loc[pml["performance_count"] > 0, "delta_score"] = (
        pml.loc[pml["performance_count"] > 0, "code"].map(delta_score_1)
        + pml.loc[pml["performance_count"] > 0, "code"].map(delta_score_2)
        + pml.loc[pml["performance_count"] > 0, "code"].map(delta_score_3)
    )

    # divide delta by performance count
    pml.loc[pml["performance_count"] > 0, "delta_score"] = (
        pml.loc[pml["performance_count"] > 0, "delta_score"]
        / pml.loc[pml["performance_count"] > 0, "performance_count"]
    ).round(2)

    # get delta score percentile
    pml.loc[pml["performance_count"] > 0, "delta_score_percentile"] = pml.loc[
        pml["performance_count"] > 0, "delta_score"
    ].rank(pct=True)

    # fillna with 0
    pml["delta_score_percentile"] = pml["delta_score_percentile"].fillna(0)
    # if it is none then set it to 0

    # get performance count percentile
    pml.loc[pml["performance_count"] > 0, "performance_count_percentile"] = pml.loc[
        pml["performance_count"] > 0, "performance_count"
    ].rank(pct=True)

    # choose a weight for each score
    performance_weight = 0.5
    delta_weight = 1

    # create song_score based on delta_score and performance_count
    pml.loc[pml["performance_count"] > 0, "song_score"] = (
        pml.loc[pml["performance_count"] > 0, "delta_score_percentile"] * delta_weight
        + pml.loc[pml["performance_count"] > 0, "performance_count_percentile"]
        * performance_weight
    ) / 2

    # fillna with 0
    pml["song_score"] = pml["song_score"].fillna(0)

    # Scale the song_score to the range [0, 100]
    scaler = MinMaxScaler(feature_range=(0, 100))
    pml.loc[pml["performance_count"] > 0, "song_score"] = scaler.fit_transform(
        pml.loc[pml["performance_count"] > 0, "song_score"].values.reshape(-1, 1)
    )

    # normalize results with highest being 100
    pml.loc[pml["performance_count"] > 0, "song_score"] = (
        pml.loc[pml["performance_count"] > 0, "song_score"]
        / pml.loc[pml["performance_count"] > 0, "song_score"].max()
    ) * 100

    pml.loc[pml["performance_count"] > 0, "average_concert_score"] = (
        pml.loc[pml["performance_count"] > 0, "average_concert_score"]
        / pml.loc[pml["performance_count"] > 0, "performance_count"]
    ).round(2)

    average_scores_1 = df.groupby("code_1")["sight_reading_final_score"].sum()
    average_scores_2 = df.groupby("code_2")["sight_reading_final_score"].sum()
    average_scores_3 = df.groupby("code_3")["sight_reading_final_score"].sum()
    pml["average_sight_reading_score"] = (
        pml["code"].map(average_scores_1)
        + pml["code"].map(average_scores_2)
        + pml["code"].map(average_scores_3)
    )
    pml.loc[pml["performance_count"] > 0, "average_sight_reading_score"] = (
        pml.loc[pml["performance_count"] > 0, "average_sight_reading_score"]
        / pml.loc[pml["performance_count"] > 0, "performance_count"]
    ).round(2)

    # find earliest year a song has been performned
    earliest_year = df.groupby("code_1")["year"].min()
    # check code_2 and 3
    earliest_year = earliest_year.combine_first(df.groupby("code_2")["year"].min())
    earliest_year = earliest_year.combine_first(df.groupby("code_3")["year"].min())

    pml["earliest_year"] = pml["code"].map(earliest_year)

    return pml, df


def main():

    conn = sqlite3.connect("combined.db")

    pml = pd.read_csv("Scrape_data/pml.csv")
    # pml = pd.read_csv("Scrape_data/pml.csv")
    df = pd.read_sql_query("SELECT * FROM results", conn)

    pml = add_performance_count(pml, df)
    pml, df = add_average_score(pml, df)

    pml.to_sql("pml", conn, if_exists="replace", index=False)
    df.to_sql("results", conn, if_exists="replace", index=False)

    conn.close()


if __name__ == "__main__":
    main()
