import pandas as pd
import sqlite3


def add_performance_count(pml, df):
    # Group df by "code" and count occurrences

    # only df where concert score is not 0
    df = df[df["concert_final_score"] != 0]

    code_counts = (
        df.groupby("code_1").size()
        + df.groupby("code_2").size()
        + df.groupby("code_3").size()
    )

    # Merge code_counts with pml DataFrame
    pml = pml.merge(
        code_counts.rename("performance_count"),
        left_on="code",
        right_index=True,
        how="left",
    )

    # Fill NaN values with 0
    pml["performance_count"] = pml["performance_count"].fillna(0).astype(int)

    return pml


def add_average_score(pml, df):
    df["average_concert_score"] = df[
        ["concert_score_1", "concert_score_2", "concert_score_3"]
    ].mean(axis=1)
    df["average_sight_reading_score"] = df[
        ["sight_reading_score_1", "sight_reading_score_2", "sight_reading_score_3"]
    ].mean(axis=1)

    average_scores_1 = df.groupby("code_1")["concert_final_score"].sum()
    average_scores_2 = df.groupby("code_2")["concert_final_score"].sum()
    average_scores_3 = df.groupby("code_3")["concert_final_score"].sum()
    pml["average_concert_score"] = (
        pml["code"].map(average_scores_1)
        + pml["code"].map(average_scores_2)
        + pml["code"].map(average_scores_3)
    )
    pml["average_concert_score"] = (
        pml["average_concert_score"] / pml["performance_count"]
    ).round(2)

    average_scores_1 = df.groupby("code_1")["sight_reading_final_score"].sum()
    average_scores_2 = df.groupby("code_2")["sight_reading_final_score"].sum()
    average_scores_3 = df.groupby("code_3")["sight_reading_final_score"].sum()
    pml["average_sight_reading_score"] = (
        pml["code"].map(average_scores_1)
        + pml["code"].map(average_scores_2)
        + pml["code"].map(average_scores_3)
    )
    pml["average_sight_reading_score"] = (
        pml["average_sight_reading_score"] / pml["performance_count"]
    ).round(2)

    return pml


conn = sqlite3.connect("uil.db")

pml = pd.read_sql_query("SELECT * FROM pml", conn)
df = pd.read_sql_query("SELECT * FROM results", conn)

conn.close()

pml = add_performance_count(pml, df)
pml = add_average_score(pml, df)

conn = sqlite3.connect("uil.db")

pml.to_sql("pml", conn, if_exists="replace", index=False)

conn.close()
