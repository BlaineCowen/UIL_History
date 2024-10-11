import pandas as pd
import sqlite3


start_time = pd.Timestamp.now()
conn = sqlite3.connect("uil_2.db")
c = conn.cursor()

results_df = pd.read_sql_query("SELECT * FROM results", conn)
grouped_df = pd.read_sql_query("SELECT * FROM grouped_entries", conn)

conn.close()


def add_concat_to_results(df):

    # fill na in all title, composer, arranger, and event columns
    df["title_1"] = df["title_1"].fillna("")
    df["title_2"] = df["title_2"].fillna("")
    df["title_3"] = df["title_3"].fillna("")
    df["composer_1"] = df["composer_1"].fillna("")
    df["composer_2"] = df["composer_2"].fillna("")
    df["composer_3"] = df["composer_3"].fillna("")
    df["event"] = df["event"].fillna("")
    df["event"] = df["event"].str.replace("/", "-")

    for i in range(1, 4):
        # make song simple lower
        df[f"title_simple_{i}"] = df[f"title_{i}"].str.lower()

        # remove anything in parenthesis
        df[f"title_simple_{i}"] = df[f"title_simple_{i}"].str.replace(
            r"\(.*?\)", "", regex=True
        )
        # remove anything not a letter
        df[f"title_simple_{i}"] = df[f"title_simple_{i}"].str.replace(
            r"[^a-zA-Z]", "", regex=True
        )
        # get rid of spaces
        df[f"title_simple_{i}"] = df[f"title_simple_{i}"].str.replace(" ", "")
        # if from is in the title, remove from and anything after
        df[f"title_simple_{i}"] = df[f"title_simple_{i}"].str.split("from").str[0]

        # erase anything inside of parenthesis and the parenthesis
        df[f"composer_{i}"] = (
            df[f"composer_{i}"]
            .str.lower()
            .str.replace(r"\(.*?\)", "", regex=True)
            .str.replace("  ", " ", regex=False)
            .str.replace("comp:", "", regex=False)
            .str.replace("arr:", "", regex=False)
        )

        df[f"composer_{i}"] = df[f"composer_{i}"].str.strip()

        # erase annything containing arr and anything after
        df[f"composer_match_{i}"] = df[f"composer_{i}"].str.split(" arr ").str[0]
        df[f"composer_match_{i}"] = df[f"composer_{i}"].str.split("arr.").str[0]
        df[f"composer_match_{i}"] = df[f"composer_{i}"].str.split("/").str[0]

        # get rid of "  "
        df[f"composer_match_{i}"] = df[f"composer_match_{i}"].str.replace("  ", " ")

        df[f"composer_match_{i}"] = df[f"composer_{i}"].str.split(" ").str[-1]

        # remove anything not a letter
        df[f"composer_match_{i}"] = df[f"composer_match_{i}"].str.replace(
            r"[^a-zA-Z]", "", regex=True
        )

        # get rid of spaces
        df[f"composer_match_{i}"] = df[f"composer_match_{i}"].str.replace(" ", "")

        arranger = df[f"composer_{i}"].str.split(" arr ").str[-1]
        arranger = arranger.str.split("arr.").str[-1]
        arranger = arranger.str.split("/").str[-1]
        arranger = arranger.str.replace("  ", " ")
        arranger = arranger.str.strip()
        arranger = arranger.str.split(" ").str[-1]
        arranger = arranger.str.replace(r"[^a-zA-Z]", "", regex=True)
        df[f"arranger_{i}"] = arranger

        # Create a new column by combining composer and arranger for the i-th round
        df[f"composer+arranger_{i}"] = df[f"composer_match_{i}"] + df[f"arranger_{i}"]

        # Use .loc with vectorized logic to check if arranger is in composer_match
        df.loc[
            df[f"arranger_{i}"].isin(df[f"composer_match_{i}"]),
            f"composer+arranger_{i}",
        ] = df[f"composer_match_{i}"]

    return df


results_df = add_concat_to_results(results_df)


missing_len_before = 0
missing_len_after = 0

for i in range(1, 4):
    missing_len_before += len(results_df[results_df[f"code_{i}"].str.lower() == "none"])

    # drop original code_i
    results_df = results_df.drop(columns=[f"code_{i}"])

    # merge th 2 df but get code
    results_df = results_df.merge(
        grouped_df[["title", "composer", "event", "code"]],
        how="left",
        left_on=[f"title_simple_{i}", f"composer+arranger_{i}", "event"],
        right_on=["title", "composer", "event"],
    )
    results_df = results_df.rename(columns={"code": f"code_{i}"})

    # if blank, rename "none"
    results_df[f"code_{i}"] = results_df[f"code_{i}"].fillna("none")

    # if "" fill with none
    results_df[f"code_{i}"] = results_df[f"code_{i}"].replace("", "none")

    # Ensure the column is of type string before applying string methods
    results_df[f"code_{i}"] = results_df[f"code_{i}"].astype(str)

    missing_len_after += len(results_df[results_df[f"code_{i}"] == "none"])
    # drop unused
    results_df = results_df.drop(columns=["title", "composer"])


print(f"Missing before: {missing_len_before}")
print(f"Missing after: {missing_len_after}")
print(f"Missing difference: {missing_len_before - missing_len_after}")
print(f"Missing percentage: {missing_len_after / missing_len_before}")

end_time = pd.Timestamp.now()

# update database
conn = sqlite3.connect("uil_2.db")
c = conn.cursor()
pd.DataFrame.to_sql(results_df, "results", conn, if_exists="replace", index=False)

conn.close()
print(f"Time to run: {end_time - start_time}")
