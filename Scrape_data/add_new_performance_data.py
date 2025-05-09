import pandas as pd
import sqlite3
from sklearn.preprocessing import MinMaxScaler
import re # Added for integrate_new_songs


def parse_date_flexible(date_str):
    if pd.isna(date_str):
        return pd.NaT
    # Ensure date_str is a string before trying to parse
    date_str = str(date_str).strip()[:10] # Clean and take first 10 chars
    # Try formats in order of likelihood or known presence
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            # Return the datetime object directly
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            continue
    # print(f"Warning: Date '{date_str}' could not be parsed by any known format.") # Keep commented for now
    return pd.NaT # Return NaT if all formats fail


def clean_event_name(event_name):
    """Standardizes the event name string. (Copied from quick_fix_events.py)"""
    if pd.isna(event_name):
        return "" 
    event_str = str(event_name)
    # Remove leading digits and hyphen (e.g., '100-')
    event_str = re.sub(r"^\d+-", "", event_str)
    # Convert to lowercase
    event_str = event_str.lower()
    # Replace / with -
    event_str = event_str.replace("/", "-")
    # Strip leading/trailing whitespace
    event_str = event_str.strip()
    return event_str


# --- Added Function: Integrate New Songs --- 
def integrate_new_songs_from_results(pml_df, results_df):
    """Adds new songs found in results_df but not in pml_df."""
    print("Integrating new songs into PML DataFrame from results...")
    
    # Ensure pml 'code' exists and is standardized for checking
    if 'code' not in pml_df.columns:
        print("Error: 'code' column missing from input pml_df. Cannot integrate new songs.")
        return pml_df # Return original pml
    pml_df['code'] = pml_df['code'].astype(str).str.strip()
    pml_codes = set(pml_df['code'])
    
    new_songs_data = []

    # Standardize results columns if not already done (should be by this stage)
    results_df.columns = [str(col).lower().replace(' ', '_').replace('-', '_') for col in results_df.columns]

    song_info_map = {
        'code_1': ('title_1', 'composer_1'),
        'code_2': ('title_2', 'composer_2'),
        'code_3': ('title_3', 'composer_3'),
    }
    required_cols = ['event'] + [item for sublist in song_info_map.values() for item in sublist] + list(song_info_map.keys())
    if not all(col in results_df.columns for col in required_cols):
        print(f"Error: Results DataFrame missing required columns for song integration ({required_cols}).")
        return pml_df

    for index, row in results_df.iterrows():
        event_name = str(row.get('event', '')).strip() # Use standardized event name from results
        for code_col, (title_col, composer_col) in song_info_map.items():
            song_code = str(row.get(code_col, '')).strip()
            invalid_codes_check = ['', 'none', 'not_found', 'nan']
            
            if song_code and song_code.lower() not in invalid_codes_check and song_code not in pml_codes:
                song_title = row.get(title_col, 'Untitled')
                song_composer = row.get(composer_col, 'Unknown Composer')
                
                new_song_record = {
                    'code': song_code,
                    'title': song_title,
                    'composer': song_composer,
                    'event_name': event_name, 
                    'grade': 0, # Default values
                    'publisher': '',
                    'arranger': '',
                    'specification': ''
                    # Add other necessary default PML columns
                }
                new_songs_data.append(new_song_record)
                pml_codes.add(song_code) 
    
    if new_songs_data:
        new_songs_df = pd.DataFrame(new_songs_data)
        print(f"Adding {len(new_songs_df)} new unique song entries to PML DataFrame.")
        # Ensure new songs df columns match pml_df columns before concat, adding missing ones with defaults
        for col in pml_df.columns:
             if col not in new_songs_df.columns:
                 # Infer default based on dtype or use a standard default
                 if pd.api.types.is_numeric_dtype(pml_df[col]): new_songs_df[col] = 0
                 else: new_songs_df[col] = '' 
        # Reorder new_songs_df columns to match pml_df for clean concat
        new_songs_df = new_songs_df[pml_df.columns] 
        
        pml_df = pd.concat([pml_df, new_songs_df], ignore_index=True)
        # Drop duplicates just in case (should be handled by pml_codes set check)
        pml_df.drop_duplicates(subset=['code'], keep='first', inplace=True)
        
        # --- Apply basic cleaning from fix_pml_dataframe_main to new rows --- 
        # Standardize names again after concat
        pml_df.columns = [str(col).lower().replace(" ", "_").replace("-", "_") for col in pml_df.columns]
        # Fill NaNs introduced during concat or originally present
        for col in ['code', 'title', 'composer', 'event_name', 'publisher', 'arranger', 'specification']:
            if col in pml_df.columns:
                pml_df[col] = pml_df[col].fillna('').astype(str) 
        # Clean code format
        pml_df["code"] = pml_df["code"].str.strip().str.replace(".", "", regex=False)
        # Clean grade
        if 'grade' in pml_df.columns:
             pml_df["grade"] = pd.to_numeric(pml_df["grade"], errors='coerce').fillna(0).astype(int)
        else:
             pml_df["grade"] = 0
        # -----------------------------------------------------------------
    else:
        print("No new songs from results to add to PML DataFrame.")
    
    return pml_df
# -----------------------------------------

def add_performance_count(pml, df):
    print("\n--- Debugging add_performance_count ---")
    print(f"Initial row count in results df: {len(df)}")
    print(f"Initial unique codes in pml['code'] (sample): {pml['code'].unique()[:5]}")

    # if performance count is in pml, drop it
    if "performance_count" in pml.columns:
        pml = pml.drop(columns=["performance_count"])

    # make sure df code_n is str
    # Ensure columns exist before astype
    for col_name in ["code_1", "code_2", "code_3", "concert_final_score"]:
        if col_name not in df.columns:
            print(f"Warning: Column '{col_name}' not found in results DataFrame for add_performance_count. Filling with default.")
            if "code" in col_name: df[col_name] = ""
            else: df[col_name] = 0 # Default for score
    
    df["code_1"] = df["code_1"].astype(str)
    df["code_2"] = df["code_2"].astype(str)
    df["code_3"] = df["code_3"].astype(str)

    # make sure pml code is str
    if 'code' in pml.columns:
        pml["code"] = pml["code"].astype(str).str.strip()
    else:
        print("Error: 'code' column missing from PML DataFrame in add_performance_count.")
        pml["performance_count"] = 0 # Add empty column and return early
        return pml

    # Convert concert_final_score to numeric, coercing errors to NaN, then fill NaN with 0 before comparison
    df["concert_final_score"] = pd.to_numeric(df["concert_final_score"], errors='coerce').fillna(0)
    df_filtered = df[df["concert_final_score"] != 0]
    print(f"Row count in results df after filtering concert_final_score != 0: {len(df_filtered)}")

    if df_filtered.empty:
        print("Results df is empty after filtering by concert_final_score. Performance counts will be 0.")
        pml["performance_count"] = 0
        return pml

    code_counts = (
        df_filtered.groupby("code_1")
        .size()
        .add(df_filtered.groupby("code_2").size(), fill_value=0)
        .add(df_filtered.groupby("code_3").size(), fill_value=0)
    )

    code_counts = code_counts.dropna()
    if code_counts.empty:
        print("code_counts is empty after grouping and dropna. Performance counts will be 0.")
        pml["performance_count"] = 0
        return pml
        
    code_counts = code_counts.reset_index()
    code_counts.columns = ["code", "performance_count"]
    code_counts["code"] = code_counts["code"].astype(str)
    code_counts["performance_count"] = code_counts["performance_count"].astype(int)
    
    code_counts = code_counts.dropna(subset=["code"])
    # Ensure we don't try to merge on 'none' or 'not_found' or empty string as a valid code
    invalid_codes_for_counting = ['', 'none', 'not_found', 'nan'] # Added 'nan' just in case
    code_counts = code_counts[~code_counts['code'].str.lower().isin(invalid_codes_for_counting)]
    print(f"code_counts DataFrame head before merge (after filtering invalid codes):\n{code_counts.head()}")
    print(f"Unique codes in code_counts['code'] (sample): {code_counts['code'].unique()[:5]}")

    pml = pml.merge(code_counts, on="code", how="left")
    pml["performance_count"] = pml["performance_count"].fillna(0).astype(int)
    print("--- Finished add_performance_count ---\n")
    return pml


def add_average_score(pml, df):
    # Group df by "code" and calculate average score
    # get rid of any where concert score is 0
    df = df[df["concert_final_score"] != 0]
    # also sr
    df = df[df["sight_reading_final_score"] != 0]

    # make sure all scores are numeric
    df["concert_score_1"] = pd.to_numeric(df["concert_score_1"], errors="coerce")
    df["concert_score_2"] = pd.to_numeric(df["concert_score_2"], errors="coerce")
    df["concert_score_3"] = pd.to_numeric(df["concert_score_3"], errors="coerce")
    df["concert_final_score"] = pd.to_numeric(
        df["concert_final_score"], errors="coerce"
    )
    df["sight_reading_score_1"] = pd.to_numeric(
        df["sight_reading_score_1"], errors="coerce"
    )
    df["sight_reading_score_2"] = pd.to_numeric(
        df["sight_reading_score_2"], errors="coerce"
    )
    df["sight_reading_score_3"] = pd.to_numeric(
        df["sight_reading_score_3"], errors="coerce"
    )
    df["sight_reading_final_score"] = pd.to_numeric(
        df["sight_reading_final_score"], errors="coerce"
    )

    df["average_concert_score"] = (
        (df["concert_score_1"] + df["concert_score_2"] + df["concert_score_3"]) / 3
    )

    df["average_sight_reading_score"] = (
        (df["sight_reading_score_1"]
        + df["sight_reading_score_2"]
        + df["sight_reading_score_3"]) / 3
    )

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
        df.loc[df["event"].str.lower().str.contains("band", na=False), "gen_event"] = (
            "Band"
        )
        df.loc[
            df["event"].str.lower().str.contains("chorus", na=False), "gen_event"
        ] = "Chorus"
        df.loc[
            df["event"].str.lower().str.contains("orchestra", na=False), "gen_event"
        ] = "Orchestra"

        # drop any rows where all scores are na
        df = df.dropna(subset=score_subset, how="all")

        # force numeric scores to be numeric
        df[score_subset] = df[score_subset].apply(pd.to_numeric, errors="coerce")

        # fill everything else with ""
        cols_not_in_subset = df.columns.difference(score_subset)
        df[cols_not_in_subset] = df[cols_not_in_subset].fillna("")

        df.loc[:, "song_concat"] = (
            df["title_1"] + " " + df["title_2"] + " " + df["title_3"]
        )
        df.loc[:, "composer_concat"] = (
            df["composer_1"] + " " + df["composer_2"] + " " + df["composer_3"]
        )
        # remove any non-alphanumeric characters and spaces
        df.loc[:, "song_concat"] = df["song_concat"].str.replace(r"[^\w\s]", "")
        df.loc[:, "song_concat"] = df["song_concat"].str.replace(" ", "")

        df.loc[:, "composer_concat"] = df["composer_concat"].str.replace(r"[^\w\s]", "")
        df.loc[:, "composer_concat"] = df["composer_concat"].str.replace(r" ", "")

        # make all characters lowercase
        df.loc[:, "song_concat"] = df["song_concat"].str.lower()
        df.loc[:, "composer_concat"] = df["composer_concat"].str.lower()
        # fix school names
        df.loc[:, "school_search"] = df["school"].str.strip().str.lower()
        df.loc[:, "school_search"] = df["school_search"].str.replace(r"[^\w\s]", "")
        df.loc[:, "school_search"] = df["school_search"].str.replace(r" ", "")

        # fix event names
        # drop events wher not str
        df = df.dropna(subset=["event"])
        df["event"] = df["event"].astype(str)

        # # director search
        # df["director_search"] = df["director"].str.lower()
        # df["additional_director_search"] = df["additional_sirector"].str.lower()

        # drop any rows where contest_date is na
        df = df.dropna(subset=["contest_date"])
        
        # Apply the robust date parsing function - it now returns datetime objects or NaT
        df["contest_date"] = df["contest_date"].apply(parse_date_flexible)

        test_df = df[df["contest_date"].isna()] # .isna() correctly checks for NaT in datetime columns
        if not test_df.empty:
            print(f"Warning: Some rows have NaT contest_date after parsing attempts. Example indices: {test_df.index.tolist()[:5]}")

        # Extract the year and store it in a new column
        df["year"] = df["contest_date"].dt.year
        # drop where year is none (which NaT dates will produce as NaN year)
        df = df.dropna(subset=["year"])
        # Ensure year is integer first (to handle potential floats from NaN drop), then convert to string
        df["year"] = df["year"].astype(int).astype(str)

        # drop where scores are na
        df = df.dropna(subset=score_subset)

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
            (df["concert_score_1"] + df["concert_score_2"] + df["concert_score_3"]) / 3
        )
        # make sure sight reading is also correct
        df["average_sight_reading_score"] = (
            (df["sight_reading_score_1"]
            + df["sight_reading_score_2"]
            + df["sight_reading_score_3"]) / 3
        )

        df = df.dropna(subset=["average_concert_score"])

        # -- Modify grouping keys: Remove concert_judge_1 --
        event_group = df.groupby(["gen_event", "contest_date"])
        # join the scores together
        df = df.merge(
            event_group["average_concert_score"].mean(),
            on=["gen_event", "contest_date"],
            suffixes=("", "_mean"),
        )
        # --------------------------------------------------

        # if contest_average_concert_score exsists, drop it
        if "contest_average_concert_score" in df.columns:
            df = df.drop(columns=["contest_average_concert_score"])

        # rename new column
        df = df.rename(
            columns={"average_concert_score_mean": "contest_average_concert_score"}
        )

        return df

    df = adjust_results_df(df)

    # --- New Logic for Average Scores --- 
    # 1. Unpivot the results data for easier aggregation per song code
    df_melted_concert = pd.melt(df, 
        id_vars=['entry_number', 'average_concert_score'], 
        value_vars=['code_1', 'code_2', 'code_3'], 
        var_name='code_slot', 
        value_name='code')
    df_melted_sr = pd.melt(df, 
        id_vars=['entry_number', 'average_sight_reading_score'], 
        value_vars=['code_1', 'code_2', 'code_3'], 
        var_name='code_slot', 
        value_name='code')

    # 2. Filter out invalid codes and NaNs
    invalid_codes = ['', 'none', 'not_found', 'nan', None]
    df_melted_concert = df_melted_concert[
        df_melted_concert['code'].notna() & 
        ~df_melted_concert['code'].astype(str).str.lower().isin(invalid_codes) & 
        df_melted_concert['average_concert_score'].notna()
    ]
    df_melted_sr = df_melted_sr[
        df_melted_sr['code'].notna() & 
        ~df_melted_sr['code'].astype(str).str.lower().isin(invalid_codes) & 
        df_melted_sr['average_sight_reading_score'].notna()
    ]

    # 3. Calculate mean scores per code
    avg_concert_scores_per_code = df_melted_concert.groupby('code')['average_concert_score'].mean().reset_index()
    avg_sr_scores_per_code = df_melted_sr.groupby('code')['average_sight_reading_score'].mean().reset_index()

    # 4. Merge these averages into the pml DataFrame
    # Ensure pml code column is string for merge
    pml['code'] = pml['code'].astype(str)
    avg_concert_scores_per_code['code'] = avg_concert_scores_per_code['code'].astype(str)
    avg_sr_scores_per_code['code'] = avg_sr_scores_per_code['code'].astype(str)
    
    # Drop old score columns if they exist before merge
    pml = pml.drop(columns=['average_concert_score', 'average_sight_reading_score'], errors='ignore')
    
    pml = pd.merge(pml, avg_concert_scores_per_code, on='code', how='left')
    pml = pd.merge(pml, avg_sr_scores_per_code, on='code', how='left')

    # Round the merged scores
    pml['average_concert_score'] = pml['average_concert_score'].round(2)
    pml['average_sight_reading_score'] = pml['average_sight_reading_score'].round(2)
    # ------------------------------------

    # --- Delta Score Calculation (adjustments needed) ---
    # We need average_concert_score for this row from df, 
    # and contest_average_concert_score (which is average for the judge/event/date)
    # Need to recalculate delta_score based on merged avg scores
    
    # Calculate contest average score (average for the judge/event/date)
    # This part seems okay from adjust_results_df, df should have 'contest_average_concert_score'
    
    # Unpivot results again to get delta score per performance/code instance
    df_melted_delta = pd.melt(df, 
        id_vars=['entry_number', 'average_concert_score', 'contest_average_concert_score'], 
        value_vars=['code_1', 'code_2', 'code_3'], 
        var_name='code_slot', 
        value_name='code')
    
    # Filter invalid codes
    df_melted_delta = df_melted_delta[
        df_melted_delta['code'].notna() & 
        ~df_melted_delta['code'].astype(str).str.lower().isin(invalid_codes)
    ]
    
    # Calculate delta score for each performance instance
    df_melted_delta['delta_score_instance'] = df_melted_delta['contest_average_concert_score'] - df_melted_delta['average_concert_score']
    
    # --- Debug Print 3 --- 
    print("\nDebug: Sample of df_melted_delta before grouping:")
    print(df_melted_delta[['code', 'average_concert_score', 'contest_average_concert_score', 'delta_score_instance']].head())
    # Check specific 2025 codes if possible (e.g., 39184)
    if '39184' in df_melted_delta['code'].values:
        print("\nDebug: df_melted_delta rows for code 39184:")
        print(df_melted_delta[df_melted_delta['code'] == '39184'][['code', 'average_concert_score', 'contest_average_concert_score', 'delta_score_instance']])
    # ---------------------

    # Calculate the average delta_score per code
    avg_delta_score_per_code = df_melted_delta.groupby('code')['delta_score_instance'].mean().reset_index()
    avg_delta_score_per_code = avg_delta_score_per_code.rename(columns={'delta_score_instance': 'delta_score'})
    
    # --- Debug Print 1 --- 
    print("\nDebug: avg_delta_score_per_code head:")
    print(avg_delta_score_per_code.head())
    if not avg_delta_score_per_code[avg_delta_score_per_code['code'] == '39184'].empty:
        print("Debug: avg_delta_score_per_code contains code 39184:")
        print(avg_delta_score_per_code[avg_delta_score_per_code['code'] == '39184'])
    else:
        print("Debug: avg_delta_score_per_code does NOT contain code 39184")
    # ---------------------
    
    # Merge average delta score into pml
    avg_delta_score_per_code['code'] = avg_delta_score_per_code['code'].astype(str)
    pml = pml.drop(columns=['delta_score'], errors='ignore') # Drop old delta_score
    pml = pd.merge(pml, avg_delta_score_per_code, on='code', how='left')
    
    # --- Debug Print 2 --- 
    print("\nDebug: PML row for code 39184 AFTER merging delta_score (before fillna/round):")
    pml_39184_after_merge = pml[pml['code'] == '39184']
    if not pml_39184_after_merge.empty:
        print(pml_39184_after_merge[['code', 'delta_score', 'average_concert_score', 'average_sight_reading_score']])
    else:
        print("Debug: Code 39184 not found in PML after delta_score merge (this should not happen if fix_pml worked)")
    # ---------------------
        
    pml['delta_score'] = pml['delta_score'].round(2)
    # -------------------------------------------
    
    # --- Recalculate Percentiles and Song Score --- 
    # Fill NaN scores (resulting from left merges for songs with 0 performances) with 0 before ranking
    pml['delta_score'] = pml['delta_score'].fillna(0)
    pml['performance_count'] = pml['performance_count'].fillna(0).astype(int) # Ensure performance_count is filled and int

    # Only calculate percentiles where performance_count > 0
    mask_pc = pml["performance_count"] > 0
    if mask_pc.any(): # Check if any True values exist in mask
        pml.loc[mask_pc, "delta_score_percentile"] = pml.loc[mask_pc, "delta_score"].rank(pct=True)
        pml.loc[mask_pc, "performance_count_percentile"] = pml.loc[mask_pc, "performance_count"].rank(pct=True)
    else:
        pml["delta_score_percentile"] = 0.0
        pml["performance_count_percentile"] = 0.0
        
    # FillNa for percentiles just in case rank produced NaNs (shouldn't if based on non-NaN data)
    pml["delta_score_percentile"] = pml["delta_score_percentile"].fillna(0)
    pml["performance_count_percentile"] = pml["performance_count_percentile"].fillna(0)

    # Choose weights 
    performance_weight = 0.5
    delta_weight = 1

    # Calculate song_score where performance_count > 0
    pml["song_score"] = 0.0 # Initialize column
    if mask_pc.any():
        pml.loc[mask_pc, "song_score"] = (
            pml.loc[mask_pc, "delta_score_percentile"] * delta_weight
            + pml.loc[mask_pc, "performance_count_percentile"] * performance_weight
        ) / (delta_weight + performance_weight) # Normalize by sum of weights

    # fillna with 0 (shouldn't be needed if initialized and calculated correctly)
    # pml["song_score"] = pml["song_score"].fillna(0) 

    # Check if there is data to scale before applying MinMaxScaler
    song_scores_to_scale = pml.loc[mask_pc, "song_score"]
    if not song_scores_to_scale.empty:
        scaler = MinMaxScaler(feature_range=(0, 100))
        pml.loc[mask_pc, "song_score"] = scaler.fit_transform(
            song_scores_to_scale.values.reshape(-1, 1)
        )
    # else: # Already initialized to 0.0
    #    print("Warning: No songs with performance_count > 0 found. Skipping song_score scaling.")

    # --- Earliest Year Calculation (adjustments needed) ---
    # Unpivot results again to get year per code instance
    df_melted_year = pd.melt(df, 
        id_vars=['entry_number', 'year'], 
        value_vars=['code_1', 'code_2', 'code_3'], 
        var_name='code_slot', 
        value_name='code')
        
    # Filter invalid codes
    df_melted_year = df_melted_year[
        df_melted_year['code'].notna() & 
        ~df_melted_year['code'].astype(str).str.lower().isin(invalid_codes) &
        df_melted_year['year'].notna()
    ]
    
    # Group by code and find the min year
    earliest_year_per_code = df_melted_year.groupby('code')['year'].min().reset_index()
    earliest_year_per_code = earliest_year_per_code.rename(columns={'year': 'earliest_year'})
    
    # Merge earliest year into pml
    earliest_year_per_code['code'] = earliest_year_per_code['code'].astype(str)
    pml = pml.drop(columns=['earliest_year'], errors='ignore') # Drop old earliest_year
    pml = pd.merge(pml, earliest_year_per_code, on='code', how='left')
    # Convert to integer year, handling potential NaNs from merge
    pml['earliest_year'] = pd.to_numeric(pml['earliest_year'], errors='coerce').astype('Int64') # Use nullable integer type
    # -------------------------------------------------------
    
    # --- Debug Print 4: Before Final Cleanup ---
    print("\nDebug: PML row for code 39184 BEFORE final cleanup in add_average_score:")
    pml_39184_before_cleanup = pml[pml['code'] == '39184']
    if not pml_39184_before_cleanup.empty:
        cols_to_check = ['code', 'average_concert_score', 'average_sight_reading_score', 
                         'delta_score', 'delta_score_percentile', 'performance_count_percentile', 
                         'song_score', 'earliest_year', 'performance_count']
        print(pml_39184_before_cleanup[[col for col in cols_to_check if col in pml_39184_before_cleanup.columns]])
    else:
        print("Debug: Code 39184 not found in PML before final cleanup.")
    # ------------------------------------------

    # --- Final Cleanup of Column Types and NaNs --- 
    numeric_cols_to_fill = ['average_concert_score', 'average_sight_reading_score', 'delta_score', 'song_score']
    for col in numeric_cols_to_fill:
        if col in pml.columns:
            pml[col] = pd.to_numeric(pml[col], errors='coerce') # Ensure numeric
            pml[col] = pml[col].fillna(0.0) # Fill NaN with float 0.0
        else:
            pml[col] = 0.0 # Add column if missing, filled with 0.0
            
    # Ensure int types where appropriate
    pml['performance_count'] = pml['performance_count'].fillna(0).astype(int)
    pml['grade'] = pml['grade'].fillna(0).astype(int)
    # ---------------------------------------------
    
    print("Finished recalculating scores and stats in add_average_score.")
    return pml, df


def main():
    db_path = "uil.db"
    conn = sqlite3.connect(db_path)
    print("Loading data from uil.db...")
    
    initial_results_columns = []
    try:
        # Get original column names first
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(results)")
        initial_results_columns = [col_info[1] for col_info in cursor.fetchall()]
        if not initial_results_columns:
             raise ValueError("Could not read columns from results table.")
             
        df = pd.read_sql_query("SELECT * FROM results", conn)
        df.columns = [str(col).lower().replace(" ", "_").replace("-", "_") for col in df.columns] # Standardize on load
        # Ensure original column names are also standardized for later selection
        initial_results_columns = [str(col).lower().replace(" ", "_").replace("-", "_") for col in initial_results_columns]

    except Exception as e:
        print(f"Error loading results table: {e}. Exiting.")
        conn.close()
        return

    try:
        pml = pd.read_sql_query("SELECT * FROM pml", conn)
        pml.columns = [str(col).lower().replace(" ", "_").replace("-", "_") for col in pml.columns] # Standardize on load
    except Exception as e:
        print(f"Error loading pml table: {e}. Exiting.")
        conn.close()
        return

    conn.close() # Close connection after loading
    
    if df.empty:
        print("Loaded results DataFrame is empty. Cannot proceed.")
        return
    # PML might be initially empty if created by fix_pml, that's ok

    # --- Step 1: Integrate New Songs into PML DataFrame --- 
    pml = integrate_new_songs_from_results(pml, df)
    # ---------------------------------------------------

    # --- Step 2: Calculate PML Stats (Performance Count, Average Scores, etc.) ---
    print("Calculating performance counts for PML...")
    pml = add_performance_count(pml, df.copy()) # Pass a copy of df
    
    print("Calculating average scores and other stats for PML...")    
    pml_updated, df_processed = add_average_score(pml, df.copy()) # Pass a copy of df
    # -------------------------------------------------------------------------

    # --- Debug Print 5: After returning from add_average_score ---
    print("\nDebug: pml_updated row for code 39184 AFTER returning from add_average_score (before save):")
    pml_39184_after_return = pml_updated[pml_updated['code'] == '39184']
    if not pml_39184_after_return.empty:
        cols_to_check = ['code', 'average_concert_score', 'average_sight_reading_score', 
                         'delta_score', 'delta_score_percentile', 'performance_count_percentile', 
                         'song_score', 'earliest_year', 'performance_count']
        print(pml_39184_after_return[[col for col in cols_to_check if col in pml_39184_after_return.columns]])
    else:
        print("Debug: Code 39184 not found in pml_updated after return from add_average_score.")
    # -----------------------------------------------------------

    # --- Step 3: Calculate Performance Delta Score for Results DataFrame --- 
    # Ensure necessary columns exist from adjust_results_df run inside add_average_score
    if 'average_concert_score' in df_processed.columns and 'contest_average_concert_score' in df_processed.columns:
        print("Calculating performance_delta_score for results data...")
        df_processed['performance_delta_score'] = df_processed['contest_average_concert_score'] - df_processed['average_concert_score']
        df_processed['performance_delta_score'] = df_processed['performance_delta_score'].round(2).fillna(0) # Round and fill NaN
        new_results_col = 'performance_delta_score'
    else:
        print("Warning: Could not calculate performance_delta_score. Required columns missing from df_processed.")
        new_results_col = None # No new column to add
    # -----------------------------------------------------------------------

    # --- Step 3.5: Clean Event Names in final PML --- 
    if 'event_name' in pml_updated.columns:
        print("Cleaning event_name column in final PML DataFrame...")
        pml_updated['event_name'] = pml_updated['event_name'].apply(clean_event_name)
    else:
        print("Warning: 'event_name' column not found in pml_updated before saving.")
    # -----------------------------------------------
    
    # --- Step 4: Save Updated PML Table --- 
    conn = sqlite3.connect(db_path)
    try:
        print("Saving updated PML table to uil.db...")
        pml_updated.to_sql("pml", conn, if_exists="replace", index=False)
        print("PML table updated successfully.")
    except Exception as e:
        print(f"Error saving updated PML table: {e}")
    finally:
        if conn: conn.close() # Ensure connection is closed
    # -----------------------------------

    # --- Step 5: Save Updated Results Table --- 
    # Select original columns + the new delta score column
    columns_to_save = initial_results_columns
    if new_results_col and new_results_col not in columns_to_save:
        columns_to_save.append(new_results_col)
        
    # Ensure all columns to save actually exist in df_processed
    columns_to_save = [col for col in columns_to_save if col in df_processed.columns]
    df_to_save = df_processed[columns_to_save]
    
    conn = sqlite3.connect(db_path)
    try:
        print(f"Saving updated results table to {db_path} (Columns: {columns_to_save})...")
        df_to_save.to_sql("results", conn, if_exists="replace", index=False)
        print("Results table updated successfully with performance_delta_score.")
    except Exception as e:
        print(f"Error saving updated results table: {e}")
    finally:
        if conn: conn.close() # Ensure connection is closed
    # ---------------------------------------

if __name__ == "__main__":
    main()
    print("add_new_performance_data.py finished.")
