import pandas as pd
import os
import sqlite3
from datetime import datetime

# This function will now be responsible for adding new songs to the PML DataFrame
# based on what's in the results table but not in the current PML.
def integrate_new_songs_from_results(pml_df, results_df):
    print("Integrating new songs into PML from results...")
    pml_codes = set(pml_df['code'].astype(str).str.strip())
    new_songs_data = []

    # Standardize column names in results_df for consistency here
    # (results_df from DB should already be standardized, but good practice if source varied)
    results_df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in results_df.columns]

    # Define expected columns in results for song info
    song_info_map = {
        'code_1': ('title_1', 'composer_1'),
        'code_2': ('title_2', 'composer_2'),
        'code_3': ('title_3', 'composer_3'),
    }

    for index, row in results_df.iterrows():
        event_name = row.get('event', '') # Get event from results row
        for code_col, (title_col, composer_col) in song_info_map.items():
            song_code = str(row.get(code_col, '')).strip()
            # Check if song_code is not empty, not 'none', not 'not_found', and not already in pml_codes
            if song_code and song_code.lower() not in ['', 'none', 'not_found'] and song_code not in pml_codes:
                song_title = row.get(title_col, 'Untitled')
                song_composer = row.get(composer_col, 'Unknown Composer')
                
                new_song_record = {
                    'code': song_code,
                    'title': song_title,
                    'composer': song_composer,
                    'event_name': event_name, # Event from the performance
                    # Add other default PML columns as None or default values if your PML schema needs them
                    # e.g., 'grade': 0, 'publisher': 'Unknown', etc.
                }
                new_songs_data.append(new_song_record)
                pml_codes.add(song_code) # Add to set to avoid duplicate additions from results
    
    if new_songs_data:
        new_songs_df = pd.DataFrame(new_songs_data)
        pml_df = pd.concat([pml_df, new_songs_df], ignore_index=True)
        # It's crucial to drop duplicates again by 'code' after concat if new_songs_df might have overlaps
        # or if the original pml_df could somehow get a duplicate code before this step.
        pml_df.drop_duplicates(subset=['code'], keep='first', inplace=True)
        print(f"Added {len(new_songs_df)} new unique song entries to PML DataFrame.")
    else:
        print("No new songs from results to add to PML DataFrame.")
    return pml_df


def fix_pml_dataframe_main(pml_df):
    """Main PML cleaning and standardization function."""
    pml = pml_df.copy()

    # Standardize all column names to lowercase_with_underscore first
    pml.columns = [str(col).lower().replace(" ", "_").replace("-", "_") for col in pml.columns]

    # Ensure essential columns exist, add if not (with default values)
    # This is important if new songs were added with a minimal set of columns
    ensure_pml_columns = {
        'code': '', 'title': 'Untitled', 'composer': 'Unknown Composer', 'event_name': '',
        'grade': 0, 'publisher': '', 'arranger': '', 'specification': '', 
        # Add any other columns your PML typically has and their defaults
        # Statistics columns will be added by add_new_performance_data.py
    }
    for col, default_val in ensure_pml_columns.items():
        if col not in pml.columns:
            pml[col] = default_val
            print(f"Added missing essential column '{col}' to PML with default: {default_val}")

    # Fill NaN values in key text columns before string operations
    for col in ['code', 'title', 'composer', 'event_name', 'publisher', 'arranger', 'specification']:
        if col in pml.columns:
            pml[col] = pml[col].fillna(ensure_pml_columns.get(col, '')).astype(str) # Fill with default then ensure string
        else: # Should have been created above, but as safeguard
             pml[col] = ensure_pml_columns.get(col, '')

    # Clean 'code' column
    pml["code"] = pml["code"].str.strip().str.replace(".", "", regex=False)
    # Example: if codes look like 'UIL-123A', take '123A'
    # This specific logic might need adjustment based on actual code patterns
    if not pml.empty and '-' in pml["code"].iloc[0]: # Check if first entry has a -
        pml["code"] = pml["code"].str.split("-").str[-1]
    pml = pml[pml["code"].str.lower().isin(['', 'none', 'not_found']) == False] # Remove invalid/placeholder codes
    pml = pml[pml["code"].notna() & (pml["code"] != "")] # Ensure no empty or NaN codes remain
    pml.drop_duplicates(subset=['code'], keep='first', inplace=True) # Final deduplication on code

    # Rename specific columns if needed (example from original script)
    if "publisher_[collection]" in pml.columns:
        pml.rename(columns={"publisher_[collection]": "publisher"}, inplace=True)

    # Clean 'grade' column
    pml["grade"] = pd.to_numeric(pml["grade"], errors="coerce").fillna(0).astype(int)
    # pml = pml[pml["grade"] != 0] # Original script removed grade 0, consider if this is desired

    # Clean 'event_name'
    pml["event_name"] = pml["event_name"].str.replace("/", "-", regex=False).str.lower().str.strip()
    
    print("PML DataFrame cleaning and standardization complete.")
    return pml


def main():
    db_path = "uil.db"
    conn = sqlite3.connect(db_path)

    print(f"Loading PML data from {db_path}...")
    try:
        pml_df = pd.read_sql_query("SELECT * FROM pml", conn)
        # Standardize pml_df columns on load to handle any case discrepancies from DB
        pml_df.columns = [str(col).lower().replace(" ", "_").replace("-", "_") for col in pml_df.columns]
        if 'code' not in pml_df.columns:
             print("Critical error: 'code' column not found in pml table from database.")
             return
    except Exception as e:
        print(f"Error loading PML table: {e}. Creating an empty PML DataFrame with essential columns.")
        pml_df = pd.DataFrame(columns=['code', 'title', 'composer', 'event_name']) # Must have at least 'code'

    print(f"Loading results data from {db_path} for new song integration...")
    try:
        results_df = pd.read_sql_query("SELECT * FROM results", conn)
    except Exception as e:
        print(f"Error loading results table: {e}. Cannot integrate new songs.")
        results_df = pd.DataFrame() # Empty df if results can't be loaded

    conn.close()

    if not results_df.empty:
        # Step 1: Integrate new songs from results into the pml_df
        pml_df = integrate_new_songs_from_results(pml_df, results_df)
    else:
        print("Results DataFrame is empty, skipping new song integration.")

    # Step 2: Perform main cleaning and standardization on the (potentially expanded) pml_df
    pml_df_fixed = fix_pml_dataframe_main(pml_df)

    # Save the fixed PML back to the database
    if not pml_df_fixed.empty:
        conn = sqlite3.connect(db_path)
        try:
            print(f"Saving updated PML table to {db_path}...")
            pml_df_fixed.to_sql("pml", conn, if_exists="replace", index=False)
            print("PML table updated successfully.")
        except Exception as e:
            print(f"Error saving PML table: {e}")
        finally:
            conn.close()
    else:
        print("Fixed PML DataFrame is empty, not saving to database.")

if __name__ == "__main__":
    main()
    print("fix_pml.py script finished.")
