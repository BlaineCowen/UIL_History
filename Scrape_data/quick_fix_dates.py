import pandas as pd
import sqlite3
import os

print(f"Current working directory: {os.getcwd()}")

def parse_date_flexible(date_str):
    """Attempts to parse a date string using multiple formats."""
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
    # print(f"Warning: Date '{date_str}' could not be parsed by any known format.") # Can uncomment for debugging
    return pd.NaT # Return NaT if all formats fail

def fix_dates_in_results(db_path="uil.db"):
    """Reads results table, fixes date formats, saves back."""
    
    print(f"Connecting to database: {db_path}")
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return
        
    conn = sqlite3.connect(db_path)
    
    try:
        print("Reading results table...")
        df = pd.read_sql_query("SELECT * FROM results", conn)
        print(f"Read {len(df)} rows from results table.")
        
        if df.empty:
            print("Results table is empty. No dates to fix.")
            return
            
        if 'contest_date' not in df.columns:
            print("Error: 'contest_date' column not found in results table.")
            return

        print("Applying flexible date parsing to 'contest_date'...")
        # Apply the parsing function
        df['contest_date_parsed'] = df['contest_date'].apply(parse_date_flexible)
        
        # Optional: Report how many dates failed parsing
        failed_count = df['contest_date_parsed'].isna().sum()
        if failed_count > 0:
            print(f"Warning: {failed_count} dates could not be parsed and were set to NaT.")

        # --- Assign the parsed datetime objects (or NaT) directly --- 
        print("Assigning parsed datetime objects back to contest_date column...")
        df['contest_date'] = df['contest_date_parsed']
        # --------------------------------------------------------------
        
        # Drop the temporary column
        df = df.drop(columns=['contest_date_parsed'])

        print("Replacing results table with fixed dates (datetime objects serialized by pandas)...")
        # Replace the entire table with the corrected DataFrame
        df.to_sql("results", conn, if_exists="replace", index=False)
        print("Successfully updated dates in results table.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing database connection.")
        conn.close()

if __name__ == "__main__":
    print("Running quick_fix_dates script...")
    fix_dates_in_results()
    print("quick_fix_dates script finished.") 