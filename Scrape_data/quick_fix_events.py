import pandas as pd
import sqlite3
import os
import re

print(f"Current working directory: {os.getcwd()}")

def clean_event_name(event_name):
    """Standardizes the event name string."""
    if pd.isna(event_name):
        return "" # Return empty string for NaN/None
    
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

def fix_events_in_results(db_path="uil.db"):
    """Reads results table, fixes event name formats, saves back."""
    
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
            print("Results table is empty. No events to fix.")
            return
            
        if 'event' not in df.columns:
            print("Error: 'event' column not found in results table.")
            return

        print("Applying event name cleaning to 'event' column...")
        # Apply the cleaning function
        original_events = df['event'].unique()[:10] # Sample before cleaning
        df['event'] = df['event'].apply(clean_event_name)
        cleaned_events = df['event'].unique()[:10] # Sample after cleaning
        
        print(f"Sample original event names: {original_events}")
        print(f"Sample cleaned event names: {cleaned_events}")
        
        print("Replacing results table with fixed event names...")
        # Replace the entire table with the corrected DataFrame
        df.to_sql("results", conn, if_exists="replace", index=False)
        print("Successfully updated event names in results table.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing database connection.")
        conn.close()

if __name__ == "__main__":
    print("Running quick_fix_events script...")
    fix_events_in_results()
    print("quick_fix_events script finished.") 