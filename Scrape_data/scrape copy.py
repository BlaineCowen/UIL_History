##disable-gpu

import requests
import pandas as pd
import sqlite3
import time
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import sqlite3
import os
import pandas as pd
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re # Add re import

def parse_date_flexible(date_str):
    # Function copied from add_new_performance_data.py
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
    # print(f"Warning: Date '{date_str}' could not be parsed by any known format.")
    return pd.NaT # Return NaT if all formats fail

def clean_event_name(event_name):
    """Standardizes the event name string. (Copied from quick_fix_events.py)"""
    if pd.isna(event_name):
        return "" 
    event_str = str(event_name)
    event_str = re.sub(r"^\d+-", "", event_str)
    event_str = event_str.lower()
    event_str = event_str.replace("/", "-")
    event_str = event_str.strip()
    return event_str

# loops of csv files in csv_downloads folder
def loop_csv():
    # Define the standard master columns using lowercase_with_underscore
    master_df_columns = [
        "contest_date", "event", "region", "school", "tea_code", "city", 
        "director", "additional_director", "accompanist", "conference", 
        "classification", "non_varsity_group", "entry_number", "title_1", 
        "composer_1", "title_2", "composer_2", "title_3", "composer_3", 
        "concert_judge", "concert_judge", "concert_judge", "concert_score_1", 
        "concert_score_2", "concert_score_3", "concert_final_score", 
        "sight_reading_judge", "sight_reading_judge", "sight_reading_judge", 
        "sight_reading_score_1", "sight_reading_score_2", "sight_reading_score_3", 
        "sight_reading_final_score", "award"
    ]
    # The database uses "Entry Number" with space, but new CSVs might too. Standardize early.
    # For database interaction, we'll use the exact DB column names (which user says are lowercase_with_underscore)
    # For pandas operations, we will use the standardized names.
    standardized_entry_number_col = "entry_number" # Key for deduplication

    # Process files from csv_downloads
    path = "scrape_data/csv_downloads"
    if not os.path.exists(path):
        print(f"Directory {path} does not exist. No CSVs to process.")
        return
    
    files = [f for f in os.listdir(path) if f.endswith(".csv")]
    if not files:
        print(f"No CSV files found in {path} to process.")
        return

    new_data_frames = []
    for file in files:
        try:
            df_temp = pd.read_csv(os.path.join(path, file), header=2, encoding="iso-8859-1")
            if not df_temp.empty:
                # Standardize column names immediately after reading
                df_temp.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df_temp.columns]
                # --- Clean event name right after standardizing columns --- 
                if 'event' in df_temp.columns:
                    df_temp['event'] = df_temp['event'].apply(clean_event_name)
                # ---------------------------------------------------------
                new_data_frames.append(df_temp)
        except Exception as e:
            print(f"Error reading or processing file {file}: {e}")

    if not new_data_frames:
        print("No data loaded from CSV files in csv_downloads.")
        return

    df_from_downloads = pd.concat(new_data_frames, ignore_index=True)
    
    if standardized_entry_number_col not in df_from_downloads.columns:
        print(f"Error: Standardized '{standardized_entry_number_col}' column not found in downloaded CSV data. Available: {df_from_downloads.columns.tolist()}. Cannot proceed.")
        return
    
    df_from_downloads[standardized_entry_number_col] = df_from_downloads[standardized_entry_number_col].astype(str)
    df_from_downloads.drop_duplicates(subset=[standardized_entry_number_col], inplace=True)

    conn = sqlite3.connect("uil.db")
    existing_entry_numbers = set()
    db_column_names_exact = [] # To store exact column names from DB for SQL query

    try:
        cursor = conn.cursor()
        # Get exact column names from DB for the SELECT query, especially for "Entry Number"
        # Assuming the table is named 'results' and user confirmed its columns are lowercase_with_underscore
        # So, entry_number is what we expect.
        cursor.execute(f"SELECT {standardized_entry_number_col} FROM results") # Use standardized for the query if DB uses it
        # However, if the DB has "Entry Number" with space, we need to query that exact name.
        # Let's get actual column names first to be safe.
        pragma_cursor = conn.cursor()
        pragma_cursor.execute("PRAGMA table_info(results)")
        db_columns_info_for_select = pragma_cursor.fetchall()
        
        actual_db_entry_number_col = standardized_entry_number_col # Default if DB uses standardized
        if db_columns_info_for_select:
            db_column_names_exact = [col_info[1] for col_info in db_columns_info_for_select]
            # Find the version of "Entry Number" that the DB actually uses
            for name in db_column_names_exact:
                if name.lower().replace(' ', '_').replace('-', '_') == standardized_entry_number_col:
                    actual_db_entry_number_col = name
                    break
        
        # Now query using the actual column name found in the DB
        existing_entries_df = pd.read_sql_query(f'SELECT DISTINCT "{actual_db_entry_number_col}" FROM results', conn)
        if not existing_entries_df.empty:
            existing_entry_numbers = set(existing_entries_df[actual_db_entry_number_col].astype(str))
            
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError) as e:
        print(f"Results table might not exist or other DB error fetching entry numbers: {e}. Assuming no existing entries.")

    # Filter df_from_downloads to get only new entries
    df_to_append_to_db = df_from_downloads[~df_from_downloads[standardized_entry_number_col].isin(existing_entry_numbers)]

    if df_to_append_to_db.empty:
        print("No new unique entries to append to the database from csv_downloads.")
    else:
        print(f"Found {len(df_to_append_to_db)} new unique entries to append to the database.")
        
        # --- Standardize date format before DB append --- 
        if 'contest_date' in df_to_append_to_db.columns:
            print("Standardizing contest_date format before DB append...")
            df_to_append_to_db['contest_date'] = df_to_append_to_db['contest_date'].apply(parse_date_flexible)
            # Convert to datetime objects (parse_date_flexible should already do this)
            # Coerce errors just in case apply result is mixed or has issues
            df_to_append_to_db['contest_date'] = pd.to_datetime(df_to_append_to_db['contest_date'], errors='coerce')
        else:
            print("Warning: 'contest_date' column not found in data being appended to DB.")
        # --------------------------------------------------
        
        # Align columns of df_to_append_to_db to match the DB schema (which is db_column_names_exact)
        if db_column_names_exact: # If table exists and we have its column names
            # We need to map our standardized DataFrame column names to the exact DB column names for reindexing
            # The df_to_append_to_db already has standardized names.
            # The db_column_names_exact are the target. These are already lowercase_with_underscore as per user.
            final_df_for_db = df_to_append_to_db.reindex(columns=db_column_names_exact)
        else: # Table likely doesn't exist, use df_to_append_to_db columns (which are standardized)
            final_df_for_db = df_to_append_to_db
            # If creating the table, ensure master_df_columns are used if we want a specific schema from start
            # For now, if table is new, it will take schema of final_df_for_db (i.e. df_to_append_to_db)

        if not final_df_for_db.empty:
            try:
                final_df_for_db.to_sql("results", conn, if_exists="append", index=False)
                print(f"Successfully appended {len(final_df_for_db)} rows to the 'results' table.")
            except Exception as e_sql:
                print(f"Error appending to SQL: {e_sql}")
                print(f"DataFrame columns being appended: {final_df_for_db.columns.tolist()}")
                if db_column_names_exact:
                    print(f"Target DB columns: {db_column_names_exact}")
        else:
            print("No data to append after aligning columns with existing database schema (if any).")

    conn.close()

    # Update all_results.csv
    all_results_path = "all_results.csv"
    df_all_results = pd.DataFrame(columns=master_df_columns) # Start with master_df_columns schema

    if os.path.exists(all_results_path):
        try:
            df_temp_all_results = pd.read_csv(all_results_path, encoding="iso-8859-1", low_memory=False, dtype=str)
            if not df_temp_all_results.empty:
                df_temp_all_results.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df_temp_all_results.columns]
                # --- Standardize date format when reading all_results.csv --- 
                if 'contest_date' in df_temp_all_results.columns:
                    df_temp_all_results['contest_date'] = df_temp_all_results['contest_date'].apply(parse_date_flexible)
                    df_temp_all_results['contest_date'] = pd.to_datetime(df_temp_all_results['contest_date'], errors='coerce')
                # ------------------------------------------------------------
                # --- Clean event name when reading all_results.csv --- 
                if 'event' in df_temp_all_results.columns:
                     df_temp_all_results['event'] = df_temp_all_results['event'].apply(clean_event_name)
                # ---------------------------------------------------
                # Reindex to ensure master_df_columns order and presence, then concat
                df_all_results = pd.concat([df_all_results, df_temp_all_results.reindex(columns=master_df_columns)], ignore_index=True)
        except Exception as e_csv_read:
            print(f"Error reading existing {all_results_path}: {e_csv_read}. It might be recreated or started fresh.")
    else:
        print(f"{all_results_path} does not exist. It will be created with standard headers if new data is added.")

    if not df_to_append_to_db.empty:
        # Data to append to CSV should also use standardized master_df_columns
        df_for_all_results_csv = df_to_append_to_db.reindex(columns=master_df_columns)
        # Date format in df_to_append_to_db was already fixed before DB append, so df_for_all_results_csv has correct dates
        
        df_all_results = pd.concat([df_all_results, df_for_all_results_csv], ignore_index=True)
        if standardized_entry_number_col in df_all_results.columns and not df_all_results.empty:
             df_all_results.drop_duplicates(subset=[standardized_entry_number_col], keep='last', inplace=True)
        
        # Ensure all columns in master_df_columns are present before saving, fill with NaN if necessary
        df_all_results = df_all_results.reindex(columns=master_df_columns)
        df_all_results.to_csv(all_results_path, index=False)
        print(f"Updated {all_results_path} with new entries, standardized columns, and deduplicated.")
    else:
        print(f"No new entries were appended to the database, so {all_results_path} was not modified with new data (or was just created with headers).")
        if not os.path.exists(all_results_path): # if it wasn't modified but also didn't exist, create it with headers
            df_all_results.to_csv(all_results_path, index=False)


# delete csv files in csv_downloads folder
def delete_csv():
    dir_path = os.path.dirname(os.path.realpath(__file__))

    path = os.path.join(dir_path, "csv_downloads")
    files = os.listdir(path)
    for file in files:
        os.remove(os.path.join(path, file))


def load_url(url, timeout):
    ans = requests.head(url, timeout=timeout)
    return ans


def get_uil_results():
    # Get the absolute path of the directory the script is in
    dir_path = os.path.dirname(os.path.realpath(__file__))
    base_url = "https://www.texasmusicforms.com/csrrptUILpublic.asp"

    service = Service()
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": os.path.join(dir_path, "./csv_downloads"),
    }
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Load already processed contests
    processed_log_file = "scrape_data/processed_contests_log.txt"
    processed_contests = set()
    try:
        with open(processed_log_file, "r") as f:
            for line in f:
                line = line.strip()
                # Only add lines that are purely contest_ids (no comments/errors)
                if line and not "#" in line:
                    processed_contests.add(line)
    except FileNotFoundError:
        # If the log file doesn't exist, create an empty one
        with open(processed_log_file, "w") as f:
            f.write("")
    # chrome_options.add_argument("--headless") # Optional: run headless

    target_year = "2025"

    driver.get(base_url)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "yr")))

    # Select Year
    year_select_element = Select(driver.find_element(By.NAME, "yr"))
    year_select_element.select_by_value(target_year)
    # Year selection might trigger a page reload or content update, wait if necessary
    # For this site, it seems selections don't auto-submit until event or a button press

    # Get Region options
    region_select_element = Select(driver.find_element(By.NAME, "reg"))
    region_options = [opt.get_attribute("value") for opt in region_select_element.options if opt.get_attribute("value") != "0"]

    for region_value in region_options:
        # Navigate back to base or ensure form is fresh if needed, then re-select year and current region
        driver.get(base_url) # Start fresh for each region to ensure clean state
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "yr")))
        
        Select(driver.find_element(By.NAME, "yr")).select_by_value(target_year)
        
        region_select = Select(driver.find_element(By.NAME, "reg"))
        region_select.select_by_value(region_value)
        print(f"Selected Year: {target_year}, Region: {region_value}")
        # Region selection doesn't auto-submit form f1

        # Get Event options
        event_select_element = Select(driver.find_element(By.NAME, "ev"))
        event_options = [opt.get_attribute("value") for opt in event_select_element.options if opt.get_attribute("value")] # Non-empty values

        for event_value in event_options:
            # Re-select year and region, then select event
            # This is because selecting an event submits form f1 and reloads part of the page or the whole page.
            driver.get(base_url) 
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, "yr")))
            Select(driver.find_element(By.NAME, "yr")).select_by_value(target_year)
            Select(driver.find_element(By.NAME, "reg")).select_by_value(region_value)
            
            event_select = Select(driver.find_element(By.NAME, "ev"))
            event_select.select_by_value(event_value)
            
            # Wait for the contest dropdown ELEMENT to be present
            try:
                WebDriverWait(driver, 15).until( # Slightly shorter wait for the element itself
                    EC.presence_of_element_located((By.NAME, "cn"))
                )
            except TimeoutException:
                print(f"  Timeout waiting for 'cn' (contest dropdown element) to appear for Y:{target_year} R:{region_value} E:{event_value}. Skipping this event.")
                continue # To next event_value

            print(f"  Selected Event: {event_value} for Y:{target_year} R:{region_value}. Checking for available contests...")
            
            contest_options = [] # Initialize contest_options
            try:
                # Now, wait for the 'cn' dropdown to have actual contest options (not just a placeholder with empty value)
                WebDriverWait(driver, 10).until( # Shorter timeout for options to appear
                    lambda d: len(Select(d.find_element(By.NAME, "cn")).options) > 1 or \
                              (len(Select(d.find_element(By.NAME, "cn")).options) == 1 and Select(d.find_element(By.NAME, "cn")).first_selected_option.get_attribute("value") != "")
                )
                
                contest_select_element = Select(driver.find_element(By.NAME, "cn"))
                contest_options = [opt.get_attribute("value") for opt in contest_select_element.options if opt.get_attribute("value")] # Filter out empty values

                if not contest_options:
                    print(f"    No actual contests listed for Y:{target_year} R:{region_value} E:{event_value} (dropdown might be empty or only placeholder after check). Skipping event.")
                    continue # To next event_value

            except TimeoutException:
                print(f"    Timeout: No actual contests became available (dropdown did not populate with valid options) for Y:{target_year} R:{region_value} E:{event_value}. Skipping event.")
                continue # To next event_value
            
            if not contest_options: # Should be redundant due to checks above, but as a final safeguard
                print(f"    No contests to process for Y:{target_year} R:{region_value} E:{event_value} after all checks. Skipping event.")
                continue

            print(f"    Found {len(contest_options)} contest(s) for Y:{target_year} R:{region_value} E:{event_value}. Processing them...")
            for contest_cn_value in contest_options:
                contest_id = f"{target_year}_reg{region_value}_evt{event_value}_cn{contest_cn_value}"
                if contest_id in processed_contests:
                    print(f"    Skipping already processed contest: {contest_id}")
                    continue
                
                print(f"    Attempting contest: {contest_id}")
                
                try:
                    # Step 1: Select the specific contest from the dropdown (cn)
                    current_contest_select_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "cn")))
                    current_contest_select = Select(current_contest_select_element)
                    current_contest_select.select_by_value(contest_cn_value)
                    print(f"      Selected Contest CN from dropdown: {contest_cn_value}")

                    # Step 2: Submit the form f3 to go to the contest results page
                    form_f3 = driver.find_element(By.NAME, "f3")
                    form_f3.submit()
                    print(f"      Submitted form for contest {contest_id}")

                    # Step 3: Look for and click the export button
                    WebDriverWait(driver, 10).until( # Shorter timeout for export button
                        EC.presence_of_element_located((By.XPATH, '//*[@id="export-csv"]'))
                    ).click()
                    print(f"        Clicked export for {contest_id}. CSV should download.")
                    
                    # If successful, log it as completed
                    with open(processed_log_file, "a") as f:
                        f.write(contest_id + "\n")
                    processed_contests.add(contest_id) # Add to set for current run
                    time.sleep(5) # Wait for download to complete

                except (TimeoutException, NoSuchElementException) as e_export:
                    print(f"        No export button found or page issue for {contest_id}: {type(e_export).__name__}. Will retry if script runs again or loops.")
                    # DO NOT log to processed_contests_log.txt or add to processed_contests set on error

                except Exception as e_general:
                    print(f"        An unexpected error occurred processing contest {contest_id}: {type(e_general).__name__} - {e_general}. Will retry if script runs again or loops.")
                    # DO NOT log to processed_contests_log.txt or add to processed_contests set on error
                
                finally:
                    # This block executes whether try was successful or an exception occurred.
                    print(f"      Finished attempt for contest {contest_id}. Navigating back to contest selection state.")
                    driver.get(base_url) 
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "yr")))
                    
                    Select(driver.find_element(By.NAME, "yr")).select_by_value(target_year)
                    Select(driver.find_element(By.NAME, "reg")).select_by_value(region_value)
                    
                    try:
                        event_select_for_reset = Select(WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "ev"))))
                        if event_value in [opt.get_attribute("value") for opt in event_select_for_reset.options]:
                            event_select_for_reset.select_by_value(event_value)
                            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, "cn")))
                            WebDriverWait(driver, 20).until(
                                lambda d: len(Select(d.find_element(By.NAME, "cn")).options) > 1 or
                                          (len(Select(d.find_element(By.NAME, "cn")).options) == 1 and Select(d.find_element(By.NAME, "cn")).first_selected_option.get_attribute("value") != "")
                            )
                            print(f"      Successfully reset to event {event_value} for next contest in this event.")
                        else:
                            print(f"ERROR: Current event_value '{event_value}' no longer in event dropdown after reset. Breaking from contests in this event.")
                            break # Breaks from the inner 'for contest_cn_value in contest_options' loop
                    except Exception as e_reset:
                        print(f"ERROR during state reset for next contest (event: {event_value}): {type(e_reset).__name__}. Breaking from contests in this event.")
                        break # Breaks from the inner 'for contest_cn_value in contest_options' loop

    driver.quit()


# call the funtion and record time

if __name__ == "__main__":
    start_time = time.time()
    # get_uil_results() # Scraping is skipped
    loop_csv()
    print("--- %s seconds ---" % (time.time() - start_time))
