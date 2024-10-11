import pandas as pd
import sqlite3
from scrape_data.fix_pml import fix_pml
from scrape_data.add_new_song_data import adjust_pml
from rapidfuzz import fuzz
from multiprocessing import Pool, cpu_count, Manager
import numpy as np
from tqdm import tqdm


def process_row(row, pml, progress_queue=None):
    if row["code"] != "":
        if progress_queue:
            progress_queue.put(1)  # Update progress for each processed row
        return row

    # Use contains logic combined with rapidfuzz for title matching
    mask = (
        (
            pml["song_search"].apply(lambda x: fuzz.ratio(x, row["title"]) >= 95)
            | pml["song_search_with_specification"].str.contains(row["title"])
        )
        & (
            pml["composer_search"].apply(
                lambda x: (fuzz.partial_ratio(x, row["composer"]) >= 95) & (x != "")
            )
            | pml["arranger_search"].apply(
                lambda x: (fuzz.partial_ratio(x, row["composer"]) >= 95) & (x != "")
            )
        )
        & (pml["event_name"] == row["event"])
    )

    pml_masked = pml[mask].copy()

    if len(pml_masked) == 1:
        row["code"] = pml_masked["code"].values[0]
    elif len(pml_masked) > 1:
        # Use rapidfuzz to find the closest match for the title
        pml_masked["title_match"] = pml_masked["song_search"].apply(
            lambda x: fuzz.ratio(x, row["title"])
        )
        pml_masked = pml_masked.sort_values("title_match", ascending=False)
        row["code"] = pml_masked["code"].values[0]

    if progress_queue:
        progress_queue.put(1)  # Update progress for each processed row

    return row


def process_chunk(chunk, pml, progress_queue):
    """Apply process_row to each row in a chunk of the DataFrame, passing pml and a progress queue."""
    for i, row in chunk.iterrows():
        row = process_row(row, pml, progress_queue)
        chunk.loc[i] = row
    return chunk


def parallel_process(df, pml, num_partitions):
    df_split = np.array_split(df, num_partitions)

    # Manager for sharing data between processes
    manager = Manager()
    progress_queue = manager.Queue()

    total_rows = len(df)
    results = []

    pool = Pool(cpu_count())

    # Start progress bar
    with tqdm(total=total_rows) as pbar:
        # Asynchronous results with progress updates
        results_async = [
            pool.apply_async(process_chunk, args=(chunk, pml, progress_queue))
            for chunk in df_split
        ]

        # Update the progress bar as rows are processed
        processed_rows = 0
        while processed_rows < total_rows:
            processed_rows += progress_queue.get()
            pbar.update(1)

        # Gather results from the pool
        results = [res.get() for res in results_async]

    pool.close()
    pool.join()

    # Concatenate all processed chunks
    return pd.concat(results)


if __name__ == "__main__":
    conn = sqlite3.connect("uil.db")
    grouped_titles = pd.read_sql_query("SELECT * FROM arranger_grouped_entries", conn)
    pml = pd.read_csv("pml_24-25.csv")
    conn.close()

    pml = fix_pml(pml)
    pml = adjust_pml(pml)

    # Reset 'code' field
    grouped_titles["code"] = ""
    grouped_titles.loc[grouped_titles["composer"] == "anon", "composer"] = "anonortrad"
    grouped_titles.loc[grouped_titles["composer"] == "trad", "composer"] = "anonortrad"
    grouped_titles["event"] = grouped_titles["event"].str.replace("/", "-")

    # Sort grouped titles by count
    grouped_titles = grouped_titles.sort_values("count", ascending=False).reset_index(
        drop=True
    )

    # Apply multiprocessing with tqdm progress bar
    num_partitions = cpu_count()
    grouped_titles = parallel_process(grouped_titles, pml, num_partitions)

    # Write results back to the database
    conn = sqlite3.connect("uil.db")
    grouped_titles.to_sql(
        "arranger_grouped_entries", conn, if_exists="replace", index=False
    )
    pml.to_sql("pml", conn, if_exists="replace", index=False)
    conn.close()

    # Output missing codes information
    missing_codes = grouped_titles[grouped_titles["code"] == ""]
    print(f"Missing codes: {len(missing_codes)}")
    print(f"Total entries: {len(grouped_titles)}")
    print(f"Percentage missing: {len(missing_codes) / len(grouped_titles)}")
