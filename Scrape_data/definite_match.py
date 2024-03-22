def definite_match(col, row, n, pml=pml):

    try:
        title_col = col.index(f"Title {n}")
        title = re.sub(r"[^a-zA-Z]", "", row[title_col]).lower()

        # if  title contains "from" get rid of from and anything after
        if "from" in title:
            title = title.split("from")[0]

        composer_col = col.index(f"Composer {n}")
        composer = row[composer_col].strip()
        # check if composer has space
        if " " in composer:
            composer_last = composer.split(" ")[-1]
            composer_last = re.sub(r"[^a-zA-Z]", "", composer_last).lower()

        else:
            composer_last = composer.lower()
        composer = re.sub(r"[^a-zA-Z]", "", row[composer_col]).lower()
        composer = composer.replace("anonortrad", "")

    except Exception as e:
        return None

    # check if title just has a number already. it will be any 5 digit number in the title
    if re.search(r"\d{5}", title):
        # extract the number
        match = re.search(r"\d{5}", title)
        code = match.group(0)

        return code

    event_name = row[col.index("Event")].lower().replace(" ", "")
    # only keep text after -
    event_name = event_name.split("-")[1]
    # get rid of anything not a letter
    event_name = re.sub(r"[^a-zA-Z]", "", event_name)

    # if contains band ==band
    if "band" in event_name:
        event_name = "band"

    # Connect to your SQLite database
    conn = sqlite3.connect("uil.db")
    c = conn.cursor()

    # Define your query
    query = """
    SELECT Code FROM pml
    WHERE (song_search = ? OR song_search_with_specification = ? OR song_simple = ?)
    AND (composer_search = ? OR composer_arranger = ? OR composer_search = ?)
    AND "Event Name" = ?
    """

    # Execute the query with parameters
    c.execute(
        query, (title, title, title, composer, composer, composer_last, event_name)
    )

    # Fetch the result
    result = c.fetchone()

    # Check if result is found
    if result:
        code = result[0]
        return code
    else:
        return None


if __name__ == "__main__":
    import sqlite3

    # Connect to your SQLite database
    conn = sqlite3.connect("uil.db")
    c = conn.cursor()
    # Define your query
    query = """
    SELECT * FROM pml
    """
    # Execute the query
    c.execute(query)
    # Create a list to hold all of your data
    data = c.fetchall()
    # Print the data
    for row in data:
        print(row)
    # Close the connection
    conn.close()
