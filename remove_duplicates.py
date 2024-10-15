import sqlite3


def remove_duplicates():
    conn = sqlite3.connect("uil_2.db")
    cursor = conn.cursor()

    # SQL to remove duplicates
    sql = """
    WITH RankedResults AS (
        SELECT
            rowid,
            ROW_NUMBER() OVER (PARTITION BY entry_number ORDER BY rowid) AS rn
        FROM
            results
    )
    DELETE FROM results
    WHERE rowid IN (
        SELECT rowid
        FROM RankedResults
        WHERE rn > 1
    );
    """

    cursor.execute(sql)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    remove_duplicates()
