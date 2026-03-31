# pipeline/ingest.py
"""
Ingest layer: discovers and reads raw CSV files into DuckDB.
Handles missing values, out-of-order timestamps, and bad data.
"""

import os
import duckdb


def get_db_connection() -> duckdb.DuckDBPyConnection:
    """
    Creates an in-memory DuckDB database connection.
    In-memory means no file is written to disk at this stage —
    we only persist the final aggregated output as Parquet.
    """
    return duckdb.connect(database=":memory:")


def load_raw_events(data_dir: str, conn: duckdb.DuckDBPyConnection) -> None:
    """
    Discovers all CSV files under data_dir, reads them into DuckDB,
    adds a device_id column, handles nulls, and sorts by timestamp.

    The result is stored as a table called 'raw_events' in the connection.
    """

    # Step 1: Find all CSV files and pair them with their device name
    device_files = []

    for device_name in os.listdir(data_dir):
        device_path = os.path.join(data_dir, device_name)

        # Skip anything that isn't a folder (e.g. .DS_Store files)
        if not os.path.isdir(device_path):
            print(f"  Skipping non-directory: {device_name}")
            continue

        for filename in os.listdir(device_path):
            if filename.endswith(".csv"):
                full_path = os.path.join(device_path, filename)
                device_files.append((device_name, full_path))

    if not device_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    print(f"  Found {len(device_files)} CSV files:")
    for device_name, path in device_files:
        print(f"    [{device_name}] {path}")

    # Step 2: Read each CSV into DuckDB and tag it with device_id
    # We build one table per file, then UNION ALL them together
    union_parts = []

    for device_name, path in device_files:
        # DuckDB can read CSVs directly
        # We cast types explicitly so we control what happens with bad data
        query_part = f"""
            SELECT
                '{device_name}'                              AS device_id,
                TRY_CAST(timestamp AS TIMESTAMP)             AS timestamp,
                COALESCE(TRY_CAST("in"  AS INTEGER), 0)     AS people_in,
                COALESCE(TRY_CAST("out" AS INTEGER), 0)      AS people_out,
                CASE WHEN TRY_CAST("in"  AS INTEGER) IS NULL 
                    THEN 1 ELSE 0 END                       AS in_was_null,
                CASE WHEN TRY_CAST("out" AS INTEGER) IS NULL 
                    THEN 1 ELSE 0 END                       AS out_was_null,
                CASE WHEN TRY_CAST("in"  AS INTEGER) < 0    
                    THEN 1 ELSE 0 END                       AS in_was_negative,
                CASE WHEN TRY_CAST("out" AS INTEGER) < 0    
                    THEN 1 ELSE 0 END                       AS out_was_negative
            FROM read_csv_auto('{path}', header=true)
        """
        union_parts.append(query_part)

    full_query = "\nUNION ALL\n".join(union_parts)

    # Step 3: Create the raw_events table, filtering out rows where
    # the timestamp failed to parse (TRY_CAST returns NULL on failure)
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw_events AS
        SELECT *
        FROM (
            {full_query}
        ) combined
        WHERE timestamp IS NOT NULL
        AND people_in  >= 0
        AND people_out >= 0
        ORDER BY device_id, timestamp
    """)

    # Step 4: Report what we loaded and flag any data quality issues
    total = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
    nulls_filled = conn.execute("""
        SELECT 
            SUM(in_was_null)      AS in_nulls_filled,
            SUM(out_was_null)     AS out_nulls_filled,
            SUM(in_was_negative)  AS in_negatives_dropped,
            SUM(out_was_negative) AS out_negatives_dropped
        FROM raw_events
    """).fetchone()

    print(f"  Loaded {total} valid rows into raw_events")
    print(f"  Nulls filled with 0   → people_in: {nulls_filled[0]}, people_out: {nulls_filled[1]}")
    print(f"  Negatives dropped     → people_in: {nulls_filled[2]}, people_out: {nulls_filled[3]}")
    print(f"  These rows are flagged in in_was_null / out_was_null columns\n")


def preview_raw_events(conn: duckdb.DuckDBPyConnection) -> None:
    """Prints the first 10 rows of raw_events for manual inspection."""
    print("--- raw_events preview ---")
    print(conn.execute("SELECT * FROM raw_events LIMIT 10").df().to_string())
    print()


if __name__ == "__main__":
    # This block runs when you execute: python -m pipeline.ingest
    # Useful for testing this file in isolation
    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
    conn = get_db_connection()
    load_raw_events(DATA_DIR, conn)
    preview_raw_events(conn)