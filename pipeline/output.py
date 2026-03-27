"""
Output layer: writes DuckDB tables to partitioned Parquet files.
These files are the contract between the pipeline and Django.
"""

import os
import duckdb


def ensure_output_dirs(output_dir: str) -> dict:
    """
    Creates the output folder structure if it doesn't exist.
    Returns a dict of paths for each table.
    """
    paths = {
        "hourly":  os.path.join(output_dir, "hourly"),
        "occupancy": os.path.join(output_dir, "occupancy"),
        "daily":   os.path.join(output_dir, "daily"),
    }

    for path in paths.values():
        os.makedirs(path, exist_ok=True)

    return paths


def write_parquet(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: str
) -> None:
    """
    Writes a DuckDB table to a Parquet file at output_path.

    COPY ... TO ... is DuckDB's way of exporting a table to disk.
    FORMAT PARQUET tells it to use the Parquet format.
    We don't partition at the file level here — we write one file
    per table and let Django's database handle indexed lookups.
    This keeps the output simple while still gaining Parquet's
    type preservation and compression benefits.
    """

    # Use forward slashes — DuckDB on Windows needs this
    safe_path = output_path.replace("\\", "/")

    conn.execute(f"""
        COPY (SELECT * FROM {table_name})
        TO '{safe_path}'
        (FORMAT PARQUET)
    """)

    # Verify the file was actually written and report its size
    size_bytes = os.path.getsize(output_path)
    size_kb = size_bytes / 1024
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    print(f"  ✓ {table_name}: {row_count} rows → {output_path} ({size_kb:.1f} KB)")


def write_all_outputs(
    conn: duckdb.DuckDBPyConnection,
    output_dir: str
) -> dict:
    """
    Writes all three pipeline outputs to Parquet.
    Returns the paths dict so callers know where files landed.
    """

    paths = ensure_output_dirs(output_dir)

    print("Writing Parquet outputs...")

    outputs = {
        "hourly_aggregations": os.path.join(paths["hourly"],    "hourly_aggregations.parquet"),
        "occupancy":           os.path.join(paths["occupancy"], "occupancy.parquet"),
        "daily_aggregations":  os.path.join(paths["daily"],     "daily_aggregations.parquet"),
    }

    for table_name, file_path in outputs.items():
        write_parquet(conn, table_name, file_path)

    print(f"\n  All outputs written to: {output_dir}")
    return outputs


def verify_outputs(
    output_paths: dict,
    conn: duckdb.DuckDBPyConnection
) -> None:
    """
    Reads the Parquet files back from disk and previews them.

    This is an important step — it proves the files are readable
    and that the schema survived the write correctly.
    Reading back from disk rather than from the DuckDB connection
    catches any serialisation issues that would break Django later.
    """

    print("\n--- Verifying Parquet output (reading from disk) ---")

    for table_name, file_path in output_paths.items():
        safe_path = file_path.replace("\\", "/")

        # Read directly from the Parquet file — not from DuckDB's memory
        df = conn.execute(f"""
            SELECT * FROM read_parquet('{safe_path}')
            LIMIT 5
        """).df()

        print(f"\n{table_name}:")
        print(f"  columns: {list(df.columns)}")
        print(f"  dtypes:  {dict(df.dtypes)}")
        print(df.to_string())


if __name__ == "__main__":
    import os
    from pipeline.ingest import get_db_connection, load_raw_events
    from pipeline.transform import (
        create_hourly_aggregations,
        create_occupancy,
        create_daily_aggregations
    )

    DATA_DIR   = os.path.join(os.path.dirname(__file__), "..", "data")
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")

    # Run the full pipeline
    conn = get_db_connection()

    print("=== Phase 1: Ingestion ===")
    load_raw_events(DATA_DIR, conn)

    print("=== Phase 2: Transforms ===")
    create_hourly_aggregations(conn)
    create_occupancy(conn)
    create_daily_aggregations(conn)

    print("\n=== Phase 3: Output ===")
    output_paths = write_all_outputs(conn, OUTPUT_DIR)
    verify_outputs(output_paths, conn)