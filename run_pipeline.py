# run_pipeline.py
"""
Main pipeline entry point.
Run with: python run_pipeline.py
"""

import os
from pipeline.ingest    import get_db_connection, load_raw_events
from pipeline.transform import (
    create_hourly_aggregations,
    create_occupancy,
    create_daily_aggregations
)
from pipeline.output    import write_all_outputs, verify_outputs

DATA_DIR   = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


def run():
    print("=" * 40)
    print("  People Counting Pipeline")
    print("=" * 40)

    # Phase 1 — Ingestion
    print("\n[Phase 1] Ingesting raw CSV files...")
    conn = get_db_connection()
    load_raw_events(DATA_DIR, conn)

    # Phase 2 — Transforms
    print("\n[Phase 2] Running transformations...")
    create_hourly_aggregations(conn)
    create_occupancy(conn)
    create_daily_aggregations(conn)

    # Phase 3 — Output
    print("\n[Phase 3] Writing Parquet files...")
    output_paths = write_all_outputs(conn, OUTPUT_DIR)
    verify_outputs(output_paths, conn)

    print("\n" + "=" * 40)
    print("  Pipeline complete.")
    print("=" * 40)


if __name__ == "__main__":
    run()