# tests/test_output.py
"""
Tests for the output layer.
Verifies that Parquet files are written correctly and
that the schema matches what Django expects.
"""

import os
import tempfile
import pytest
import duckdb
from pipeline.transform import (
    create_hourly_aggregations,
    create_occupancy,
    create_daily_aggregations
)
from pipeline.output import write_all_outputs


def test_parquet_files_are_created(raw_events_table):
    """All three output files should exist after write_all_outputs."""
    conn = raw_events_table
    create_hourly_aggregations(conn)
    create_occupancy(conn)
    create_daily_aggregations(conn)

    with tempfile.TemporaryDirectory() as tmpdir:
        paths = write_all_outputs(conn, tmpdir)

        for table_name, path in paths.items():
            assert os.path.exists(path), f"Missing output file: {path}"


def test_parquet_schema_matches_contract(raw_events_table):
    """
    The column names in the Parquet files must exactly match
    what Django models expect. This test acts as a guard —
    if someone renames a column in the pipeline, this test
    breaks immediately and alerts them that Django will break too.
    """
    conn = raw_events_table
    create_hourly_aggregations(conn)
    create_occupancy(conn)
    create_daily_aggregations(conn)

    expected_schemas = {
        "hourly_aggregations": [
            "device_id", "hour", "total_in", "total_out",
            "net_flow", "has_imputed_in", "has_imputed_out"
        ],
        "occupancy": [
            "device_id", "hour", "total_in", "total_out",
            "net_flow", "occupancy", "occupancy_is_invalid",
            "has_imputed_in", "has_imputed_out"
        ],
        "daily_aggregations": [
            "device_id", "date", "total_in", "total_out",
            "net_flow", "peak_occupancy", "min_occupancy"
        ],
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        paths = write_all_outputs(conn, tmpdir)

        for table_name, path in paths.items():
            safe_path = path.replace("\\", "/")
            df = conn.execute(
                f"SELECT * FROM read_parquet('{safe_path}') LIMIT 1"
            ).df()

            actual_columns   = list(df.columns)
            expected_columns = expected_schemas[table_name]

            assert actual_columns == expected_columns, (
                f"{table_name} schema mismatch.\n"
                f"  Expected: {expected_columns}\n"
                f"  Got:      {actual_columns}"
            )


def test_parquet_row_counts_match_duckdb(raw_events_table):
    """
    Row counts in the Parquet files should exactly match
    what's in the DuckDB tables — nothing lost in writing.
    """
    conn = raw_events_table
    create_hourly_aggregations(conn)
    create_occupancy(conn)
    create_daily_aggregations(conn)

    table_map = {
        "hourly_aggregations": "hourly_aggregations",
        "occupancy":           "occupancy",
        "daily_aggregations":  "daily_aggregations",
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        paths = write_all_outputs(conn, tmpdir)

        for table_name, path in paths.items():
            safe_path = path.replace("\\", "/")

            expected = conn.execute(
                f"SELECT COUNT(*) FROM {table_map[table_name]}"
            ).fetchone()[0]

            actual = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{safe_path}')"
            ).fetchone()[0]

            assert actual == expected, (
                f"{table_name} row count mismatch: "
                f"expected {expected}, got {actual}"
            )