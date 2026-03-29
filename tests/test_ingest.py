# tests/test_ingest.py
"""
Tests for the ingestion layer.
We test that bad data is handled correctly — nulls, bad timestamps,
non-directory files, and that device_id is extracted from folder name.
"""

import os
import csv
import tempfile
import pytest
import duckdb
from pipeline.ingest import load_raw_events, get_db_connection


def make_csv(folder: str, filename: str, rows: list[dict]) -> str:
    """
    Helper: writes a CSV file to a temp folder.
    Returns the full path of the created file.
    """
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "in", "out"])
        writer.writeheader()
        writer.writerows(rows)
    return filepath


def test_loads_valid_rows():
    """Basic case: valid CSV loads all rows correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        device_dir = os.path.join(tmpdir, "device_A")
        make_csv(device_dir, "2024-01-01.csv", [
            {"timestamp": "2024-01-01T08:00:00", "in": 5, "out": 2},
            {"timestamp": "2024-01-01T09:00:00", "in": 3, "out": 1},
        ])

        conn = get_db_connection()
        load_raw_events(tmpdir, conn)

        count = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
        assert count == 2, f"Expected 2 rows, got {count}"


def test_device_id_extracted_from_folder():
    """device_id should come from the folder name, not the file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        device_dir = os.path.join(tmpdir, "device_TEST")
        make_csv(device_dir, "2024-01-01.csv", [
            {"timestamp": "2024-01-01T08:00:00", "in": 1, "out": 0},
        ])

        conn = get_db_connection()
        load_raw_events(tmpdir, conn)

        device_id = conn.execute(
            "SELECT device_id FROM raw_events LIMIT 1"
        ).fetchone()[0]

        assert device_id == "device_TEST", f"Expected 'device_TEST', got '{device_id}'"


def test_null_out_is_filled_and_flagged():
    """
    A missing 'out' value should become 0 AND be flagged.
    This tests both the fill and the audit trail.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        device_dir = os.path.join(tmpdir, "device_A")
        make_csv(device_dir, "2024-01-01.csv", [
            {"timestamp": "2024-01-01T08:00:00", "in": 5, "out": ""},
        ])

        conn = get_db_connection()
        load_raw_events(tmpdir, conn)

        row = conn.execute("""
            SELECT people_out, out_was_null 
            FROM raw_events 
            LIMIT 1
        """).fetchone()

        assert row[0] == 0,  f"Expected people_out=0, got {row[0]}"
        assert row[1] == 1,  f"Expected out_was_null=1, got {row[1]}"


def test_invalid_timestamp_row_is_dropped():
    """
    Rows with unparseable timestamps should be silently dropped,
    not crash the pipeline.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        device_dir = os.path.join(tmpdir, "device_A")
        make_csv(device_dir, "2024-01-01.csv", [
            {"timestamp": "NOT_A_DATE",          "in": 5, "out": 2},
            {"timestamp": "2024-01-01T08:00:00", "in": 3, "out": 1},
        ])

        conn = get_db_connection()
        load_raw_events(tmpdir, conn)

        count = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
        assert count == 1, f"Expected 1 valid row, got {count}"


def test_timestamps_are_sorted():
    """
    Rows should come out sorted by timestamp regardless of
    the order they appear in the CSV file.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        device_dir = os.path.join(tmpdir, "device_A")
        make_csv(device_dir, "2024-01-01.csv", [
            {"timestamp": "2024-01-01T10:00:00", "in": 1, "out": 0},
            {"timestamp": "2024-01-01T08:00:00", "in": 2, "out": 0},
            {"timestamp": "2024-01-01T09:00:00", "in": 3, "out": 0},
        ])

        conn = get_db_connection()
        load_raw_events(tmpdir, conn)

        timestamps = conn.execute("""
            SELECT timestamp FROM raw_events
        """).fetchall()

        extracted = [row[0] for row in timestamps]
        assert extracted == sorted(extracted), "Timestamps are not sorted"


def test_empty_data_dir_raises_error():
    """An empty data directory should raise FileNotFoundError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        conn = get_db_connection()
        with pytest.raises(FileNotFoundError):
            load_raw_events(tmpdir, conn)

def test_negative_values_are_handled():
    """
    Negative counts are physically impossible.
    They should be treated as invalid and either dropped or flagged.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        device_dir = os.path.join(tmpdir, "device_A")
        make_csv(device_dir, "2024-01-01.csv", [
            {"timestamp": "2024-01-01T08:00:00", "in": -3, "out": 1},
            {"timestamp": "2024-01-01T09:00:00", "in": 5,  "out": 1},
        ])

        conn = get_db_connection()
        load_raw_events(tmpdir, conn)

        # The negative row should be dropped
        count = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
        assert count == 1, f"Expected 1 valid row, got {count}"

def test_raw_events_has_quality_flag_columns():
    """
    raw_events must contain all quality flag columns so downstream
    transforms and reports can use them.
    This acts as a schema contract test for the ingestion layer.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        device_dir = os.path.join(tmpdir, "device_A")
        make_csv(device_dir, "2024-01-01.csv", [
            {"timestamp": "2024-01-01T08:00:00", "in": 5, "out": 2},
        ])

        conn = get_db_connection()
        load_raw_events(tmpdir, conn)

        columns = [
            row[0] for row in
            conn.execute("DESCRIBE raw_events").fetchall()
        ]

        expected = [
            "device_id", "timestamp", "people_in", "people_out",
            "in_was_null", "out_was_null",
            "in_was_negative", "out_was_negative"
        ]

        for col in expected:
            assert col in columns, (
                f"Expected column '{col}' in raw_events, not found.\n"
                f"Actual columns: {columns}"
            )