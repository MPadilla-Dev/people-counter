# tests/conftest.py
"""
Shared test fixtures available to all test files.
pytest automatically loads this file before running any tests.
"""

import os
import csv
import pytest
import duckdb


@pytest.fixture
def make_csv():
    """
    Returns the make_csv helper function as a fixture
    so all test files can use it without importing.
    """
    def _make_csv(folder: str, filename: str, rows: list) -> str:
        os.makedirs(folder, exist_ok=True)
        filepath = os.path.join(folder, filename)
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "in", "out"])
            writer.writeheader()
            writer.writerows(rows)
        return filepath
    return _make_csv


@pytest.fixture
def conn():
    """
    Creates a fresh in-memory DuckDB connection for each test.
    """
    connection = duckdb.connect(database=":memory:")
    yield connection
    connection.close()


@pytest.fixture
def raw_events_table(conn):
    """
    Creates a raw_events table with known, controlled data.
    """
    conn.execute("""
        CREATE TABLE raw_events (
            device_id    VARCHAR,
            timestamp    TIMESTAMP,
            people_in    INTEGER,
            people_out   INTEGER,
            in_was_null  INTEGER,
            out_was_null INTEGER,
            in_was_negative  INTEGER,
            out_was_negative INTEGER
        )
    """)

    conn.execute("""
        INSERT INTO raw_events VALUES
        ('device_A', '2024-01-01 08:00:00', 5, 2, 0, 0, 0, 0),
        ('device_A', '2024-01-01 08:30:00', 3, 0, 0, 1, 0, 0),
        ('device_A', '2024-01-01 09:00:00', 1, 8, 0, 0, 0, 0),
        ('device_B', '2024-01-01 08:00:00', 4, 1, 0, 0, 0, 0),
        ('device_B', '2024-01-01 09:00:00', 2, 3, 0, 0, 0, 0)
    """)

    return conn