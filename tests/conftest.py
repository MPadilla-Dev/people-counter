# tests/conftest.py
"""
Shared test fixtures available to all test files.
pytest automatically loads this file before running any tests.
A fixture is a reusable piece of test setup — think of it as
a helper that creates known data for tests to work with.
"""

import pytest
import duckdb


@pytest.fixture
def conn():
    """
    Creates a fresh in-memory DuckDB connection for each test.
    The 'yield' means: set up before the test, tear down after.
    Each test gets a completely clean database — no shared state.
    """
    connection = duckdb.connect(database=":memory:")
    yield connection
    connection.close()


@pytest.fixture
def raw_events_table(conn):
    """
    Creates a raw_events table with known, controlled data.
    We design this data to test specific scenarios:

    Row 1-2: two rows in the same hour → should be summed together
    Row 3:   a null out value          → should become 0, flagged
    Row 4:   out > in                  → tests negative net_flow
    Row 5:   different device          → should be partitioned separately
    """
    conn.execute("""
        CREATE TABLE raw_events (
            device_id    VARCHAR,
            timestamp    TIMESTAMP,
            people_in    INTEGER,
            people_out   INTEGER,
            in_was_null  INTEGER,
            out_was_null INTEGER
        )
    """)

    conn.execute("""
        INSERT INTO raw_events VALUES
        -- device_A: two rows in 08:00 hour, one row in 09:00 hour
        ('device_A', '2024-01-01 08:00:00', 5, 2, 0, 0),
        ('device_A', '2024-01-01 08:30:00', 3, 0, 0, 1),
        ('device_A', '2024-01-01 09:00:00', 1, 8, 0, 0),
        -- device_B: independent device, same timestamps
        ('device_B', '2024-01-01 08:00:00', 4, 1, 0, 0),
        ('device_B', '2024-01-01 09:00:00', 2, 3, 0, 0)
    """)

    return conn