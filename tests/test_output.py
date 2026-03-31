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

def test_full_pipeline_end_to_end(make_csv):
    """
    End-to-end test: runs the complete pipeline from CSV files
    to Parquet output and verifies the final values are correct.

    This is the only test that exercises all three layers together:
        ingest → transform → output

    All other tests verify layers in isolation. This test verifies
    the layers produce correct results when chained together.

    Input is designed so every expected value can be calculated by hand:
        device_A 08:00: in=5, out=2 → net_flow=3
        device_A 08:30: in=3, out=0 → net_flow=3  (out_was_null filled)
        device_A 09:00: in=1, out=8 → net_flow=-7

    Hourly aggregations:
        08:00 bucket: total_in=8, total_out=2, net_flow=6
        09:00 bucket: total_in=1, total_out=8, net_flow=-7

    Occupancy:
        08:00: occupancy=6
        09:00: occupancy=6+(-7)=-1 → occupancy_is_invalid=1

    Daily:
        total_in=9, total_out=10, peak_occupancy=6, min_occupancy=-1
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # --- ARRANGE ---
        # Create real CSV files on disk — same as production input
        data_dir   = os.path.join(tmpdir, "data")
        output_dir = os.path.join(tmpdir, "output")

        device_dir = os.path.join(data_dir, "device_A")
        make_csv(device_dir, "2024-01-01.csv", [
            {"timestamp": "2024-01-01T08:00:00", "in": 5, "out": 2},
            {"timestamp": "2024-01-01T08:30:00", "in": 3, "out": ""},
            {"timestamp": "2024-01-01T09:00:00", "in": 1, "out": 8},
        ])

        # --- ACT ---
        # Run the full pipeline exactly as run_pipeline.py does
        from pipeline.ingest    import get_db_connection, load_raw_events
        from pipeline.transform import (
            create_hourly_aggregations,
            create_occupancy,
            create_daily_aggregations
        )
        from pipeline.output import write_all_outputs

        conn = get_db_connection()
        load_raw_events(data_dir, conn)
        create_hourly_aggregations(conn)
        create_occupancy(conn)
        create_daily_aggregations(conn)
        paths = write_all_outputs(conn, output_dir)

        # --- ASSERT ---
        # Read each Parquet file from disk and verify values
        # These are calculated by hand in the docstring above

        def read(path, query):
            safe = path.replace("\\", "/")
            return conn.execute(f"""
                {query}
                FROM read_parquet('{safe}')
                ORDER BY hour
            """).fetchall()

        # Verify hourly aggregations
        hourly_path = paths["hourly_aggregations"]
        hourly = read(hourly_path, """
            SELECT hour, total_in, total_out, net_flow, has_imputed_out
        """)

        assert len(hourly) == 2, \
            f"Expected 2 hourly rows, got {len(hourly)}"

        # 08:00 bucket: 5+3=8 in, 2+0=2 out, net=6, imputed=1
        assert hourly[0][1] == 8,  f"08:00 total_in:  expected 8,  got {hourly[0][1]}"
        assert hourly[0][2] == 2,  f"08:00 total_out: expected 2,  got {hourly[0][2]}"
        assert hourly[0][3] == 6,  f"08:00 net_flow:  expected 6,  got {hourly[0][3]}"
        assert hourly[0][4] == 1,  f"08:00 imputed:   expected 1,  got {hourly[0][4]}"

        # 09:00 bucket: 1 in, 8 out, net=-7, not imputed
        assert hourly[1][1] == 1,  f"09:00 total_in:  expected 1,  got {hourly[1][1]}"
        assert hourly[1][2] == 8,  f"09:00 total_out: expected 8,  got {hourly[1][2]}"
        assert hourly[1][3] == -7, f"09:00 net_flow:  expected -7, got {hourly[1][3]}"
        assert hourly[1][4] == 0,  f"09:00 imputed:   expected 0,  got {hourly[1][4]}"

        # Verify occupancy
        occupancy_path = paths["occupancy"]
        safe_occupancy = occupancy_path.replace("\\", "/")
        occ = conn.execute(f"""
            SELECT hour, occupancy, occupancy_is_invalid
            FROM read_parquet('{safe_occupancy}')
            ORDER BY hour
        """).fetchall()

        # 08:00: occupancy=6, valid
        assert occ[0][1] == 6,  f"08:00 occupancy: expected 6,  got {occ[0][1]}"
        assert occ[0][2] == 0,  f"08:00 invalid:   expected 0,  got {occ[0][2]}"

        # 09:00: occupancy=-1, invalid
        assert occ[1][1] == -1, f"09:00 occupancy: expected -1, got {occ[1][1]}"
        assert occ[1][2] == 1,  f"09:00 invalid:   expected 1,  got {occ[1][2]}"

        # Verify daily aggregations
        daily_path = paths["daily_aggregations"]
        safe_daily = daily_path.replace("\\", "/")
        daily = conn.execute(f"""
            SELECT total_in, total_out, net_flow,
                   peak_occupancy, min_occupancy
            FROM read_parquet('{safe_daily}')
        """).fetchall()

        assert len(daily) == 1,     f"Expected 1 daily row, got {len(daily)}"
        assert daily[0][0] == 9,    f"total_in:       expected 9,  got {daily[0][0]}"
        assert daily[0][1] == 10,   f"total_out:      expected 10, got {daily[0][1]}"
        assert daily[0][2] == -1,   f"net_flow:       expected -1, got {daily[0][2]}"
        assert daily[0][3] == 6,    f"peak_occupancy: expected 6,  got {daily[0][3]}"
        assert daily[0][4] == -1,   f"min_occupancy:  expected -1, got {daily[0][4]}"