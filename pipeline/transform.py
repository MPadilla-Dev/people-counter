# pipeline/transform.py
"""
Transform layer: produces hourly aggregations, net flow, and occupancy.
Assumes raw_events table already exists in the DuckDB connection.
"""

import duckdb


def create_hourly_aggregations(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Aggregates raw events into hourly buckets per device.

    DATE_TRUNC('hour', timestamp) rounds any timestamp down to its hour.
    e.g. 08:10, 08:15, 08:30 all become 08:00 and are summed together.

    We also carry forward a quality flag: if ANY row in that hour had a
    null filled in, the whole hour is flagged as containing imputed data.
    
    CAST(...AS INTEGER) ensures people counts are whole numbers,
    not floats. A null during aggregation can promote types to float64
    without this explicit cast.
    """
    conn.execute("""
        CREATE OR REPLACE TABLE hourly_aggregations AS
        SELECT
            device_id,
            DATE_TRUNC('hour', timestamp)                      AS hour,
            CAST(SUM(people_in)                   AS INTEGER)  AS total_in,
            CAST(SUM(people_out)                  AS INTEGER)  AS total_out,
            CAST(SUM(people_in) - SUM(people_out) AS INTEGER)  AS net_flow,
            MAX(in_was_null)                                   AS has_imputed_in,
            MAX(out_was_null)                                  AS has_imputed_out
        FROM raw_events
        GROUP BY device_id, DATE_TRUNC('hour', timestamp)
        ORDER BY device_id, hour
    """)

    total = conn.execute("SELECT COUNT(*) FROM hourly_aggregations").fetchone()[0]
    imputed = conn.execute("""
        SELECT COUNT(*) FROM hourly_aggregations
        WHERE has_imputed_in = 1 OR has_imputed_out = 1
    """).fetchone()[0]

    print(f"  Hourly aggregations: {total} rows ({imputed} hours contain imputed data)")


def create_occupancy(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Computes running occupancy per device using a window function.

    SUM(net_flow) OVER (...) means:
        'for each row, sum all net_flow values from the first row up to
         and including this one, separately for each device, in time order'

    occupancy_is_invalid flags rows where cumulative occupancy went
    negative — preserved as-is rather than clamped, so the dashboard
    can surface a warning without losing the raw signal.
    """
    conn.execute("""
        CREATE OR REPLACE TABLE occupancy AS
        SELECT
            device_id,
            hour,
            total_in,
            total_out,
            net_flow,
            CAST(
                SUM(net_flow) OVER (
                    PARTITION BY device_id
                    ORDER BY hour
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS INTEGER
            )                        AS occupancy,
            CAST(
                SUM(net_flow) OVER (
                    PARTITION BY device_id
                    ORDER BY hour
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) < 0 AS INTEGER
            )                        AS occupancy_is_invalid,
            has_imputed_in,
            has_imputed_out
        FROM hourly_aggregations
        ORDER BY device_id, hour
    """)

    total = conn.execute("SELECT COUNT(*) FROM occupancy").fetchone()[0]
    negative = conn.execute(
        "SELECT COUNT(*) FROM occupancy WHERE occupancy < 0"
    ).fetchone()[0]

    print(f"  Occupancy: {total} rows computed")

    if negative > 0:
        print(f"  ⚠ WARNING: {negative} rows have negative occupancy — check source data")
    else:
        print(f"  ✓ No negative occupancy values detected")


def create_daily_aggregations(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Rolls hourly occupancy data up to daily summaries.
    Useful for trend views in the dashboard (zoomed-out view).

    peak_occupancy and min_occupancy come from the pre-computed
    occupancy column — not recalculated here.
    """
    conn.execute("""
        CREATE OR REPLACE TABLE daily_aggregations AS
        SELECT
            device_id,
            DATE_TRUNC('day', hour)                AS date,
            CAST(SUM(total_in)       AS INTEGER)   AS total_in,
            CAST(SUM(total_out)      AS INTEGER)   AS total_out,
            CAST(SUM(net_flow)       AS INTEGER)   AS net_flow,
            CAST(MAX(occupancy)      AS INTEGER)   AS peak_occupancy,
            CAST(MIN(occupancy)      AS INTEGER)   AS min_occupancy
        FROM occupancy
        GROUP BY device_id, DATE_TRUNC('day', hour)
        ORDER BY device_id, date
    """)

    total = conn.execute("SELECT COUNT(*) FROM daily_aggregations").fetchone()[0]
    print(f"  Daily aggregations: {total} rows")


def preview_transforms(conn: duckdb.DuckDBPyConnection) -> None:
    """Prints all three output tables for manual inspection."""

    print("\n--- hourly_aggregations ---")
    print(conn.execute("SELECT * FROM hourly_aggregations").df().to_string())

    print("\n--- occupancy ---")
    print(conn.execute("SELECT * FROM occupancy").df().to_string())

    print("\n--- daily_aggregations ---")
    print(conn.execute("SELECT * FROM daily_aggregations").df().to_string())


if __name__ == "__main__":
    import os
    from pipeline.ingest import get_db_connection, load_raw_events

    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
    conn = get_db_connection()
    load_raw_events(DATA_DIR, conn)

    print("\nRunning transformations...")
    create_hourly_aggregations(conn)
    create_occupancy(conn)
    create_daily_aggregations(conn)
    preview_transforms(conn)