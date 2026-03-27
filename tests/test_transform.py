# tests/test_transform.py
"""
Tests for the transform layer.
These use the raw_events_table fixture from conftest.py.
Each test verifies one specific calculation with known inputs
so we can check the output by hand.
"""

import pytest
from pipeline.transform import (
    create_hourly_aggregations,
    create_occupancy,
    create_daily_aggregations
)


class TestHourlyAggregations:

    def test_rows_in_same_hour_are_summed(self, raw_events_table):
        """
        device_A has two rows in the 08:00 hour:
            row1: in=5, out=2
            row2: in=3, out=0
        Expected: total_in=8, total_out=2
        """
        conn = raw_events_table
        create_hourly_aggregations(conn)

        row = conn.execute("""
            SELECT total_in, total_out
            FROM hourly_aggregations
            WHERE device_id = 'device_A'
            AND hour = '2024-01-01 08:00:00'
        """).fetchone()

        assert row is not None,  "Expected a row for device_A 08:00"
        assert row[0] == 8,    f"Expected total_in=8, got {row[0]}"
        assert isinstance(row[0], int), f"Expected int, got {type(row[0])}"
        assert row[1] == 2,    f"Expected total_out=2, got {row[1]}"
        assert isinstance(row[1], int), f"Expected int, got {type(row[1])}"

    def test_net_flow_is_correct(self, raw_events_table):
        """
        device_A 08:00: total_in=8, total_out=2 → net_flow=6
        device_A 09:00: total_in=1, total_out=8 → net_flow=-7
        """
        conn = raw_events_table
        create_hourly_aggregations(conn)

        rows = conn.execute("""
            SELECT hour, net_flow
            FROM hourly_aggregations
            WHERE device_id = 'device_A'
            ORDER BY hour
        """).fetchall()

        assert rows[0][1] == 6,  f"Expected net_flow=6 at 08:00, got {rows[0][1]}"
        assert isinstance(rows[0][1], int), f"Expected int, got {type(rows[0][1])}"
        assert rows[1][1] == -7, f"Expected net_flow=-7 at 09:00, got {rows[1][1]}"
        assert isinstance(rows[1][1], int), f"Expected int, got {type(rows[1][1])}"
        

    def test_devices_are_aggregated_separately(self, raw_events_table):
        """
        device_A and device_B share the same timestamps but should
        never be mixed together in aggregation.
        """
        conn = raw_events_table
        create_hourly_aggregations(conn)

        devices = conn.execute("""
            SELECT DISTINCT device_id FROM hourly_aggregations
            ORDER BY device_id
        """).fetchall()

        device_list = [row[0] for row in devices]
        assert "device_A" in device_list
        assert "device_B" in device_list
        assert len(device_list) == 2

    def test_imputed_flag_propagates(self, raw_events_table):
        """
        device_A 08:00 has one row with out_was_null=1.
        The hourly bucket should have has_imputed_out=1.
        """
        conn = raw_events_table
        create_hourly_aggregations(conn)

        flag = conn.execute("""
            SELECT has_imputed_out
            FROM hourly_aggregations
            WHERE device_id = 'device_A'
            AND hour = '2024-01-01 08:00:00'
        """).fetchone()[0]

        assert flag == 1, f"Expected has_imputed_out=1, got {flag}"


class TestOccupancy:

    def test_occupancy_is_cumulative(self, raw_events_table):
        """
        The core occupancy test — calculated by hand:
            device_A 08:00: net_flow=6  → occupancy=6
            device_A 09:00: net_flow=-7 → occupancy=6+(-7)=-1

        Note: this produces negative occupancy.
        Our sanity check should warn about it.
        This is intentional — we test that the math is right,
        not that the data is realistic.
        """
        conn = raw_events_table
        create_hourly_aggregations(conn)
        create_occupancy(conn)

        rows = conn.execute("""
            SELECT hour, occupancy
            FROM occupancy
            WHERE device_id = 'device_A'
            ORDER BY hour
        """).fetchall()

        assert rows[0][1] == 6,  f"Expected occupancy=6 at 08:00,  got {rows[0][1]}"
        assert isinstance(rows[0][1], int), f"Expected int, got {type(rows[0][1])}"
        assert rows[1][1] == -1, f"Expected occupancy=-1 at 09:00, got {rows[1][1]}"
        assert isinstance(rows[1][1], int), f"Expected int, got {type(rows[1][1])}"

    def test_occupancy_partitioned_by_device(self, raw_events_table):
        """
        device_B's occupancy should be independent of device_A.
        device_B 08:00: net_flow = 4-1 = 3  → occupancy=3
        device_B 09:00: net_flow = 2-3 = -1 → occupancy=3+(-1)=2
        """
        conn = raw_events_table
        create_hourly_aggregations(conn)
        create_occupancy(conn)

        rows = conn.execute("""
            SELECT hour, occupancy
            FROM occupancy
            WHERE device_id = 'device_B'
            ORDER BY hour
        """).fetchall()

        assert rows[0][1] == 3, f"Expected occupancy=3 at 08:00, got {rows[0][1]}"
        assert isinstance(rows[0][1], int), f"Expected int, got {type(rows[0][1])}"
        assert rows[1][1] == 2, f"Expected occupancy=2 at 09:00, got {rows[1][1]}"
        assert isinstance(rows[1][1], int), f"Expected int, got {type(rows[1][1])}"


class TestDailyAggregations:

    def test_daily_sums_hourly_data(self, raw_events_table):
        """
        device_A across both hours on 2024-01-01:
            total_in  = 8 + 1 = 9
            total_out = 2 + 8 = 10
        """
        conn = raw_events_table
        create_hourly_aggregations(conn)
        create_occupancy(conn)
        create_daily_aggregations(conn)

        row = conn.execute("""
            SELECT total_in, total_out
            FROM daily_aggregations
            WHERE device_id = 'device_A'
            AND date = '2024-01-01'
        """).fetchone()

        assert row is not None,  "Expected a daily row for device_A"
        assert row[0] == 9,   f"Expected total_in=9,  got {row[0]}"
        assert isinstance(row[0], int), f"Expected int, got {type(row[0])}"
        assert row[1] == 10,  f"Expected total_out=10, got {row[1]}"
        assert isinstance(row[1], int), f"Expected int, got {type(row[1])}"

    def test_peak_occupancy_is_max(self, raw_events_table):
        """
        device_A occupancy values: [6, -1]
        peak_occupancy should be MAX = 6
        """
        conn = raw_events_table
        create_hourly_aggregations(conn)
        create_occupancy(conn)
        create_daily_aggregations(conn)

        peak = conn.execute("""
            SELECT peak_occupancy
            FROM daily_aggregations
            WHERE device_id = 'device_A'
        """).fetchone()[0]

        assert peak == 6, f"Expected peak_occupancy=6, got {peak}"
        assert isinstance(peak, int), f"Expected int, got {type(peak)}"

def test_negative_occupancy_is_flagged(raw_events_table):
    """
    Our fixture has device_A reaching occupancy=-1 at 09:00.
    That row should have occupancy_is_invalid=1.
    The value itself should be preserved as-is, not clamped.
    """
    conn = raw_events_table
    create_hourly_aggregations(conn)
    create_occupancy(conn)

    row = conn.execute("""
        SELECT occupancy, occupancy_is_invalid
        FROM occupancy
        WHERE device_id = 'device_A'
        AND hour = '2024-01-01 09:00:00'
    """).fetchone()

    assert row[0] == -1, f"Expected occupancy=-1 (preserved), got {row[0]}"
    assert row[1] == 1,  f"Expected occupancy_is_invalid=1, got {row[1]}"