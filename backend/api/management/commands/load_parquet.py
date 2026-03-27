# backend/api/management/commands/load_parquet.py
"""
Management command: loads pipeline Parquet output into Django's database.

Run with: python manage.py load_parquet --output-dir ../output

Key design decisions:
- Clears existing data before loading (idempotent — safe to run repeatedly)
- Loads all three tables in dependency order
- Reports row counts for verification
"""

import os
import duckdb
from django.core.management.base import BaseCommand
from api.models import HourlyMetric, OccupancySnapshot, DailySummary


class Command(BaseCommand):
    help = "Loads Parquet pipeline output into Django database"

    def add_arguments(self, parser):
        parser.add_argument(
            '--output-dir',
            type=str,
            default='../output',
            help='Path to the pipeline output directory'
        )

    def handle(self, *args, **options):
        output_dir = options['output_dir']
        conn = duckdb.connect(database=':memory:')

        self.stdout.write("Loading Parquet files into Django database...")

        # Load each table
        self._load_hourly(conn, output_dir)
        self._load_occupancy(conn, output_dir)
        self._load_daily(conn, output_dir)

        self.stdout.write(self.style.SUCCESS("\nAll tables loaded successfully."))

    def _load_hourly(self, conn, output_dir):
        path = os.path.join(output_dir, "hourly", "hourly_aggregations.parquet")
        safe_path = path.replace("\\", "/")

        rows = conn.execute(f"""
            SELECT device_id, hour, total_in, total_out,
                   net_flow, has_imputed_in, has_imputed_out
            FROM read_parquet('{safe_path}')
            ORDER BY device_id, hour
        """).fetchall()

        # Clear existing data — makes this command idempotent
        HourlyMetric.objects.all().delete()

        # Bulk create is much faster than creating one row at a time
        # It sends one INSERT with all rows instead of N separate INSERTs
        HourlyMetric.objects.bulk_create([
            HourlyMetric(
                device_id       = row[0],
                hour            = row[1],
                total_in        = row[2],
                total_out       = row[3],
                net_flow        = row[4],
                has_imputed_in  = bool(row[5]),
                has_imputed_out = bool(row[6]),
            )
            for row in rows
        ])

        self.stdout.write(f"  ✓ HourlyMetric:       {len(rows)} rows")

    def _load_occupancy(self, conn, output_dir):
        path = os.path.join(output_dir, "occupancy", "occupancy.parquet")
        safe_path = path.replace("\\", "/")

        rows = conn.execute(f"""
            SELECT device_id, hour, total_in, total_out, net_flow,
                   occupancy, occupancy_is_invalid,
                   has_imputed_in, has_imputed_out
            FROM read_parquet('{safe_path}')
            ORDER BY device_id, hour
        """).fetchall()

        OccupancySnapshot.objects.all().delete()

        OccupancySnapshot.objects.bulk_create([
            OccupancySnapshot(
                device_id            = row[0],
                hour                 = row[1],
                total_in             = row[2],
                total_out            = row[3],
                net_flow             = row[4],
                occupancy            = row[5],
                occupancy_is_invalid = bool(row[6]),
                has_imputed_in       = bool(row[7]),
                has_imputed_out      = bool(row[8]),
            )
            for row in rows
        ])

        self.stdout.write(f"  ✓ OccupancySnapshot:  {len(rows)} rows")

    def _load_daily(self, conn, output_dir):
        path = os.path.join(output_dir, "daily", "daily_aggregations.parquet")
        safe_path = path.replace("\\", "/")

        rows = conn.execute(f"""
            SELECT device_id, date, total_in, total_out,
                   net_flow, peak_occupancy, min_occupancy
            FROM read_parquet('{safe_path}')
            ORDER BY device_id, date
        """).fetchall()

        DailySummary.objects.all().delete()

        DailySummary.objects.bulk_create([
            DailySummary(
                device_id      = row[0],
                date           = row[1],
                total_in       = row[2],
                total_out      = row[3],
                net_flow       = row[4],
                peak_occupancy = row[5],
                min_occupancy  = row[6],
            )
            for row in rows
        ])

        self.stdout.write(f"  ✓ DailySummary:       {len(rows)} rows")