# backend/api/models.py
"""
Database models for the people counting API.
Each model maps exactly to one Parquet output file from the pipeline.
The field names and types must match the Parquet schema — if the
pipeline changes a column name, the model must change too.
"""

from django.db import models


class HourlyMetric(models.Model):
    """
    Maps to: output/hourly/hourly_aggregations.parquet
    Grain: one row per device per hour.
    Used for: hourly traffic charts, net flow visualization.
    """
    device_id       = models.CharField(max_length=100, db_index=True)
    hour            = models.DateTimeField(db_index=True)
    total_in        = models.IntegerField()
    total_out       = models.IntegerField()
    net_flow        = models.IntegerField()
    has_imputed_in  = models.BooleanField(default=False)
    has_imputed_out = models.BooleanField(default=False)

    class Meta:
        # Composite index — queries filtering by both device AND hour
        # are very common. This makes them fast.
        indexes = [
            models.Index(fields=['device_id', 'hour'])
        ]
        # Enforce uniqueness — one row per device per hour
        unique_together = [['device_id', 'hour']]
        ordering = ['device_id', 'hour']

    def __str__(self):
        return f"{self.device_id} @ {self.hour}"


class OccupancySnapshot(models.Model):
    """
    Maps to: output/occupancy/occupancy.parquet
    Grain: one row per device per hour.
    Used for: occupancy line chart, current occupancy KPI.
    """
    device_id            = models.CharField(max_length=100, db_index=True)
    hour                 = models.DateTimeField(db_index=True)
    total_in             = models.IntegerField()
    total_out            = models.IntegerField()
    net_flow             = models.IntegerField()
    occupancy            = models.IntegerField()
    occupancy_is_invalid = models.BooleanField(default=False)
    has_imputed_in       = models.BooleanField(default=False)
    has_imputed_out      = models.BooleanField(default=False)

    class Meta:
        indexes = [
            models.Index(fields=['device_id', 'hour'])
        ]
        unique_together = [['device_id', 'hour']]
        ordering = ['device_id', 'hour']

    def __str__(self):
        return f"{self.device_id} @ {self.hour} — occupancy: {self.occupancy}"


class DailySummary(models.Model):
    """
    Maps to: output/daily/daily_aggregations.parquet
    Grain: one row per device per day.
    Used for: daily KPI cards, trend charts, peak occupancy.
    """
    device_id       = models.CharField(max_length=100, db_index=True)
    date            = models.DateField(db_index=True)
    total_in        = models.IntegerField()
    total_out       = models.IntegerField()
    net_flow        = models.IntegerField()
    peak_occupancy  = models.IntegerField()
    min_occupancy   = models.IntegerField()

    class Meta:
        indexes = [
            models.Index(fields=['device_id', 'date'])
        ]
        unique_together = [['device_id', 'date']]
        ordering = ['device_id', 'date']

    def __str__(self):
        return f"{self.device_id} on {self.date}"