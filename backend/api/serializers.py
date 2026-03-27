# backend/api/serializers.py
"""
Serializers convert Django model instances to JSON for API responses.
Each serializer maps to one model and controls exactly which fields
are exposed — you may not want to expose every internal field.
"""

from rest_framework import serializers
from .models import HourlyMetric, OccupancySnapshot, DailySummary


class HourlyMetricSerializer(serializers.ModelSerializer):
    class Meta:
        model  = HourlyMetric
        fields = [
            'device_id', 'hour', 'total_in', 'total_out',
            'net_flow', 'has_imputed_in', 'has_imputed_out'
        ]


class OccupancySnapshotSerializer(serializers.ModelSerializer):
    class Meta:
        model  = OccupancySnapshot
        fields = [
            'device_id', 'hour', 'occupancy',
            'occupancy_is_invalid', 'has_imputed_out'
        ]


class DailySummarySerializer(serializers.ModelSerializer):
    class Meta:
        model  = DailySummary
        fields = [
            'device_id', 'date', 'total_in', 'total_out',
            'net_flow', 'peak_occupancy', 'min_occupancy'
        ]