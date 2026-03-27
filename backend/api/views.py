# backend/api/views.py
"""
API views — each view handles one endpoint.

All views are read-only (GET only). The pipeline writes data,
Django only reads and serves it. This is enforced by using
ReadOnlyModelViewSet which disables POST, PUT, DELETE.
"""

from rest_framework import viewsets, filters
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import HourlyMetric, OccupancySnapshot, DailySummary
from .serializers import (
    HourlyMetricSerializer,
    OccupancySnapshotSerializer,
    DailySummarySerializer
)


class HourlyMetricViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GET /api/hourly/
    GET /api/hourly/?device_id=device_A
    GET /api/hourly/?device_id=device_A&date=2024-01-01
    """
    serializer_class = HourlyMetricSerializer

    def get_queryset(self):
        queryset = HourlyMetric.objects.all()

        # Optional filters from query parameters
        device_id = self.request.query_params.get('device_id')
        date      = self.request.query_params.get('date')

        if device_id:
            queryset = queryset.filter(device_id=device_id)
        if date:
            queryset = queryset.filter(hour__date=date)

        return queryset


class OccupancyViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GET /api/occupancy/
    GET /api/occupancy/?device_id=device_A
    GET /api/occupancy/?device_id=device_A&date=2024-01-01
    """
    serializer_class = OccupancySnapshotSerializer

    def get_queryset(self):
        queryset = OccupancySnapshot.objects.all()

        device_id = self.request.query_params.get('device_id')
        date      = self.request.query_params.get('date')

        if device_id:
            queryset = queryset.filter(device_id=device_id)
        if date:
            queryset = queryset.filter(hour__date=date)

        return queryset


class DailySummaryViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GET /api/daily/
    GET /api/daily/?device_id=device_A
    """
    serializer_class = DailySummarySerializer

    def get_queryset(self):
        queryset = DailySummary.objects.all()

        device_id = self.request.query_params.get('device_id')
        if device_id:
            queryset = queryset.filter(device_id=device_id)

        return queryset


@api_view(['GET'])
def device_list(request):
    """
    GET /api/devices/
    Returns a list of all known device IDs.
    Used to populate the device selector in the dashboard.
    """
    devices = (
        DailySummary.objects
        .values_list('device_id', flat=True)
        .distinct()
        .order_by('device_id')
    )
    return Response(list(devices))