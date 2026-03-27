# backend/api/urls.py
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# Router automatically generates URL patterns for ViewSets
# HourlyMetricViewSet → /api/hourly/ and /api/hourly/{id}/
router = DefaultRouter()
router.register(r'hourly',    views.HourlyMetricViewSet,   basename='hourly')
router.register(r'occupancy', views.OccupancyViewSet,      basename='occupancy')
router.register(r'daily',     views.DailySummaryViewSet,   basename='daily')

urlpatterns = [
    path('', include(router.urls)),
    path('devices/', views.device_list, name='device-list'),
]