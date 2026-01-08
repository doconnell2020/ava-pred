"""Shared pytest fixtures for ava-pred tests."""

import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir() -> Path:
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_incident_response() -> dict:
    """Sample incident API response."""
    return {
        "count": 2,
        "next": None,
        "previous": None,
        "results": [
            {"id": "incident-1"},
            {"id": "incident-2"},
        ],
    }


@pytest.fixture
def sample_incident_detail() -> dict:
    """Sample incident detail response."""
    return {
        "id": "incident-1",
        "ob_date": "2024-01-15",
        "location_coords": "[51.1234, -115.5678]",
        "location_coords_type": "Lat/lng",
        "title": "Test Incident",
    }


@pytest.fixture
def sample_weather_csv() -> str:
    """Sample weather CSV content."""
    return """Date/Time,Year,Month,Day,Max Temp (°C),Min Temp (°C),Mean Temp (°C),Total Rain (mm),Total Snow (cm),Total Precip (mm),Snow on Grnd (cm)
2024-01-15,2024,1,15,-5.0,-15.0,-10.0,0.0,5.0,5.0,25.0
2024-01-16,2024,1,16,-3.0,-12.0,-7.5,0.0,2.0,2.0,27.0
"""
