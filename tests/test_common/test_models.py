"""Tests for common.models module."""

from datetime import date, datetime

import pytest
from common.models import Incident, WeatherRecord, WeatherStation
from pydantic import ValidationError


class TestIncident:
    """Tests for Incident model."""

    def test_valid_incident(self) -> None:
        """Test creating a valid incident."""
        incident = Incident(
            id="test-123",
            ob_date="2024-01-15",
            latitude=51.1234,
            longitude=-115.5678,
        )
        assert incident.id == "test-123"
        assert incident.ob_date == date(2024, 1, 15)
        assert incident.latitude == 51.1234
        assert incident.longitude == -115.5678

    def test_date_parsing_iso_format(self) -> None:
        """Test date parsing with ISO format."""
        incident = Incident(id="test", ob_date="2024-01-15")
        assert incident.ob_date == date(2024, 1, 15)

    def test_date_parsing_slash_format(self) -> None:
        """Test date parsing with slash format."""
        incident = Incident(id="test", ob_date="2024/01/15")
        assert incident.ob_date == date(2024, 1, 15)

    def test_date_from_datetime(self) -> None:
        """Test date parsing from datetime object with zero time."""
        dt = datetime(2024, 1, 15, 0, 0, 0)
        incident = Incident(id="test", ob_date=dt)
        assert incident.ob_date == date(2024, 1, 15)

    def test_date_from_date(self) -> None:
        """Test date parsing from date object."""
        d = date(2024, 1, 15)
        incident = Incident(id="test", ob_date=d)
        assert incident.ob_date == d

    def test_optional_coordinates(self) -> None:
        """Test that coordinates are optional."""
        incident = Incident(id="test", ob_date="2024-01-15")
        assert incident.latitude is None
        assert incident.longitude is None

    def test_invalid_date_format(self) -> None:
        """Test that invalid date formats raise error."""
        with pytest.raises(ValidationError):
            Incident(id="test", ob_date="not-a-date")


class TestWeatherStation:
    """Tests for WeatherStation model."""

    def test_valid_station(self) -> None:
        """Test creating a valid weather station."""
        station = WeatherStation(
            station_id="1234",
            station_name="Test Station",
            latitude=51.0,
            longitude=-115.0,
            first_year=2000,
            last_year=2024,
        )
        assert station.station_id == "1234"
        assert station.station_name == "Test Station"

    def test_optional_years(self) -> None:
        """Test that year fields are optional."""
        station = WeatherStation(
            station_id="1234",
            station_name="Test",
            latitude=51.0,
            longitude=-115.0,
        )
        assert station.first_year is None
        assert station.last_year is None


class TestWeatherRecord:
    """Tests for WeatherRecord model."""

    def test_valid_record(self) -> None:
        """Test creating a valid weather record."""
        record = WeatherRecord(
            station_id="1234",
            date=date(2024, 1, 15),
            max_temp=-5.0,
            min_temp=-15.0,
            total_snow=5.0,
        )
        assert record.station_id == "1234"
        assert record.max_temp == -5.0

    def test_alias_mapping(self) -> None:
        """Test that field aliases work."""
        record = WeatherRecord(
            station_id="1234",
            date=date(2024, 1, 15),
            **{"Max Temp (°C)": -5.0, "Min Temp (°C)": -15.0}
        )
        assert record.max_temp == -5.0
        assert record.min_temp == -15.0

    def test_optional_fields(self) -> None:
        """Test that weather fields are optional."""
        record = WeatherRecord(
            station_id="1234",
            date=date(2024, 1, 15),
        )
        assert record.max_temp is None
        assert record.total_precip is None
