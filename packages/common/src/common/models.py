"""Pydantic models for data validation and serialization."""

from datetime import date, datetime

from pydantic import BaseModel, Field, field_validator


class Incident(BaseModel):
    """Avalanche incident data model."""

    id: str
    ob_date: date
    latitude: float | None = None
    longitude: float | None = None
    location_coords: str | None = None
    location_coords_type: str | None = None

    @field_validator("ob_date", mode="before")
    @classmethod
    def parse_date(cls, v: str | date | datetime) -> date:
        """Parse date from various formats."""
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            # Handle common date formats
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue
            raise ValueError(f"Unable to parse date: {v}")
        raise TypeError(f"Expected str, date, or datetime, got {type(v)}")


class WeatherStation(BaseModel):
    """Weather station metadata."""

    station_id: str
    station_name: str
    latitude: float
    longitude: float
    first_year: int | None = None
    last_year: int | None = None


class WeatherRecord(BaseModel):
    """Daily weather observation record."""

    station_id: str
    date: date
    max_temp: float | None = Field(default=None, alias="Max Temp (°C)")
    min_temp: float | None = Field(default=None, alias="Min Temp (°C)")
    mean_temp: float | None = Field(default=None, alias="Mean Temp (°C)")
    heat_deg_days: float | None = Field(default=None, alias="Heat Deg Days (°C)")
    cool_deg_days: float | None = Field(default=None, alias="Cool Deg Days (°C)")
    total_rain: float | None = Field(default=None, alias="Total Rain (mm)")
    total_snow: float | None = Field(default=None, alias="Total Snow (cm)")
    total_precip: float | None = Field(default=None, alias="Total Precip (mm)")
    snow_on_ground: float | None = Field(default=None, alias="Snow on Grnd (cm)")

    model_config = {"populate_by_name": True}


class LabeledWeatherRecord(BaseModel):
    """Weather record with avalanche label for ML training."""

    max_temp: float | None = None
    min_temp: float | None = None
    mean_temp: float | None = None
    heat_deg_days: float | None = None
    cool_deg_days: float | None = None
    total_rain: float | None = None
    total_snow: float | None = None
    total_precip: float | None = None
    snow_on_ground: float | None = None
    avalanche: bool = Field(description="Whether an avalanche occurred")
