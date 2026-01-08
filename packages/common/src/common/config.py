"""Configuration management using pydantic-settings."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="AVA_",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Paths
    data_dir: Path = Field(default=Path("data"), description="Root data directory")
    output_dir: Path = Field(default=Path("data/output"), description="Output data directory")
    input_dir: Path = Field(default=Path("data/input"), description="Input data directory")

    # API Configuration
    incidents_api_url: str = Field(
        default="https://incidents.avalanche.ca/public/incidents/",
        description="Base URL for avalanche incidents API",
    )
    weather_api_url: str = Field(
        default="https://climate.weather.gc.ca/climate_data/bulk_data_e.html",
        description="Base URL for weather data API",
    )

    # Processing settings
    api_request_delay: float = Field(
        default=0.1,
        description="Delay between API requests in seconds",
    )
    max_concurrent_requests: int = Field(
        default=10,
        description="Maximum concurrent API requests",
    )

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")

    @property
    def raw_data_path(self) -> Path:
        """Path for raw extracted data."""
        return self.output_dir / "raw"

    @property
    def processed_data_path(self) -> Path:
        """Path for processed/transformed data."""
        return self.output_dir / "processed"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
