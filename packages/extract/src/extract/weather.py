"""Fetch weather data from Environment Canada."""

import asyncio
import logging
from datetime import date
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import ClientSession
from common.config import get_settings

logger = logging.getLogger(__name__)

WEATHER_URL_TEMPLATE = (
    "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"
    "?format=csv&stationID={station_id}&Year={year}&Month={month}&Day={day}&timeframe=2"
)


async def _fetch_single_weather(
    session: ClientSession,
    station_id: str,
    observation_date: date,
    output_dir: Path,
    station_name: str = "",
) -> Path | None:
    """Fetch weather data for a single station and date.

    Args:
        session: An aiohttp ClientSession.
        station_id: The weather station ID.
        observation_date: The date to fetch weather for.
        output_dir: Directory to save the CSV file.
        station_name: Optional station name for the filename.

    Returns:
        Path to the saved file, or None if fetch failed.
    """
    url = WEATHER_URL_TEMPLATE.format(
        station_id=station_id,
        year=observation_date.year,
        month=observation_date.month,
        day=observation_date.day,
    )

    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning(
                    "Failed to fetch weather for station %s on %s: status %d",
                    station_id,
                    observation_date,
                    resp.status,
                )
                return None

            content = await resp.read()

            safe_name = station_name.replace(" ", "_") if station_name else "unknown"
            filename = f"{observation_date}_{safe_name}_{station_id}.csv"
            output_path = output_dir / filename

            output_path.write_bytes(content)
            logger.debug("Saved weather data to %s", output_path)
            return output_path

    except aiohttp.ClientError as e:
        logger.warning("Error fetching weather for station %s: %s", station_id, e)
        return None


async def fetch_weather_daily(
    stations: list[dict[str, Any]],
    output_dir: Path | None = None,
) -> list[Path]:
    """Fetch daily weather data for multiple stations.

    Args:
        stations: List of dicts with keys: station_id, ob_date, station_name (optional).
        output_dir: Directory to save CSV files. Uses settings default if not provided.

    Returns:
        List of paths to successfully downloaded files.
    """
    settings = get_settings()
    output_dir = output_dir or settings.raw_data_path / "weather"
    output_dir.mkdir(parents=True, exist_ok=True)

    results: list[Path] = []

    async with aiohttp.ClientSession() as session:
        for station in stations:
            station_id = str(station["station_id"])
            ob_date: str | date = station["ob_date"]
            station_name = str(station.get("station_name", ""))

            if isinstance(ob_date, str):
                ob_date = date.fromisoformat(ob_date)

            path = await _fetch_single_weather(
                session=session,
                station_id=station_id,
                observation_date=ob_date,
                output_dir=output_dir,
                station_name=station_name,
            )

            if path:
                results.append(path)

            await asyncio.sleep(settings.api_request_delay)

    logger.info("Downloaded %d weather files to %s", len(results), output_dir)
    return results


async def main() -> None:
    """Main entry point for weather extraction."""
    import pandas as pd

    settings = get_settings()
    stations_file = settings.input_dir / "nearest_stations.csv"

    if not stations_file.exists():
        logger.error("Stations file not found: %s", stations_file)
        return

    df = pd.read_csv(stations_file, parse_dates=["ob_date"])
    stations: list[dict[str, Any]] = df.to_dict("records")  # type: ignore[assignment]

    await fetch_weather_daily(stations)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    asyncio.run(main())
