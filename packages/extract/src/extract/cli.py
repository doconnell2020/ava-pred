"""CLI for the extract package."""

import asyncio
import logging
from pathlib import Path
from typing import Annotated, Any

import typer
from common.config import get_settings

app = typer.Typer(
    name="ava-extract",
    help="Extract avalanche incident and weather data.",
    no_args_is_help=True,
)


def setup_logging(verbose: bool) -> None:
    """Configure logging based on verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


@app.command()
def incidents(
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output path for parquet file"),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging"),
    ] = False,
) -> None:
    """Extract avalanche incident data from the Canadian Avalanche API."""
    from extract.incidents import fetch_incidents

    setup_logging(verbose)
    settings = get_settings()
    output_path = output or settings.raw_data_path / "incidents.parquet"

    typer.echo(f"Extracting incidents to {output_path}")
    df = asyncio.run(fetch_incidents(output_path=output_path))
    typer.echo(f"Extracted {len(df)} incidents")


@app.command()
def weather(
    stations_file: Annotated[
        Path | None,
        typer.Option("--stations", "-s", help="CSV file with station info"),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output directory for weather CSVs"),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging"),
    ] = False,
) -> None:
    """Extract weather data from Environment Canada."""
    import pandas as pd

    from extract.weather import fetch_weather_daily

    setup_logging(verbose)
    settings = get_settings()

    stations_path = stations_file or settings.input_dir / "nearest_stations.csv"
    if not stations_path.exists():
        typer.echo(f"Error: Stations file not found: {stations_path}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Reading stations from {stations_path}")
    df = pd.read_csv(stations_path, parse_dates=["ob_date"])
    stations: list[dict[str, Any]] = df.to_dict("records")  # type: ignore[assignment]

    typer.echo(f"Fetching weather data for {len(stations)} station-date pairs")
    results = asyncio.run(fetch_weather_daily(stations, output_dir))
    typer.echo(f"Downloaded {len(results)} weather files")


@app.command()
def all(
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging"),
    ] = False,
) -> None:
    """Extract all data (incidents and weather)."""
    from extract.incidents import fetch_incidents

    setup_logging(verbose)
    settings = get_settings()

    # Extract incidents
    typer.echo("Extracting incidents...")
    output_path = settings.raw_data_path / "incidents.parquet"
    df = asyncio.run(fetch_incidents(output_path=output_path))
    typer.echo(f"Extracted {len(df)} incidents")

    # Check for weather stations file
    stations_path = settings.input_dir / "nearest_stations.csv"
    if stations_path.exists():
        import pandas as pd

        from extract.weather import fetch_weather_daily

        typer.echo("Extracting weather data...")
        stations_df = pd.read_csv(stations_path, parse_dates=["ob_date"])
        stations: list[dict[str, Any]] = stations_df.to_dict("records")  # type: ignore[assignment]
        results = asyncio.run(fetch_weather_daily(stations))
        typer.echo(f"Downloaded {len(results)} weather files")
    else:
        typer.echo(f"Skipping weather: {stations_path} not found")


if __name__ == "__main__":
    app()
