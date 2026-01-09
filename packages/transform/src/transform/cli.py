"""CLI for the transform package."""

import logging
from pathlib import Path
from typing import Annotated

import typer
from common.config import get_settings

app = typer.Typer(
    name="ava-transform",
    help="Transform avalanche and weather data.",
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
def coordinates(
    input_file: Annotated[
        Path | None,
        typer.Option("--input", "-i", help="Input parquet file with incidents"),
    ] = None,
    output_file: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output CSV file"),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging"),
    ] = False,
) -> None:
    """Normalize coordinate formats from incident data."""
    from transform.coordinates import normalize_coordinates

    setup_logging(verbose)
    settings = get_settings()

    input_path = input_file or settings.raw_data_path / "incidents.parquet"
    output_path = output_file or settings.processed_data_path / "incidents_normalized.csv"

    typer.echo(f"Normalizing coordinates from {input_path}")
    df = normalize_coordinates(input_path=input_path, output_path=output_path)
    typer.echo(f"Normalized {len(df)} coordinates to {output_path}")


@app.command()
def weather(
    avalanche_dir: Annotated[
        Path | None,
        typer.Option("--avalanche-dir", "-a", help="Directory with avalanche weather CSVs"),
    ] = None,
    non_avalanche_dir: Annotated[
        Path | None,
        typer.Option(
            "--non-avalanche-dir", "-n", help="Directory with non-avalanche weather CSVs"
        ),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output directory"),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging"),
    ] = False,
) -> None:
    """Aggregate weather data by date."""
    from transform.weather import aggregate_weather_data

    setup_logging(verbose)

    typer.echo("Aggregating weather data...")
    df_av, df_non_av = aggregate_weather_data(
        avalanche_weather_dir=avalanche_dir,
        non_avalanche_weather_dir=non_avalanche_dir,
        output_dir=output_dir,
    )
    typer.echo(f"Aggregated {len(df_av)} avalanche days, {len(df_non_av)} non-avalanche days")


@app.command()
def dataset(
    avalanche_weather: Annotated[
        Path | None,
        typer.Option("--avalanche", "-a", help="Avalanche weather CSV"),
    ] = None,
    non_avalanche_weather: Annotated[
        Path | None,
        typer.Option("--non-avalanche", "-n", help="Non-avalanche weather CSV"),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output directory"),
    ] = None,
    balanced: Annotated[
        bool,
        typer.Option("--balanced/--no-balanced", help="Create balanced dataset"),
    ] = True,
    seed: Annotated[
        int,
        typer.Option("--seed", help="Random seed for balancing"),
    ] = 42,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging"),
    ] = False,
) -> None:
    """Create labeled dataset for ML training."""
    from transform.dataset import create_labeled_dataset

    setup_logging(verbose)

    typer.echo("Creating labeled dataset...")
    df = create_labeled_dataset(
        avalanche_weather_path=avalanche_weather,
        non_avalanche_weather_path=non_avalanche_weather,
        output_dir=output_dir,
        balanced=balanced,
        random_seed=seed,
    )
    typer.echo(f"Created dataset with {len(df)} samples")


@app.command()
def all(
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging"),
    ] = False,
) -> None:
    """Run all transformation steps."""
    from transform.coordinates import normalize_coordinates
    from transform.dataset import create_labeled_dataset
    from transform.weather import aggregate_weather_data

    setup_logging(verbose)
    settings = get_settings()

    # Step 1: Normalize coordinates
    input_path = settings.raw_data_path / "incidents.parquet"
    if input_path.exists():
        typer.echo("Step 1: Normalizing coordinates...")
        output_path = settings.processed_data_path / "incidents_normalized.csv"
        df = normalize_coordinates(input_path=input_path, output_path=output_path)
        typer.echo(f"  Normalized {len(df)} coordinates")
    else:
        typer.echo(f"  Skipping coordinates: {input_path} not found")

    # Step 2: Aggregate weather
    typer.echo("Step 2: Aggregating weather data...")
    df_av, df_non_av = aggregate_weather_data()
    typer.echo(f"  Aggregated {len(df_av)} avalanche, {len(df_non_av)} non-avalanche days")

    # Step 3: Create dataset
    av_weather = settings.processed_data_path / "avalanche_weather.csv"
    non_av_weather = settings.processed_data_path / "non_avalanche_weather.csv"
    if av_weather.exists() and non_av_weather.exists():
        typer.echo("Step 3: Creating labeled dataset...")
        df = create_labeled_dataset()
        typer.echo(f"  Created dataset with {len(df)} samples")
    else:
        typer.echo("  Skipping dataset: weather aggregates not found")

    typer.echo("Done!")


if __name__ == "__main__":
    app()
