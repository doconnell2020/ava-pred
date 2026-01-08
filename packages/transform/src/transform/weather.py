"""Aggregate weather data from individual CSV files."""

import logging
from pathlib import Path

import pandas as pd
from common.config import get_settings

logger = logging.getLogger(__name__)


def _load_weather_directory(weather_dir: Path) -> pd.DataFrame:
    """Load all CSV files from a directory into a single DataFrame.

    Args:
        weather_dir: Directory containing weather CSV files.

    Returns:
        Combined DataFrame with all weather data.
    """
    frames: list[pd.DataFrame] = []

    for csv_file in weather_dir.glob("*.csv"):
        try:
            df = pd.read_csv(csv_file)
            frames.append(df)
        except Exception as e:
            logger.warning("Failed to read %s: %s", csv_file, e)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def aggregate_weather_data(
    avalanche_weather_dir: Path | None = None,
    non_avalanche_weather_dir: Path | None = None,
    observation_dates_path: Path | None = None,
    output_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate weather data by date for avalanche and non-avalanche days.

    Args:
        avalanche_weather_dir: Directory with weather data for avalanche days.
        non_avalanche_weather_dir: Directory with weather data for non-avalanche days.
        observation_dates_path: CSV file with observation dates to filter by.
        output_dir: Directory to save aggregated CSV files.

    Returns:
        Tuple of (avalanche_weather_df, non_avalanche_weather_df).
    """
    settings = get_settings()

    avalanche_weather_dir = avalanche_weather_dir or settings.raw_data_path / "weather_avalanche"
    non_avalanche_weather_dir = (
        non_avalanche_weather_dir or settings.raw_data_path / "weather_non_avalanche"
    )
    observation_dates_path = observation_dates_path or settings.input_dir / "nearest_stations.csv"
    output_dir = output_dir or settings.processed_data_path

    # Load observation dates
    if observation_dates_path.exists():
        av_dates = pd.read_csv(observation_dates_path, usecols=["ob_date"])
        av_dates_set = set(av_dates["ob_date"].astype(str))
    else:
        logger.warning("Observation dates file not found: %s", observation_dates_path)
        av_dates = pd.DataFrame(columns=["ob_date"])
        av_dates_set = set()

    # Process avalanche weather data
    logger.info("Loading avalanche weather data from %s", avalanche_weather_dir)
    df_av = _load_weather_directory(avalanche_weather_dir)

    if not df_av.empty and "Date/Time" in df_av.columns:
        df_av["ob_date"] = df_av["Date/Time"]
        df_av = pd.merge(av_dates, df_av, on="ob_date", how="inner")
        df_av_grouped = df_av.groupby("ob_date").mean(numeric_only=True).dropna()
    else:
        df_av_grouped = pd.DataFrame()

    # Process non-avalanche weather data
    logger.info("Loading non-avalanche weather data from %s", non_avalanche_weather_dir)
    df_non_av = _load_weather_directory(non_avalanche_weather_dir)

    if not df_non_av.empty and "Date/Time" in df_non_av.columns:
        df_non_av["ob_date"] = df_non_av["Date/Time"]
        df_non_av_grouped = (
            df_non_av.groupby("ob_date").mean(numeric_only=True).reset_index().dropna()
        )
        # Exclude avalanche dates
        df_non_av_grouped = df_non_av_grouped[
            ~df_non_av_grouped["ob_date"].astype(str).isin(av_dates_set)
        ]
    else:
        df_non_av_grouped = pd.DataFrame()

    # Save outputs
    output_dir.mkdir(parents=True, exist_ok=True)

    if not df_av_grouped.empty:
        av_output = output_dir / "avalanche_weather.csv"
        df_av_grouped.to_csv(av_output, index=False)
        logger.info("Saved avalanche weather to %s", av_output)

    if not df_non_av_grouped.empty:
        non_av_output = output_dir / "non_avalanche_weather.csv"
        df_non_av_grouped.to_csv(non_av_output, index=False)
        logger.info("Saved non-avalanche weather to %s", non_av_output)

    return df_av_grouped, df_non_av_grouped


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    aggregate_weather_data()
