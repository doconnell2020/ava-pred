"""Create labeled datasets for ML training."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from common.config import get_settings

logger = logging.getLogger(__name__)

WEATHER_COLUMNS = [
    "Max Temp (°C)",
    "Min Temp (°C)",
    "Mean Temp (°C)",
    "Heat Deg Days (°C)",
    "Cool Deg Days (°C)",
    "Total Rain (mm)",
    "Total Snow (cm)",
    "Total Precip (mm)",
    "Snow on Grnd (cm)",
]


def _load_and_label(path: Path, label: int) -> pd.DataFrame:
    """Load weather CSV and add avalanche label.

    Args:
        path: Path to weather CSV file.
        label: 1 for avalanche, 0 for non-avalanche.

    Returns:
        DataFrame with weather features and label.
    """
    df = pd.read_csv(path, usecols=lambda c: c in WEATHER_COLUMNS)
    df = df.dropna(subset=["Max Temp (°C)"])
    df["avalanche"] = label
    return df


def create_labeled_dataset(
    avalanche_weather_path: Path | None = None,
    non_avalanche_weather_path: Path | None = None,
    output_dir: Path | None = None,
    balanced: bool = True,
    random_seed: int = 42,
) -> pd.DataFrame:
    """Create labeled dataset combining avalanche and non-avalanche weather.

    Args:
        avalanche_weather_path: Path to avalanche weather CSV.
        non_avalanche_weather_path: Path to non-avalanche weather CSV.
        output_dir: Directory to save output files.
        balanced: If True, undersample majority class to balance dataset.
        random_seed: Random seed for reproducibility.

    Returns:
        Combined labeled DataFrame.
    """
    settings = get_settings()

    avalanche_weather_path = (
        avalanche_weather_path or settings.processed_data_path / "avalanche_weather.csv"
    )
    non_avalanche_weather_path = (
        non_avalanche_weather_path or settings.processed_data_path / "non_avalanche_weather.csv"
    )
    output_dir = output_dir or settings.output_dir

    logger.info("Loading avalanche weather from %s", avalanche_weather_path)
    df_av = _load_and_label(avalanche_weather_path, label=1)
    logger.info("Loaded %d avalanche samples", len(df_av))

    logger.info("Loading non-avalanche weather from %s", non_avalanche_weather_path)
    df_non_av = _load_and_label(non_avalanche_weather_path, label=0)
    logger.info("Loaded %d non-avalanche samples", len(df_non_av))

    # Create full dataset
    full_df = pd.concat([df_av, df_non_av], ignore_index=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    full_output = output_dir / "full_dataset.csv"
    full_df.to_csv(full_output, index=False)
    logger.info("Saved full dataset (%d samples) to %s", len(full_df), full_output)

    # Create balanced dataset if requested
    if balanced:
        rng = np.random.default_rng(random_seed)
        minority_size = len(df_av)

        if len(df_non_av) > minority_size:
            indices = rng.choice(len(df_non_av), size=minority_size, replace=False)
            df_non_av_sampled = df_non_av.iloc[indices]
        else:
            df_non_av_sampled = df_non_av

        balanced_df = pd.concat([df_av, df_non_av_sampled], ignore_index=True)
        balanced_output = output_dir / "balanced_dataset.csv"
        balanced_df.to_csv(balanced_output, index=False)
        logger.info(
            "Saved balanced dataset (%d samples) to %s", len(balanced_df), balanced_output
        )
        return balanced_df

    return full_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_labeled_dataset()
