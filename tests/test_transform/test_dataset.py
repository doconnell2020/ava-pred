"""Tests for transform.dataset module."""

import pandas as pd
import pytest
from transform.dataset import _load_and_label, create_labeled_dataset


class TestLoadAndLabel:
    """Tests for _load_and_label helper function."""

    def test_adds_label_column(self, temp_dir) -> None:
        """Test that label column is added."""
        # Create test CSV
        df = pd.DataFrame(
            {
                "Max Temp (°C)": [-5.0, -3.0],
                "Min Temp (°C)": [-15.0, -12.0],
                "Total Snow (cm)": [5.0, 2.0],
            }
        )
        test_path = temp_dir / "test_weather.csv"
        df.to_csv(test_path, index=False)

        result = _load_and_label(test_path, label=1)

        assert "avalanche" in result.columns
        assert all(result["avalanche"] == 1)

    def test_drops_na_rows(self, temp_dir) -> None:
        """Test that rows with missing max temp are dropped."""
        df = pd.DataFrame(
            {
                "Max Temp (°C)": [-5.0, None, -3.0],
                "Min Temp (°C)": [-15.0, -10.0, -12.0],
            }
        )
        test_path = temp_dir / "test_weather.csv"
        df.to_csv(test_path, index=False)

        result = _load_and_label(test_path, label=0)

        assert len(result) == 2


class TestCreateLabeledDataset:
    """Tests for create_labeled_dataset function."""

    def test_combines_avalanche_and_non_avalanche(self, temp_dir) -> None:
        """Test that both datasets are combined."""
        # Create avalanche weather
        av_df = pd.DataFrame(
            {
                "Max Temp (°C)": [-5.0, -3.0],
                "Min Temp (°C)": [-15.0, -12.0],
            }
        )
        av_path = temp_dir / "avalanche.csv"
        av_df.to_csv(av_path, index=False)

        # Create non-avalanche weather
        non_av_df = pd.DataFrame(
            {
                "Max Temp (°C)": [0.0, 2.0, 5.0, 8.0],
                "Min Temp (°C)": [-5.0, -2.0, 0.0, 3.0],
            }
        )
        non_av_path = temp_dir / "non_avalanche.csv"
        non_av_df.to_csv(non_av_path, index=False)

        result = create_labeled_dataset(
            avalanche_weather_path=av_path,
            non_avalanche_weather_path=non_av_path,
            output_dir=temp_dir,
            balanced=False,
        )

        # Should have all samples
        assert len(result) == 6
        assert sum(result["avalanche"]) == 2  # 2 avalanche samples
        assert sum(~result["avalanche"].astype(bool)) == 4  # 4 non-avalanche

    def test_balanced_dataset(self, temp_dir) -> None:
        """Test that balanced option undersamples majority class."""
        # Create avalanche weather (minority)
        av_df = pd.DataFrame(
            {
                "Max Temp (°C)": [-5.0, -3.0],
                "Min Temp (°C)": [-15.0, -12.0],
            }
        )
        av_path = temp_dir / "avalanche.csv"
        av_df.to_csv(av_path, index=False)

        # Create non-avalanche weather (majority)
        non_av_df = pd.DataFrame(
            {
                "Max Temp (°C)": [0.0, 2.0, 5.0, 8.0, 10.0, 12.0],
                "Min Temp (°C)": [-5.0, -2.0, 0.0, 3.0, 5.0, 7.0],
            }
        )
        non_av_path = temp_dir / "non_avalanche.csv"
        non_av_df.to_csv(non_av_path, index=False)

        result = create_labeled_dataset(
            avalanche_weather_path=av_path,
            non_avalanche_weather_path=non_av_path,
            output_dir=temp_dir,
            balanced=True,
            random_seed=42,
        )

        # Should be balanced: 2 avalanche + 2 non-avalanche
        assert len(result) == 4
        assert sum(result["avalanche"]) == 2
        assert sum(~result["avalanche"].astype(bool)) == 2

    def test_creates_output_files(self, temp_dir) -> None:
        """Test that output files are created."""
        # Create test data
        av_df = pd.DataFrame({"Max Temp (°C)": [-5.0], "Min Temp (°C)": [-15.0]})
        non_av_df = pd.DataFrame({"Max Temp (°C)": [0.0], "Min Temp (°C)": [-5.0]})

        av_path = temp_dir / "avalanche.csv"
        non_av_path = temp_dir / "non_avalanche.csv"
        av_df.to_csv(av_path, index=False)
        non_av_df.to_csv(non_av_path, index=False)

        create_labeled_dataset(
            avalanche_weather_path=av_path,
            non_avalanche_weather_path=non_av_path,
            output_dir=temp_dir,
            balanced=True,
        )

        assert (temp_dir / "full_dataset.csv").exists()
        assert (temp_dir / "balanced_dataset.csv").exists()
