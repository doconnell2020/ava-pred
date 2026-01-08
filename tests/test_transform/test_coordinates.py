"""Tests for transform.coordinates module."""

import pandas as pd
import pytest
from transform.coordinates import _split_coordinates


class TestSplitCoordinates:
    """Tests for _split_coordinates helper function."""

    def test_basic_split(self) -> None:
        """Test splitting basic coordinate strings."""
        series = pd.Series(["[51.1234, -115.5678]", "[49.0, -120.0]"])
        lat, lon = _split_coordinates(series)

        assert lat.iloc[0] == "51.1234"
        assert lon.iloc[0] == "-115.5678"
        assert lat.iloc[1] == "49.0"
        assert lon.iloc[1] == "-120.0"

    def test_strips_brackets(self) -> None:
        """Test that brackets are stripped."""
        series = pd.Series(["[51.0, -115.0]"])
        lat, lon = _split_coordinates(series)

        assert "[" not in lat.iloc[0]
        assert "]" not in lon.iloc[0]


class TestNormalizeCoordinates:
    """Tests for normalize_coordinates function."""

    def test_lat_lng_format(self, temp_dir) -> None:
        """Test normalization of Lat/lng format coordinates."""
        from unittest.mock import patch

        from transform.coordinates import normalize_coordinates

        # Create test input
        df = pd.DataFrame(
            {
                "ob_date": ["2024-01-15"],
                "location_coords": ["[51.1234, -115.5678]"],
                "location_coords_type": ["Lat/lng"],
            }
        )
        input_path = temp_dir / "test_input.csv"
        df.to_csv(input_path, index=False)

        # Mock the Canada filter to return the input unchanged
        with patch("transform.coordinates._filter_to_canada", side_effect=lambda x: x):
            result = normalize_coordinates(input_path=input_path)

        # Check results
        assert "latitude" in result.columns
        assert "longitude" in result.columns
        assert len(result) == 1
        assert abs(result.iloc[0]["latitude"] - 51.1234) < 0.001
        assert abs(result.iloc[0]["longitude"] - (-115.5678)) < 0.001

    def test_missing_columns_raises_error(self, temp_dir) -> None:
        """Test that missing required columns raise TransformError."""
        from common.exceptions import TransformError
        from transform.coordinates import normalize_coordinates

        # Create input missing required columns
        df = pd.DataFrame({"ob_date": ["2024-01-15"]})
        input_path = temp_dir / "bad_input.csv"
        df.to_csv(input_path, index=False)

        with pytest.raises(TransformError):
            normalize_coordinates(input_path=input_path)
