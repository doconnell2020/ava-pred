"""Normalize coordinate formats from avalanche incident data.

Handles four coordinate types:
1. LatLon: Reversed lat/lon, needs sign correction
2. Lat/lng: Correct format
3. Lat/Long Decimal Degrees: Correct format
4. UTM: Needs projection conversion

Rules:
- All longitude should be negative (Western Hemisphere)
- Latitude should not exceed 90 (swap if needed)
- Filter to points within Canada
"""

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from common.config import get_settings
from common.exceptions import TransformError

logger = logging.getLogger(__name__)


def _split_coordinates(series: "pd.Series[str]") -> tuple["pd.Series[str]", "pd.Series[str]"]:
    """Split coordinate string into latitude and longitude series."""
    coordinates = series.str.strip("[]").str.split(", ", expand=True)
    return coordinates[0], coordinates[1]


def _parse_utm_coordinates(df: pd.DataFrame) -> tuple[list[float], list[float]]:
    """Convert UTM coordinates to lat/lon.

    Args:
        df: DataFrame with location_coords and location_coords_type columns.

    Returns:
        Tuple of (latitudes, longitudes) lists.
    """
    coords = df["location_coords_type"].str.replace("(assumed)", "", regex=False).str.split(expand=True)
    zone = coords[1].str.extract(r"^(\d+)", expand=False).values
    datum = coords[2].values

    eastings_series, northings_series = _split_coordinates(df["location_coords"])
    eastings = eastings_series.values
    northings = northings_series.values

    lats: list[float] = []
    longs: list[float] = []

    for i in range(len(df)):
        if datum[i] == "Unknown":
            lats.append(np.nan)
            longs.append(np.nan)
        else:
            try:
                utm_proj = pyproj.Proj(proj="utm", zone=zone[i], datum=datum[i])
                wgs84_proj = pyproj.Proj(proj="latlong", datum="WGS84")
                lon, lat = pyproj.transform(utm_proj, wgs84_proj, eastings[i], northings[i])
                lats.append(lat)
                longs.append(lon)
            except Exception as e:
                logger.warning("Failed to convert UTM coordinates at index %d: %s", i, e)
                lats.append(np.nan)
                longs.append(np.nan)

    return lats, longs


def _filter_to_canada(df: pd.DataFrame) -> pd.DataFrame:
    """Filter DataFrame to only include points within Canada."""
    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
    canada = world[world.name == "Canada"]

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="EPSG:4326",
    )

    joined = gpd.tools.sjoin(gdf, canada, how="left", predicate="within")
    filtered = joined[joined["index_right"].notna()].drop(columns=["index_right", "geometry"])

    return pd.DataFrame(filtered)


def normalize_coordinates(
    input_path: Path | None = None,
    output_path: Path | None = None,
) -> pd.DataFrame:
    """Normalize coordinate formats from raw incident data.

    Args:
        input_path: Path to raw incident data. Uses settings default if not provided.
        output_path: Optional path to save normalized data.

    Returns:
        DataFrame with normalized lat/lon columns.

    Raises:
        TransformError: If required columns are missing.
    """
    settings = get_settings()
    input_path = input_path or settings.raw_data_path / "incidents.parquet"

    logger.info("Loading incident data from %s", input_path)

    if input_path.suffix == ".parquet":
        df = pd.read_parquet(input_path)
    else:
        df = pd.read_csv(input_path)

    required_cols = ["ob_date", "location_coords", "location_coords_type"]
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise TransformError(f"Missing required columns: {missing}", source=str(input_path))

    result = df[required_cols].copy()
    result["location_coords"] = result["location_coords"].astype(str)
    result["location_coords_type"] = result["location_coords_type"].astype(str)

    # Create masks for different coordinate types
    lat_lng_mask = result["location_coords_type"] == "Lat/lng"
    lat_lng_dd_mask = result["location_coords_type"] == "Lat/Long Decimal Degrees"
    lat_lon_mask = result["location_coords_type"] == "LatLon"
    utm_mask = result["location_coords_type"].str.startswith("UTM")

    # Initialize coordinate columns
    result["latitude"] = np.nan
    result["longitude"] = np.nan

    # Process Lat/lng format (correct order)
    if lat_lng_mask.any():
        lat, lon = _split_coordinates(result.loc[lat_lng_mask, "location_coords"])
        result.loc[lat_lng_mask, "latitude"] = lat.astype(float)
        result.loc[lat_lng_mask, "longitude"] = lon.astype(float)

    # Process Lat/Long Decimal Degrees (correct order)
    if lat_lng_dd_mask.any():
        lat, lon = _split_coordinates(result.loc[lat_lng_dd_mask, "location_coords"])
        result.loc[lat_lng_dd_mask, "latitude"] = lat.astype(float)
        result.loc[lat_lng_dd_mask, "longitude"] = lon.astype(float)

    # Process LatLon format (reversed order)
    if lat_lon_mask.any():
        lon, lat = _split_coordinates(result.loc[lat_lon_mask, "location_coords"])
        result.loc[lat_lon_mask, "latitude"] = lat.astype(float)
        result.loc[lat_lon_mask, "longitude"] = lon.astype(float)

    # Process UTM format
    if utm_mask.any():
        logger.info("Converting %d UTM coordinates", utm_mask.sum())
        lats, lons = _parse_utm_coordinates(result[utm_mask])
        result.loc[utm_mask, "latitude"] = lats
        result.loc[utm_mask, "longitude"] = lons

    # Sanity checks: latitude cannot exceed 90
    result["latitude"] = result["latitude"].abs()
    swap_mask = result["latitude"] > 90
    if swap_mask.any():
        logger.info("Swapping %d coordinates where latitude > 90", swap_mask.sum())
        result.loc[swap_mask, ["latitude", "longitude"]] = result.loc[
            swap_mask, ["longitude", "latitude"]
        ].values

    # Ensure correct signs for Canadian coordinates
    result["longitude"] = -result["longitude"].abs()
    result["latitude"] = result["latitude"].abs()

    # Drop rows with missing coordinates
    result = result.dropna(subset=["latitude", "longitude"])

    # Filter to points within Canada
    logger.info("Filtering to points within Canada")
    result = _filter_to_canada(result)

    logger.info("Normalized %d coordinates", len(result))

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output_path, index=False)
        logger.info("Saved to %s", output_path)

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()
    normalize_coordinates(
        output_path=settings.processed_data_path / "incidents_normalized.csv"
    )
