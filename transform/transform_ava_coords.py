"""
This script reads in the raw avalanche incident data and reprocesses the coordinate
information so that it is standardised.

There are 4 categories of 'location_coords' in the source file,
therefore it needs 4 different treatments.

Rules to think about
- We can assume all longtitude should be negative (Western Hemisphere)
- Decimal precision is not very important
- Latitude should not be >90, otherwise swap.
- UTM is only 2 distict types
- 4 types to cover:
    1. LatLon :  Reversed and sign needed
    2. Lat/lng : Correct
    3. Lat/Long Decimal Degrees : Correct
    4. UTM (starts with) : Remove letters, function parse
"""

import logging
import time
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from geopy.geocoders import Nominatim

logging.basicConfig(level=logging.INFO)
start = time.time()

warnings.filterwarnings("ignore")


df = pd.read_csv("data/can_avs_raw.csv")

# Create new df with reduced info
new_df = df[["ob_date", "location_coords", "location_coords_type"]].copy()

new_df["location_coords"] = new_df["location_coords"].astype(str)
new_df["location_coords_type"] = new_df["location_coords_type"].astype(str)

# Create idxs for all types of location coordinate type
# These two are in the correct position
lat_lng_idx = new_df["location_coords_type"] == "Lat/lng"
lat_lng_dd_idx = new_df["location_coords_type"] == "Lat/Long Decimal Degrees"

# These rows have their latitude and longitude reversed, so that will have to be caught
# during assignment
lat_lon_idx = new_df["location_coords_type"] == "LatLon"

# The UTM format has more cleaning to go through than the others; removing letters,
# the "assumed" word etc.
utm_idx = new_df["location_coords_type"].str.startswith("UTM")


# Define some processing functions


def split_coordinates(series: pd.Series) -> tuple:
    """
    Splits the series from a point to individual latitude and longitude series.
    """
    coordinates = series.str.strip("[]").str.split(", ", expand=True)
    latitude, longitude = coordinates
    return latitude, longitude


# The UTM type requires more wrangling to parse


def parse_utm(df: pd.DataFrame) -> tuple:
    """
    Processing function for UTM location type.
    """
    coords = df["location_coords_type"].str.strip("(assumed)").str.split(expand=True)
    _, zone, datum = coords[0], coords[1], coords[2]
    eastings, northings = split_coordinates(df["location_coords"])

    zone = zone.str.extract(r"^(\d+)", expand=False).values
    datum = datum.values
    eastings = eastings.values
    northings = northings.values

    lats, longs = [], []
    for i in range(len(df)):
        if datum[i] == "Unknown":
            lats.append(np.nan)
            longs.append(np.nan)
        else:
            utm_proj = pyproj.Proj(proj="utm", zone=zone[i], datum=datum[i])
            wgs84_proj = pyproj.Proj(proj="latlong", datum="WGS84")
            long, lat = pyproj.transform(
                utm_proj, wgs84_proj, eastings[i], northings[i]
            )
            lats.append(lat)
            longs.append(long)

    return lats, longs


# Function to check if coordinates are within Canada
def is_within_canada(latitude: float, longitude: float) -> bool:
    geolocator = Nominatim(user_agent="my-app")
    location = geolocator.reverse((latitude, longitude), exactly_one=True)
    if location is None:
        return False
    country = location.raw["address"].get("country")
    return country == "Canada"


logging.info("Creating indices.")
# These two are in the correct position
lat_lng_idx = new_df["location_coords_type"] == "Lat/lng"
lat_lng_dd_idx = new_df["location_coords_type"] == "Lat/Long Decimal Degrees"

# These rows have their latitude and longitude reversed, so that will have to be caught
# during assignment
lat_lon_idx = new_df["location_coords_type"] == "LatLon"

# The UTM format has more cleaning to go through than the others; removing letters,
# the "assumed" word etc.
utm_idx = new_df["location_coords_type"].str.startswith("UTM")

new_df[["latitude", "longitude"]] = split_coordinates(
    new_df["location_coords"][lat_lng_idx]
)

new_df[["latitude", "longitude"]] = split_coordinates(
    new_df["location_coords"][lat_lng_dd_idx]
)

# Recall, the lats and longs in these rows are reversed, hence reverse assignment
new_df[["longitude", "latitude"]] = split_coordinates(
    new_df["location_coords"][lat_lon_idx]
)

logging.info("Starting UTM processing.")
lats_4, longs_4 = parse_utm(new_df[utm_idx])
new_df["latitude"].loc[utm_idx] = lats_4
new_df["longitude"].loc[utm_idx] = longs_4
logging.info("UTM processing complete.")
# --------------------------------------------------------------------------------------
# Potential optimisation but unable to implement yet. Keep getting Index error or access
# before assignment.

# Use Numpy to conditionally apply functions
# new_df["latitude"], new_df["longitude"] = np.where(
#    df["location_coords_type"] == utm_idx,
#    parse_utm(new_df),
#    split_coordinates(new_df["location_coords_type"]),
# )

## Reversing lat_lon_idx values.

# new_df.loc[lat_lon_idx, ["latitude", "longitude"]] = new_df.loc[
#    lat_lon_idx, ["longitude", "latitude"]
# ].values
#
# --------------------------------------------------------------------------------------


## Latittude cannot be >90. If it is, it is swapped with longitude
logging.info("Sanity check latitudes.")
new_df["latitude"] = abs(new_df["latitude"].astype(float))


idx = new_df["latitude"] > 90

new_df.loc[idx, ["latitude", "longitude"]] = new_df.loc[
    idx, ["longitude", "latitude"]
].values


# All longitude values should be <0 and all latitude should be >0 based on location.
new_df["longitude"] = -1 * abs(new_df["longitude"].astype(float))
new_df["latitude"] = abs(new_df["latitude"].astype(float))

new_df = new_df.dropna()
logging.info("Sanity check: Within Canada.")
# new_df["IsWithinCanada"] = new_df.apply(
# lambda row: is_within_canada(row["latitude"], row["longitude"]), axis=1
# )
world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))

can = world[world.name == "Canada"]
data = gpd.GeoDataFrame(
    new_df, geometry=gpd.points_from_xy(new_df.longitude, new_df.latitude)
)
p = gpd.tools.sjoin(data, can, how="left")
p = p.loc[p["index_right"] > 0]
# new_df = new_df.loc[new_df["IsWithinCanada"] == True]  # noqa: E712
logging.info("Canada check complete.")
# new_df.drop(columns="IsWithinCanada").dropna().to_csv(
# "./data/can_avs_lat_long_date.csv", index=False
# )

time_taken = time.time() - start
print(
    "Time to taken for transform_ava_coords.py to run: {}s.".format(
        round(time_taken, 3)
    )
)
