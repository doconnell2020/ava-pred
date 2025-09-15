import os
import time
import warnings

import pandas as pd

warnings.filterwarnings("ignore")

start = time.time()

avalanche_weather_dir = "/home/david/Documents/ARU/AvalancheProject/demo/data/daily_weather_avs"
dull_weather_dir = "/home/david/Documents/ARU/AvalancheProject/demo/data/daily_weather_dulls"


def single_dataframe(root_path: str) -> pd.DataFrame:
    """
    Pass in a root directory containing csv files and return a singe
    pandas DataFrame.
    """
    results = pd.DataFrame()
    for file in os.listdir(root_path):
        results = pd.concat([results, pd.read_csv(root_path + "/" + file)], ignore_index=True)
    return results.reset_index().drop(columns="index")


# Read in the dates where avalanches happened.
av_dates = pd.read_csv(
    "/home/david/Documents/ARU/AvalancheProject/demo/data/nearest_stations.csv",
    usecols=["ob_date"],
)

df_av = single_dataframe(avalanche_weather_dir)
df_av["ob_date"] = df_av["Date/Time"]
df_av = pd.merge(av_dates, df_av, on="ob_date", how="inner")
group_av = df_av.groupby(by="ob_date").mean().dropna()
group_av.to_csv(
    "/home/david/Documents/ARU/AvalancheProject/demo/data/avs_weather.csv", index=False
)

df_dull = single_dataframe(dull_weather_dir)
df_dull["ob_date"] = df_dull["Date/Time"]
group_dull = df_dull.groupby(by="ob_date").mean().reset_index().dropna()
group_dull = group_dull[~group_dull["ob_date"].isin(av_dates)]
group_dull.to_csv(
    "/home/david/Documents/ARU/AvalancheProject/demo/data/dull_weather.csv", index=False
)

time_taken = time.time() - start
print("Time to taken for transformWeather.py to run: {}s.".format(round(time_taken, 3)))
