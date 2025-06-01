import time

import numpy as np
import pandas as pd

start = time.time()

av_path = "/home/david/Documents/ARU/AvalancheProject/demo/data/avs_weather.csv"
dull_path = "/home/david/Documents/ARU/AvalancheProject/demo/data/dull_weather.csv"


def load_and_label(path: str, label: int) -> pd.DataFrame:
    """
    Loads CSV from path, loading the appropriate columns.
    Removes null Max Temp values and adds a bool value to a new "avalanche" column.
    """
    columns_to_load = [
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
    df = pd.read_csv(path, usecols=columns_to_load)
    df = df[df["Max Temp (°C)"].notnull()]
    df["avalanche"] = label
    return df


# Create DataFrames for both 1s and 0s.
df_av = load_and_label(av_path, 1)
df_dull = load_and_label(dull_path, 0)

# Create full dataset
pd.concat([df_av, df_dull]).reset_index().drop(columns="index").to_csv(
    "/home/david/Documents/ARU/AvalancheProject/demo/load/full_cleaned.csv", index=False
)


# Create a randomised index to have a balanced dataset
random_idx = np.arange(len(df_dull.index))
np.random.shuffle(random_idx)

df_dull_short = df_dull.iloc[random_idx][: len(df_av)]
pd.concat([df_av, df_dull_short]).reset_index().drop(columns="index").to_csv(
    "/home/david/Documents/ARU/AvalancheProject/demo/load/balanced_cleaned.csv",
    index=False,
)

time_taken = time.time() - start
print(
    "Time to taken for transformFinalData.py to run: {}s.".format(round(time_taken, 3))
)
