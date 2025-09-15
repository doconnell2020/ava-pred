import random
import time

import pandas as pd
import requests

start = time.time()

url = """https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={id}&Year={year}&Month={month}&Day={day}&timeframe=2&submit=%20Download+Data"""

df = pd.read_csv(
    "/home/david/Documents/ARU/AvalancheProject/demo/data/nearest_stations.csv",
    parse_dates=["ob_date"],
)


def get_weather_daily(df: pd.DataFrame, url=url) -> str:
    for i in range(len(df.index)):
        time.sleep(0.1)
        response = requests.get(
            url.format(
                id=df.iloc[i]["station_id"],
                day=df.iloc[i]["ob_date"].day,
                month=df.iloc[i]["ob_date"].month,
                year=df.iloc[i]["ob_date"].year,
            )
        )
        response.raise_for_status()  # Raise an exception if the request failed
        output_file = "/home/david/Documents/ARU/AvalancheProject/demo/data/daily_weather_avs/{}_{}_{}.csv".format(
            str(df.iloc[i]["ob_date"].date()),
            df.iloc[i]["station_name"],
            df.iloc[i]["station_id"],
        ).replace(" ", "_")
        print(f"Writing to file {output_file}")
        with open(output_file, "wb") as f:
            f.write(response.content)
    return "End of list."


def get_weather_daily_randoms(df: pd.DataFrame, url=url) -> str:
    for i in range(len(df.index)):
        success = False
        for _ in range(10):
            try:
                time.sleep(0.5)
                day = random.choice(list(range(1, 29)))  # Choose from the winter/spring months
                month = random.choice([1, 2, 3, 4, 11, 12])  # Choose from most of any month
                year = df.iloc[i]["ob_date"].year
                response = requests.get(
                    url.format(id=df.iloc[i]["station_id"], day=day, month=month, year=year)
                )
                response.raise_for_status()  # Raise an exception if the request failed
                success = True
                break
            except requests.exceptions.ConnectionError:
                continue

        if success:
            output_file = "/home/david/Documents/ARU/AvalancheProject/demo/data/daily_weather_dulls/{}_{}_{}.csv".format(
                f"{year}_{month}_{day}",
                df.iloc[i]["station_name"],
                df.iloc[i]["station_id"],
            ).replace(" ", "_")
            try:
                with open(output_file, "wb") as f:
                    f.write(response.content)
            except Exception as e:
                print(f"Error writing to file {output_file}: {e}")
                continue

        else:
            print(f"No successful response after 10 attempts for line {i}")
            continue

    return "End of list."


get_weather_daily(df)

# get_weather_daily_randoms(df)

time_taken = time.time() - start
print("Time to taken for extract_weather.py to run: {}s.".format(round(time_taken, 3)))
