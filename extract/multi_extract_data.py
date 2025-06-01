import logging
import multiprocessing
import time
from typing import List

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)

session = None


def set_global_session():
    global session
    if not session:
        session = requests.Session()


def get_URLs(initial_url: str) -> List[str]:
    """
    Generates a List of URLs based on the expected format of the API endpoints and
    the number of events and events per page. This is ~15x faster than visiting
    each endpoint to find the next one.
    The `initial_url` acts as a jumping off point to begin the iteration.

    param: initial_url: str
    returns: List[str]
    """
    urls = [initial_url]
    data = requests.get(initial_url).json()
    total_events = data["count"]
    events_per_page = len(data["results"])
    num_pages = int(total_events / events_per_page) + 1
    if num_pages < 2:
        return urls
    else:
        for i in range(2, num_pages + 1):
            urls.append(f"{initial_url}&page={i}")
    return urls


def get_event_ids(url: str):
    event_ids = pd.read_json(url)["results"].apply(lambda row: row["id"])
    return event_ids


def all_events(urls: List[str]):
    with multiprocessing.Pool(processes=8, initializer=set_global_session) as pool:
        df = pd.concat(pool.map(get_event_ids, urls))
    return df.to_list()


def get_avalanche(event: str):
    with session.get(
        f"https://incidents.avalanche.ca/public/incidents/{event}/?format=json"
    ) as data:
        df = pd.json_normalize(data.json())
    return df


def all_avalanches(events: List[str]):
    with multiprocessing.Pool(processes=8, initializer=set_global_session) as pool:
        df = pd.concat(pool.map(get_avalanche, events))
    return df


def main() -> None:
    logging.info("Starting get_URLS")
    urls = get_URLs("https://incidents.avalanche.ca/public/incidents/?format=json")
    logging.info("Finished get_URLS")
    logging.info("Starting get_incident_ids")

    incident_ids = all_events(urls)
    logging.info("Finished get_incident_ids")
    logging.info("Starting generate_canadian_avalanche_data")
    av_data = all_avalanches(incident_ids)
    av_data.to_csv("./data/multiproc_can_avs_raw.csv")
    logging.info("Finished generate_canadian_avalanche_data")
    logging.info("Retrieving weather stastions.")

    # Get the canadian weather station data
    pd.read_csv(
        "https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/Station%20Inventory%20EN.csv",
        skiprows=3,
        dtype={
            "First Year": "Int64",
            "Last Year": "Int64",
            "HLY First Year": "Int64",
            "HLY Last Year": "Int64",
            "DLY First Year": "Int64",
            "DLY Last Year": "Int64",
            "MLY First Year": "Int64",
            "MLY Last Year": "Int64",
        },
    ).to_csv(
        "/home/david/Documents/ARU/AvalancheProject/demo/data/station_inv.csv",
        index=False,
    )


if __name__ == "__main__":
    start = time.time()
    main()
    time_taken = time.time() - start
    print(f"Time to taken for extract_data.py to run: {round(time_taken, 3)}s.")
