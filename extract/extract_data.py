import logging
import time
from typing import List

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)


def get_URLs(initial_url: str) -> List[str]:
    """
    Generates a List of URLs based on the expected format of the API enpoints and
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
        pass
    else:
        for i in range(2, num_pages + 1):
            urls.append(f"{initial_url}&page={i}")
    return urls


# Next we need to visit each url and extract each id
def get_incident_ids(URLS: List[str]) -> List[str]:
    """Given a list of URLs, return the incident ids found at each URL.

    params: URLS: List[str]
    returns: ids: List[str]
    """
    ids = pd.Series(name="results", dtype="str")
    for url in URLS:
        new_ids = pd.read_json(url)["results"].apply(lambda row: row["id"])
        ids = pd.concat([ids, new_ids])
    return ids.to_list()


# Lastly, build the full data source
def generate_canadian_avalanche_data(incident_ids: List[str]) -> str:
    df = pd.DataFrame()
    for id in incident_ids:
        data = requests.get(
            "https://incidents.avalanche.ca/public/incidents/{}/?format=json".format(id)
        ).json()
        new_df = pd.json_normalize(data)
        df = pd.concat([df, new_df])
    df.to_csv("./data/can_avs_raw.csv")

    return "Data was saved."


def main() -> None:
    logging.info("Starting get_URLS")
    urls = get_URLs("https://incidents.avalanche.ca/public/incidents/?format=json")
    logging.info("Finished get_URLS")
    logging.info("Starting get_incident_ids")

    incident_ids = get_incident_ids(urls)
    logging.info("Finished get_incident_ids")
    logging.info("Starting generate_canadian_avalanche_data")
    generate_canadian_avalanche_data(incident_ids)
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
    print("Time to taken for extract_data.py to run: {}s.".format(round(time_taken, 3)))
