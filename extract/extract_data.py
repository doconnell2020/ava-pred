import asyncio
import logging
import time
from typing import List

import aiohttp
import pandas as pd
import requests
from aiohttp import ClientSession

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def get_urls(url: str) -> list[str] | None:
    """Generates a List of URLs based on the expected format of the API endpoints and
    the number of events and events per page.
    This is ~15x faster than visiting each endpoint to find the next one.
    The `url` acts as a jumping off point to begin the iteration.

    param: url: str
    returns: List of urls
    """
    urls = [url]
    resp = requests.get(url)
    if resp.status_code != 200:
        logging.error(f"Got status code {resp.status_code}")
        raise
    data = resp.json()
    event_count = data["count"]
    events_per_page = len(data["results"])
    num_pages = int(event_count / events_per_page) + 1
    if num_pages < 2:
        return urls
    else:
        urls.extend([f"{url}&page={i}" for i in range(2, num_pages + 1)])
        return urls

async def get_incident_id(url: str, session: ClientSession) -> List[str]:
    """Get the incident IDs from an API endpoint.

    Args:
        url: The API endpoint URL.
        session: An aiohttp ClientSession.

    Returns:
        A list of incident IDs.
    """
    async with session.get(url) as resp:
        logger.info(f"Got status code {resp.status}")
        if resp.status != 200:
            logging.error(f"Got status code {resp.status}")
            return []
        else:
            data = resp.json()
            results = data.get("results")
            ids = [dct["id"] for dct in results]
            return ids



# Next we need to visit each url and extract each id
def get_incident_ids(urls: List[str]) -> List[str]:
    """Given a list of urls, return the incident ids found at each URL.

    params: urls: List[str]
    returns: ids: List[str]
    """
    with aiohttp.ClientSession() as session:
        tasks = [get_incident_id(url, session) for url in urls]
        list_ids = asyncio.run(asyncio.gather(*tasks))


    return list_ids


# Lastly, build the full data source
def generate_canadian_avalanche_data(incident_ids: List[str]) -> str:
    df = pd.DataFrame()
    for id in incident_ids:
        data = requests.get(
            "https://incidents.avalanche.ca/public/incidents/{}/?format=json".format(id)
        ).json()
        new_df = pd.json_normalize(data)
        df = pd.concat([df, new_df])
    df.to_parquet("data/can_avs_raw.parquet", index=False)

    return "Data was saved."


def main() -> None:
    logging.info("Starting get_URLS")
    urls = get_urls("https://incidents.avalanche.ca/public/incidents/?format=json")
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
    ).to_parquet(
        "data/station_inv.parquet",
        index=False,
    )


if __name__ == "__main__":
    start = time.time()
    main()
    time_taken = time.time() - start
    print("Time to taken for extract_data.py to run: {}s.".format(round(time_taken, 3)))
