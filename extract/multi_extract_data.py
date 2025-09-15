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
        logger.info(f"Fetching incident IDs from {url}")
        if resp.status != 200:
            logging.error(f"Got status code {resp.status}")
            return []
        else:
            data = await resp.json()
            results = data.get("results")
            ids = [dct["id"] for dct in results]
            logger.info(f"Found {len(ids)} incident IDs")
            return ids


# Next we need to visit each url and extract each id
async def get_incident_ids(urls: List[str]) -> List[str]:
    """Given a list of urls, return the incident iasyncio.run(ds found at each) URL.

    params: urls: List[str]
    returns: ids: List[str]
    """
    async with aiohttp.ClientSession() as session:
        list_ids = []
        tasks = [get_incident_id(url, session) for url in urls]
        for done in asyncio.as_completed(tasks):
            try:
                ids = await done
                list_ids.extend(ids)
            except asyncio.TimeoutError:
                logger.error(f"Got status code {done}")
    return list_ids


def main() -> None:
    logging.info("Starting get_URLS")
    urls = get_urls("https://incidents.avalanche.ca/public/incidents/?format=json")
    logging.info("Finished get_URLS")
    logging.info("Starting get_incident_ids")

    incident_ids = asyncio.run(get_incident_ids(urls))


if __name__ == "__main__":
    start = time.time()
    main()
    time_taken = time.time() - start
    print("Time to taken for extract_data.py to run: {}s.".format(round(time_taken, 3)))
