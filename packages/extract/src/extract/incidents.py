"""Fetch avalanche incident data from the Canadian Avalanche API."""

import asyncio
import logging
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
import requests
from aiohttp import ClientSession
from common.config import get_settings
from common.exceptions import ExtractionError

logger = logging.getLogger(__name__)


def generate_incident_urls(base_url: str | None = None) -> list[str]:
    """Generate paginated URLs for the incidents API.

    This is ~15x faster than visiting each endpoint to find the next one.

    Args:
        base_url: Base API URL. Uses settings default if not provided.

    Returns:
        List of paginated URLs to fetch.

    Raises:
        ExtractionError: If the initial API request fails.
    """

    settings = get_settings()
    url = base_url or f"{settings.incidents_api_url}?format=json"

    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        raise ExtractionError(f"API returned status {resp.status_code}", url=url)

    data = resp.json()
    event_count = data["count"]
    events_per_page = len(data["results"])

    if events_per_page == 0:
        return [url]

    num_pages = (event_count // events_per_page) + 1
    urls = [url]

    if num_pages > 1:
        urls.extend(f"{url}&page={i}" for i in range(2, num_pages + 1))

    return urls


async def _fetch_incident_ids_from_page(url: str, session: ClientSession) -> list[str]:
    """Fetch incident IDs from a single API page.

    Args:
        url: The API endpoint URL.
        session: An aiohttp ClientSession.

    Returns:
        List of incident IDs from this page.
    """
    async with session.get(url) as resp:
        if resp.status != 200:
            logger.warning("Got status %d from %s", resp.status, url)
            return []

        data = await resp.json()
        results = data.get("results", [])
        return [item["id"] for item in results]


async def get_incident_ids(urls: list[str] | None = None) -> list[str]:
    """Fetch all incident IDs from the API.

    Args:
        urls: List of paginated URLs. Generates them if not provided.

    Returns:
        List of all incident IDs.
    """
    if urls is None:
        urls = generate_incident_urls()

    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_incident_ids_from_page(url, session) for url in urls]
        nested_ids = await asyncio.gather(*tasks)

    return [inc_id for page_ids in nested_ids for inc_id in page_ids]


async def fetch_incident(incident_id: str, session: ClientSession) -> dict[str, Any] | None:
    """Fetch a single incident's full data.

    Args:
        incident_id: The incident ID to fetch.
        session: An aiohttp ClientSession.

    Returns:
        The incident data as a dict, or None if fetch failed.
    """
    settings = get_settings()
    url = f"{settings.incidents_api_url}{incident_id}/?format=json"

    async with session.get(url) as resp:
        if resp.status != 200:
            logger.warning("Failed to fetch incident %s: status %d", incident_id, resp.status)
            return None
        data: dict[str, Any] = await resp.json()
        return data


async def fetch_incidents(
    incident_ids: list[str] | None = None,
    output_path: Path | None = None,
) -> pd.DataFrame:
    """Fetch full data for all incidents.

    Args:
        incident_ids: List of incident IDs. Fetches all if not provided.
        output_path: Optional path to save the output parquet file.

    Returns:
        DataFrame containing all incident data.
    """
    settings = get_settings()

    if incident_ids is None:
        logger.info("Fetching incident IDs...")
        incident_ids = await get_incident_ids()
        logger.info("Found %d incidents", len(incident_ids))

    logger.info("Fetching incident data...")
    records: list[dict[str, Any]] = []

    async with aiohttp.ClientSession() as session:
        # Process in batches to avoid overwhelming the API
        batch_size = settings.max_concurrent_requests
        for i in range(0, len(incident_ids), batch_size):
            batch = incident_ids[i : i + batch_size]
            tasks = [fetch_incident(inc_id, session) for inc_id in batch]
            results = await asyncio.gather(*tasks)
            records.extend(r for r in results if r is not None)

            if i + batch_size < len(incident_ids):
                await asyncio.sleep(settings.api_request_delay)

    df = pd.json_normalize(records)

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info("Saved %d incidents to %s", len(df), output_path)

    return df


async def main() -> None:
    """Main entry point for incident extraction."""
    settings = get_settings()
    output_path = settings.raw_data_path / "incidents.parquet"
    await fetch_incidents(output_path=output_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    asyncio.run(main())
