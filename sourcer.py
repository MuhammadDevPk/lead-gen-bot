#!/usr/bin/env python3
"""
sourcer.py: Queries Serper.dev Places API to find local service businesses.
Sorts businesses into leads with websites (saving URLs to data/source_urls.txt)
and leads without websites (saving details to data/no_website_leads.jsonl).
"""

import argparse
import asyncio
import datetime
import json
import logging
import os
import sys
from pathlib import Path
from typing import Set, Dict, List, Any, Optional

import aiofiles
import httpx
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Setup logs directory
Path("logs").mkdir(exist_ok=True)

# Configure Logging to file logs/sourcer.log and stdout
log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler("logs/sourcer.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("lead-sourcer")


def get_api_key() -> Optional[str]:
    """Retrieves Serper.dev API key from environment variables."""
    key = os.getenv("SERPER_API_KEY")
    if not key:
        key = os.getenv("SERP_API_KEY")
    return key


def parse_arguments() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="B2B Lead Sourcer using Serper.dev Places API."
    )
    parser.add_argument(
        "query",
        type=str,
        help="Search query to run (e.g. 'Roofers in Dallas, TX')."
    )
    parser.add_argument(
        "-u", "--urls-file",
        type=str,
        default="data/source_urls.txt",
        help="Path to output file for leads WITH websites (default: data/source_urls.txt)."
    )
    parser.add_argument(
        "-n", "--no-website-file",
        type=str,
        default="data/no_website_leads.jsonl",
        help="Path to output JSONL file for leads WITHOUT websites (default: data/no_website_leads.jsonl)."
    )
    return parser.parse_args()


async def load_existing_urls(file_path: Path) -> Set[str]:
    """Loads existing URLs to prevent duplicate entries in source_urls.txt."""
    if not file_path.exists():
        return set()

    existing: Set[str] = set()
    try:
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            async for line in f:
                url = line.strip()
                if url:
                    existing.add(url)
    except Exception as e:
        logger.error(f"Failed to read existing URLs file {file_path}: {e}")
    return existing


async def load_existing_no_website_leads(file_path: Path) -> Set[str]:
    """Loads existing no-website leads to prevent duplicates (keyed by title + phone)."""
    if not file_path.exists():
        return set()

    existing_keys: Set[str] = set()
    try:
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            async for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    title = data.get("title", "")
                    phone = data.get("phoneNumber", "")
                    # Generate a unique key based on name and phone
                    key = f"{title.lower()}|{phone}"
                    existing_keys.add(key)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.error(f"Failed to read existing no-website leads file {file_path}: {e}")
    return existing_keys


async def query_serper_places(query: str, api_key: str) -> List[Dict[str, Any]]:
    """Makes a POST request to Serper.dev Places API."""
    url = "https://google.serper.dev/places"
    payload = {"q": query}
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    }

    logger.info(f"Querying Serper Places API with query: '{query}'...")
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, headers=headers, json=payload, timeout=30.0)
            response.raise_for_status()
            data = response.json()
            places = data.get("places", [])
            logger.info(f"Successfully retrieved {len(places)} results from API.")
            return places
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred while querying Serper: {e.response.status_code} - {e.response.text}")
            raise RuntimeError(f"Serper API HTTP Error: {e.response.status_code}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred during API call: {e}", exc_info=True)
            raise


async def verify_business_website(business_name: str, address: Optional[str], api_key: str) -> Optional[str]:
    """Queries Serper Google Search to check if a business actually owns a website (double verification)."""
    from urllib.parse import urlparse
    city = ""
    if address:
        parts = address.split(",")
        if len(parts) > 1:
            city = parts[1].strip()
            
    query = f"{business_name} {city}".strip()
    url = "https://google.serper.dev/search"
    payload = {"q": query}
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    }
    
    # Common directory domains to exclude
    directory_exclusions = (
        "yelp.com", "tripadvisor.com", "facebook.com", "yellowpages.com", "groupon.com",
        "mapquest.com", "foursquare.com", "instagram.com", "linkedin.com", "twitter.com",
        "pinterest.com", "grubhub.com", "opentable.com", "doordash.com", "ubereats.com",
        "wikipedia.org", "yahoo.com", "local.yahoo.com", "realtor.com",
        "zillow.com", "trulia.com", "angi.com", "homeadvisor.com", "nextdoor.com"
    )
    
    try:
        logger.info(f"[DOUBLE-VERIFY] Checking if {business_name} in {city} has an official website...")
        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, json=payload, timeout=15.0)
            response.raise_for_status()
            data = response.json()
            
        organic = data.get("organic", [])
        for item in organic:
            link = item.get("link")
            if not link:
                continue
            
            parsed = urlparse(link)
            domain = parsed.netloc.lower()
            if domain.startswith("www."):
                domain = domain[4:]
                
            # Check if domain is a directory
            if not any(ex in domain for ex in directory_exclusions):
                logger.info(f"[DOUBLE-VERIFY FOUND SITE] {business_name} actually owns website: {link}")
                return link
                
        return None
    except Exception as e:
        logger.error(f"Error double-verifying website for {business_name}: {e}")
        return None


async def process_leads(
    places: List[Dict[str, Any]],
    urls_file: Path,
    no_website_file: Path,
    api_key: str
) -> None:
    """Processes search results and splits them into website URLs and no-website JSONL leads."""
    # Ensure parent directories exist
    urls_file.parent.mkdir(parents=True, exist_ok=True)
    no_website_file.parent.mkdir(parents=True, exist_ok=True)

    # 1. Load existing data for deduplication
    existing_urls = await load_existing_urls(urls_file)
    existing_no_website = await load_existing_no_website_leads(no_website_file)

    new_urls_count = 0
    new_no_website_count = 0

    async with aiofiles.open(urls_file, "a", encoding="utf-8") as f_urls, \
               aiofiles.open(no_website_file, "a", encoding="utf-8") as f_no_web:

        for place in places:
            title = place.get("title", "Unknown")
            website = place.get("website", "").strip()
            phone = place.get("phoneNumber", "").strip()
            address = place.get("address", "")

            # If Serper Places claims no website, double-verify using standard Google search fallback
            if not website:
                # Sleep briefly to be respectful to API rate limits
                await asyncio.sleep(0.5)
                website_verified = await verify_business_website(title, address, api_key)
                if website_verified:
                    website = website_verified.strip()

            if website:
                # Clean URL (strip whitespace)
                if website not in existing_urls:
                    await f_urls.write(website + "\n")
                    existing_urls.add(website)
                    new_urls_count += 1
                    logger.debug(f"Added website lead: {title} ({website})")
            else:
                # Hot lead: No website
                key = f"{title.lower()}|{phone}"
                if key not in existing_no_website:
                    # Enrich lead with timestamp
                    lead_payload = {
                        "title": title,
                        "address": place.get("address"),
                        "category": place.get("category"),
                        "phoneNumber": phone if phone else None,
                        "rating": place.get("rating"),
                        "ratingCount": place.get("ratingCount"),
                        "position": place.get("position"),
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    }
                    await f_no_web.write(json.dumps(lead_payload, ensure_ascii=False) + "\n")
                    existing_no_website.add(key)
                    new_no_website_count += 1
                    logger.debug(f"Added hot lead (no website): {title}")

    logger.info(
        f"Processing completed. Added {new_urls_count} new website URLs to {urls_file} "
        f"and {new_no_website_count} new no-website leads to {no_website_file}."
    )


async def main_async() -> None:
    """Async main entrypoint."""
    args = parse_arguments()
    urls_file = Path(args.urls_file)
    no_website_file = Path(args.no_website_file)

    # Retrieve and check API key inside main_async
    api_key = get_api_key()
    if not api_key:
        logger.error("SERPER_API_KEY (or SERP_API_KEY) environment variable not found. Please set it in your environment or a .env file.")
        print("Error: SERPER_API_KEY is not set. Check logs/sourcer.log for details.", file=sys.stderr)
        sys.exit(1)

    try:
        places = await query_serper_places(args.query, api_key)
        if not places:
            logger.info("No places found for the search query.")
            return

        await process_leads(places, urls_file, no_website_file, api_key)
    except Exception as e:
        logger.error(f"Sourcing failed: {e}")
        sys.exit(1)


def main() -> None:
    """Sync wrapper for asyncio run."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Exiting.")
        sys.exit(0)


if __name__ == "__main__":
    main()
