#!/usr/bin/env python3
"""
enricher.py: B2B Contact Enricher using Apollo.io People Search API.
Loads service business leads from no-website leads and qualified website leads,
queries Apollo for key decision makers (Owner, Founder, CEO, etc.),
extracts contact info, and saves them to data/enriched_leads.jsonl.
"""

import argparse
import asyncio
import datetime
import json
import logging
import os
import sys
from pathlib import Path
from typing import Set, Dict, List, Any, Optional, Tuple
from urllib.parse import urlparse

import aiofiles
import httpx
from dotenv import load_dotenv


# Load environment variables from .env
load_dotenv()

# Setup logs directory
Path("logs").mkdir(exist_ok=True)

# Configure Logging to file logs/enricher.log and stdout
log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler("logs/enricher.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("lead-enricher")


def get_api_key() -> Optional[str]:
    """Retrieves Apollo.io API key from environment variables."""
    return os.getenv("APOLLO_API_KEY")


def parse_arguments() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="B2B Contact Enricher using Apollo.io People Search API."
    )
    parser.add_argument(
        "-n", "--no-website-file",
        type=str,
        default="data/no_website_leads.jsonl",
        help="Path to input JSONL file for leads WITHOUT websites (default: data/no_website_leads.jsonl)."
    )
    parser.add_argument(
        "-q", "--qualified-file",
        type=str,
        default="data/qualified_leads.jsonl",
        help="Path to input JSONL file for qualified leads WITH websites (default: data/qualified_leads.jsonl)."
    )
    parser.add_argument(
        "-o", "--output-file",
        type=str,
        default="data/enriched_leads.jsonl",
        help="Path to output JSONL file to save enriched leads (default: data/enriched_leads.jsonl)."
    )
    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        default=2,
        help="Maximum concurrent API enrichment sessions (default: 2)."
    )
    return parser.parse_args()


def get_domain(url: str) -> Optional[str]:
    """Parses and cleans the domain from a website URL."""
    if not url:
        return None
    try:
        netloc = urlparse(url).netloc
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return None


async def load_already_enriched(file_path: Path) -> Tuple[Set[str], Set[str]]:
    """Loads previously enriched business names and URLs to skip duplicates."""
    enriched_names: Set[str] = set()
    enriched_urls: Set[str] = set()
    
    if not file_path.exists():
        return enriched_names, enriched_urls

    try:
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            async for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    name = data.get("business_name")
                    url = data.get("url")
                    if name:
                        enriched_names.add(name.strip().lower())
                    if url:
                        enriched_urls.add(url.strip().lower())
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.error(f"Failed to read existing enriched leads file {file_path}: {e}")
        
    return enriched_names, enriched_urls


async def save_enriched_contact_async(
    file_path: Path,
    original_lead: Dict[str, Any],
    contact: Optional[Dict[str, Any]],
    error: Optional[str] = None
) -> None:
    """Appends an enriched lead record immediately to the output JSONL file for crash-resilience."""
    file_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "business_name": original_lead.get("business_name") or original_lead.get("title") or "Unknown",
        "url": original_lead.get("url"),
        "phone": original_lead.get("phone") or original_lead.get("phoneNumber"),
        "contact_first_name": contact.get("first_name") if contact else None,
        "contact_last_name": contact.get("last_name") if contact else None,
        "contact_title": contact.get("title") if contact else None,
        "contact_email": contact.get("email") if (contact and contact.get("email")) else original_lead.get("contact_email"),
        "contact_email_status": contact.get("email_status") if contact else None,
        "contact_linkedin": contact.get("linkedin_url") if contact else None,
        "lead_qualification_reason": original_lead.get("reason"),
        "enriched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "error": error
    }


    async with aiofiles.open(file_path, "a", encoding="utf-8") as f:
        await f.write(json.dumps(payload, ensure_ascii=False) + "\n")


async def query_apollo_with_backoff(
    client: httpx.AsyncClient,
    payload: Dict[str, Any],
    api_key: str
) -> Optional[Dict[str, Any]]:
    """Sends a POST request to Apollo mixed_people/search with exponential backoff on HTTP 429."""
    url = "https://api.apollo.io/v1/mixed_people/search"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key
    }
    # Pass api_key as a query param as well to ensure maximum compatibility
    params = {"api_key": api_key}
    
    max_retries = 5
    backoff_factor = 2.0
    initial_delay = 1.0

    for attempt in range(max_retries):
        try:
            response = await client.post(
                url,
                headers=headers,
                params=params,
                json=payload,
                timeout=25.0
            )

            # Respect rate limit 429
            if response.status_code == 429:
                delay = initial_delay * (backoff_factor ** attempt)
                logger.warning(
                    f"Apollo rate limit reached (HTTP 429). Retrying in {delay:.2f} seconds... "
                    f"(Attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(delay)
                continue

            response.raise_for_status()
            return response.json()

        except httpx.HTTPStatusError as e:
            # For status errors except 429, don't retry and log it
            if e.response.status_code != 429:
                logger.error(f"Apollo API HTTP Status Error: {e.response.status_code} - {e.response.text}")
                return None
        except Exception as e:
            logger.error(f"Network error on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt == max_retries - 1:
                return None
            delay = initial_delay * (backoff_factor ** attempt)
            await asyncio.sleep(delay)

    logger.error(f"Failed to query Apollo after {max_retries} attempts.")
    return None


async def enrich_single_lead(
    lead: Dict[str, Any],
    output_path: Path,
    api_key: str,
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient
) -> None:
    """Enriches contact information for a single lead using the Apollo API."""
    async with semaphore:
        business_name = lead.get("business_name") or lead.get("title") or "Unknown"
        url = lead.get("url")
        domain = get_domain(url)

        logger.info(f"Enriching: {business_name} (Domain: {domain})...")

        # 1. Build search filters payload
        payload = {
            "person_titles": ["Owner", "Founder", "CEO", "President", "Managing Partner"]
        }
        if domain:
            payload["q_organization_domains_list"] = [domain]
        else:
            payload["organization_names"] = [business_name]

        # Respect API rate limits with a small baseline delay between consecutive requests
        await asyncio.sleep(1.0)

        # 2. Query Apollo
        data = await query_apollo_with_backoff(client, payload, api_key)
        
        if not data:
            await save_enriched_contact_async(output_path, lead, None, error="Apollo query failed or returned no data.")
            return

        # 3. Harvest contacts
        people = data.get("people", [])
        if not people:
            logger.info(f"No matching executives found for: {business_name}")
            await save_enriched_contact_async(output_path, lead, None, error="No contacts found.")
            return

        # Take the first matching contact for simplicity (or we can save all; the user said 'the person's first_name...')
        # Let's save the first one that has an email, or default to the first one returned
        best_contact = None
        for p in people:
            if p.get("email"):
                best_contact = p
                break
        
        if not best_contact:
            best_contact = people[0]

        logger.info(
            f"[ENRICHED] {business_name} -> Contact: {best_contact.get('first_name')} {best_contact.get('last_name')} "
            f"({best_contact.get('title')}) - Email: {best_contact.get('email')}"
        )
        await save_enriched_contact_async(output_path, lead, best_contact)


async def main_async() -> None:
    """Async main routine."""
    args = parse_arguments()
    no_web_path = Path(args.no_website_file)
    qualified_path = Path(args.qualified_file)
    output_path = Path(args.output_file)

    # 1. Check API Key
    api_key = get_api_key()
    if not api_key:
        logger.error("APOLLO_API_KEY environment variable not found. Please set it in your environment or .env file.")
        print("Error: APOLLO_API_KEY is not set. Check logs/enricher.log for details.", file=sys.stderr)
        sys.exit(1)

    # 2. Check Input Files
    if not no_web_path.exists() and not qualified_path.exists():
        logger.error("No input files found. Run the crawler/analyzer first.")
        print(f"Error: Neither '{no_web_path}' nor '{qualified_path}' exists.", file=sys.stderr)
        sys.exit(1)

    # 3. Load already enriched leads to skip duplicates
    enriched_names, enriched_urls = await load_already_enriched(output_path)
    if enriched_names or enriched_urls:
        logger.info(f"Loaded {len(enriched_names)} enriched names and {len(enriched_urls)} enriched URLs for deduplication.")

    # 4. Load leads to process
    leads_to_process: List[Dict[str, Any]] = []

    # Parse no_website_leads
    if no_web_path.exists():
        try:
            async with aiofiles.open(no_web_path, "r", encoding="utf-8") as f:
                async for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        name = data.get("title")
                        if name:
                            # Check deduplication
                            if name.strip().lower() in enriched_names:
                                continue
                            leads_to_process.append(data)
                    except json.JSONDecodeError:
                        continue
            logger.info(f"Parsed leads from '{no_web_path}'. Total leads queue size: {len(leads_to_process)}")
        except Exception as e:
            logger.error(f"Error reading no-website leads file {no_web_path}: {e}")

    # Parse qualified_leads
    if qualified_path.exists():
        qualified_count = 0
        try:
            async with aiofiles.open(qualified_path, "r", encoding="utf-8") as f:
                async for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        is_qualified = data.get("is_qualified", False)
                        name = data.get("business_name")
                        url = data.get("url")

                        # Only process qualified leads
                        if not is_qualified:
                            continue

                        # Check deduplication
                        name_match = name.strip().lower() in enriched_names if name else False
                        url_match = url.strip().lower() in enriched_urls if url else False

                        if name_match or url_match:
                            continue

                        leads_to_process.append(data)
                        qualified_count += 1
                    except json.JSONDecodeError:
                        continue
            logger.info(f"Parsed {qualified_count} qualified leads from '{qualified_path}'. Total leads queue size: {len(leads_to_process)}")
        except Exception as e:
            logger.error(f"Error reading qualified leads file {qualified_path}: {e}")

    if not leads_to_process:
        logger.info("No new leads to enrich. Exiting.")
        return

    logger.info(f"Starting contact enrichment for {len(leads_to_process)} leads with concurrency={args.concurrency}...")

    # 5. Process concurrently using Semaphore
    semaphore = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient() as client:
        tasks = [
            enrich_single_lead(lead, output_path, api_key, semaphore, client)
            for lead in leads_to_process
        ]
        await asyncio.gather(*tasks)

    logger.info("Enrichment completed successfully.")


def main() -> None:
    """Sync wrapper for main."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Exiting.")
        sys.exit(0)


if __name__ == "__main__":
    main()
