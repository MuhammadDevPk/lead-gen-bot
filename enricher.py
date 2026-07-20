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
import re
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


def get_serper_api_key() -> Optional[str]:
    """Retrieves Serper API key from environment variables."""
    return os.getenv("SERPER_API_KEY")


def get_query_location(lead: Dict[str, Any]) -> str:
    """Extracts city/region location from lead address for search queries."""
    if lead.get("city"):
        return lead.get("city")
    
    address = lead.get("address") or lead.get("formatted_address")
    if address:
        parts = address.split(",")
        if len(parts) > 1:
            city_part = parts[1].strip()
            return city_part
    return ""


async def search_serper_google_fallback(
    client: httpx.AsyncClient,
    query: str,
    api_key: str
) -> List[str]:
    """Queries Serper.dev Google Search API and extracts emails from snippet results."""
    if not api_key:
        return []
        
    url = "https://google.serper.dev/search"
    payload = {"q": query}
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    }
    
    try:
        logger.info(f"[SERPER FALLBACK] Querying Google Search: '{query}'...")
        response = await client.post(url, headers=headers, json=payload, timeout=20.0)
        response.raise_for_status()
        data = response.json()
        
        organic = data.get("organic", [])
        snippets = []
        for item in organic:
            if item.get("snippet"):
                snippets.append(item["snippet"])
            if item.get("title"):
                snippets.append(item["title"])
                
        # Parse emails using regex
        emails = []
        email_pattern = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
        for text in snippets:
            found = email_pattern.findall(text)
            if found:
                emails.extend(found)
                
        # Filter and clean
        valid_emails = []
        invalid_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.css', '.js', '.pdf', '.ico')
        ignored_domains = ('sentry.io', 'wix.com', 'domain.com', 'example.com', 'yourdomain.com', 'bootstrap.com', 'jquery.com', 'wixpress.com')
        for email in emails:
            email = email.strip().lower()
            if "@" not in email:
                continue
            if email.endswith(invalid_extensions):
                continue
            if any(ext in email for ext in invalid_extensions):
                continue
            domain = email.split("@")[-1]
            if domain in ignored_domains:
                continue
            if any(dom in domain for dom in ignored_domains):
                continue
            valid_emails.append(email)
            
        return sorted(list(set(valid_emails)))
        
    except Exception as e:
        logger.error(f"[SERPER FALLBACK ERROR] Serper Search request failed: {e}")
        return []


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


# Helper function to resolve cascading email
def resolve_cascading_email(contact: Optional[Dict[str, Any]], original_lead: Dict[str, Any]) -> Optional[str]:
    # 1. Apollo Decision Maker Email (if found via API search)
    if contact and contact.get("email"):
        return contact.get("email")

    # Gather all website scraped emails
    scraped_emails = []
    # Try contact_emails list first
    if "contact_emails" in original_lead and isinstance(original_lead["contact_emails"], list):
        scraped_emails.extend(original_lead["contact_emails"])
    # Try scraped_emails (from crawler payload)
    if "scraped_emails" in original_lead and isinstance(original_lead["scraped_emails"], list):
        scraped_emails.extend(original_lead["scraped_emails"])
    # Add primary contact_email
    if original_lead.get("contact_email"):
        scraped_emails.append(original_lead["contact_email"])

    # Deduplicate while preserving order
    seen = set()
    unique_scraped = []
    for email in scraped_emails:
        if email and email.strip().lower() not in seen:
            seen.add(email.strip().lower())
            unique_scraped.append(email.strip())

    # Prefixes of generic emails
    generic_prefixes = (
        "info@", "contact@", "sales@", "hello@", "support@", "office@",
        "admin@", "mail@", "billing@", "jobs@", "careers@", "team@",
        "service@", "webmaster@", "help@", "press@", "marketing@"
    )

    # 2. Website Scraped Email (personal/specific)
    # Filter unique_scraped to find non-generic emails first
    for email in unique_scraped:
        email_lower = email.lower()
        if not any(email_lower.startswith(prefix) for prefix in generic_prefixes):
            return email

    # 3. Generic Domain Contact (if available)
    # If no specific email is found, return the first generic email
    for email in unique_scraped:
        email_lower = email.lower()
        if any(email_lower.startswith(prefix) for prefix in generic_prefixes):
            return email

    # Fallback to whatever was originally in contact_email
    return original_lead.get("contact_email")


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
        "contact_email": resolve_cascading_email(contact, original_lead),
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
        
        people = []
        if data:
            people = data.get("people", [])
            
        best_contact = None
        if people:
            for p in people:
                if p.get("email"):
                    best_contact = p
                    break
            if not best_contact:
                best_contact = people[0]

        # 3. Check if Apollo found a contact with a valid email
        if best_contact and best_contact.get("email"):
            logger.info(
                f"[ENRICHED] {business_name} -> Contact: {best_contact.get('first_name')} {best_contact.get('last_name')} "
                f"({best_contact.get('title')}) - Email: {best_contact.get('email')}"
            )
            await save_enriched_contact_async(output_path, lead, best_contact)
            return

        # OTHERWISE: Apollo 403, error, or no email found. Execute Serper Google Search Fallback!
        logger.warning(f"Apollo enrichment failed to locate contact email for: {business_name}. Launching Serper Fallback...")
        
        serper_key = get_serper_api_key()
        fallback_emails = []
        if serper_key:
            city = get_query_location(lead)
            search_query = f"{business_name} {city} email OR contact".strip()
            fallback_emails = await search_serper_google_fallback(client, search_query, serper_key)
            
        if fallback_emails:
            serper_email = fallback_emails[0]
            mock_contact = {
                "first_name": "Decision Maker",
                "last_name": "",
                "title": "Contact",
                "email": serper_email,
                "email_status": "scraped_via_serper"
            }
            logger.info(f"[SERPER FALLBACK SUCCESS] Found email for {business_name}: {serper_email}")
            await save_enriched_contact_async(output_path, lead, mock_contact)
        else:
            # If both fail, save_enriched_contact_async with None contact will auto-cascade to Crawl4AI crawled emails
            logger.info(f"[NO CONTACT FOUND] Apollo & Serper failed for {business_name}. Falling back to Crawl4AI website scraped emails.")
            await save_enriched_contact_async(output_path, lead, None, error="Apollo & Serper fallback found no contacts.")


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
