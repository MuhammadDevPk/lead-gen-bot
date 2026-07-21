#!/usr/bin/env python3
"""
outreach.py: Automated Lead Injector for Instantly.ai.
Asynchronously reads data/enriched_leads.jsonl, filters for valid emails,
deduplicates using data/outreach_history.jsonl, generates dynamic personalization,
and injects up to 30 leads per run to active Instantly campaign with retry backoff.
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

import aiofiles
import httpx
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Setup logs directory
Path("logs").mkdir(exist_ok=True)

# Configure Logging to file logs/outreach.log and stdout
log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler("logs/outreach.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("lead-outreach")

# Basic email regex validation pattern
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def sanitize_first_name(first_name: Optional[str]) -> str:
    """Cleans the first name; returns 'there' if missing, empty, 'Decision Maker', or 'Unknown'."""
    if not first_name:
        return "there"
    name_strip = first_name.strip()
    if name_strip.lower() in ("decision maker", "unknown", ""):
        return "there"
    return name_strip


def get_credentials() -> Tuple[Optional[str], Optional[str]]:
    """Retrieves Instantly API credentials from environment variables."""
    api_key = os.getenv("INSTANTLY_API_KEY")
    campaign_id = os.getenv("INSTANTLY_CAMPAIGN_ID")
    return api_key, campaign_id


def parse_arguments() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="Automated Lead Injector for Instantly.ai Campaign."
    )
    parser.add_argument(
        "-i", "--input-file",
        type=str,
        default="data/enriched_leads.jsonl",
        help="Path to input JSONL file containing enriched leads (default: data/enriched_leads.jsonl)."
    )
    parser.add_argument(
        "-t", "--tracker-file",
        type=str,
        default="data/outreach_history.jsonl",
        help="Path to outreach tracker JSONL file (default: data/outreach_history.jsonl)."
    )
    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=30,
        help="Maximum number of leads to successfully inject in this run (default: 30)."
    )
    return parser.parse_args()


async def load_outreach_history(file_path: Path) -> Set[str]:
    """Loads contacted email addresses from the history tracker file to avoid duplicates."""
    contacted_emails: Set[str] = set()
    if not file_path.exists():
        return contacted_emails

    try:
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            async for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    email = data.get("email")
                    if email:
                        contacted_emails.add(email.strip().lower())
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.error(f"Failed to read outreach history tracker {file_path}: {e}")
        
    return contacted_emails


async def save_outreach_history_async(file_path: Path, email: str) -> None:
    """Appends contacted email address with timestamp immediately for crash-resilience."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "email": email,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    async with aiofiles.open(file_path, "a", encoding="utf-8") as f:
        await f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def is_valid_email(email: Optional[str]) -> bool:
    """Validates email format using basic regex."""
    if not email:
        return False
    return bool(EMAIL_REGEX.match(email.strip()))


def generate_personalization(lead: Dict[str, Any]) -> str:
    """Generates dynamic personalization intro lines based on lead properties."""
    # 1. Prioritize AI-generated personalization hook if present
    hook = lead.get("personalization_hook")
    if hook:
        return hook.strip()

    # Fallback to structured copy writing lines
    url = lead.get("url")
    reason = lead.get("lead_qualification_reason") or lead.get("reason")

    if not url:
        # Hot lead without website
        return (
            "I noticed your business doesn't have an active website online, which means you're missing out "
            "on local search clients searching for services in your area. A simple, modern landing page "
            "with booking automation can capture these leads and boost your revenue."
        )
    else:
        # Lead has a website but failed qualification check
        if not reason:
            reason = "lacks a modern automated booking system"
        clean_reason = reason.strip().rstrip(".")
        if clean_reason.lower().startswith(("the business", "this business")):
            return (
                f"I took a look at your website. {clean_reason} "
                "Implementing an automated booking system or modern layout could capture these lost visitors and "
                "significantly increase your booking rate."
            )
        else:
            return (
                f"I took a look at your website and noticed it looks like it {clean_reason}. "
                "Implementing an automated booking system or modern layout could capture these lost visitors and "
                "significantly increase your booking rate."
            )


async def inject_lead_with_backoff(
    client: httpx.AsyncClient,
    campaign_id: str,
    api_key: str,
    lead_payload: Dict[str, Any]
) -> bool:
    """Sends a single lead to Instantly.ai campaign endpoint with exponential 429 backoff."""
    url = "https://api.instantly.ai/api/v2/leads/add"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "campaign_id": campaign_id,
        "skip_if_in_workspace": True,
        "leads": [lead_payload]
    }
    
    max_retries = 5
    backoff_factor = 2.0
    initial_delay = 1.0

    for attempt in range(max_retries):
        try:
            response = await client.post(
                url,
                headers=headers,
                json=payload,
                timeout=25.0
            )

            # Respect rate limit 429
            if response.status_code == 429:
                delay = initial_delay * (backoff_factor ** attempt)
                logger.warning(
                    f"Instantly rate limit reached (HTTP 429). Retrying in {delay:.2f} seconds... "
                    f"(Attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(delay)
                continue

            response.raise_for_status()
            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code != 429:
                logger.error(f"Instantly API HTTP Status Error: {e.response.status_code} - {e.response.text}")
                return False
        except Exception as e:
            logger.error(f"Network error on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt == max_retries - 1:
                return False
            delay = initial_delay * (backoff_factor ** attempt)
            await asyncio.sleep(delay)

    logger.error(f"Failed to inject lead to Instantly after {max_retries} attempts.")
    return False


async def migrate_existing_leads_files() -> None:
    """Updates all existing enriched leads JSONL files with cleaned names and personalization hooks."""
    data_dir = Path("data")
    if not data_dir.exists():
        return
        
    files_to_update = list(data_dir.glob("*enriched_leads.jsonl"))
    
    for file_path in files_to_update:
        logger.info(f"Migrating and updating names/hooks in existing file: {file_path}")
        temp_path = file_path.with_suffix(".tmp")
        
        try:
            records = []
            async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                async for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        lead = json.loads(line)
                        fname = lead.get("contact_first_name")
                        lead["contact_first_name"] = sanitize_first_name(fname)
                        
                        hook = lead.get("personalization_hook")
                        if hook and ("noticed it looks like it The business" in hook or "noticed it looks like it This business" in hook):
                            hook = None
                            
                        if not hook:
                            url = lead.get("url")
                            reason = lead.get("lead_qualification_reason") or lead.get("reason")
                            if not url:
                                lead["personalization_hook"] = (
                                    "I noticed your business doesn't have an active website online, which means you're missing out "
                                    "on local search clients searching for services in your area. A simple, modern landing page "
                                    "with booking automation can capture these leads and boost your revenue."
                                )
                            else:
                                if not reason:
                                    reason = "lacks a modern automated booking system"
                                clean_reason = reason.strip().rstrip(".")
                                if clean_reason.lower().startswith(("the business", "this business")):
                                    lead["personalization_hook"] = (
                                        f"I took a look at your website. {clean_reason} "
                                        "Implementing an automated booking system or modern layout could capture these lost visitors and "
                                        "significantly increase your booking rate."
                                    )
                                else:
                                    lead["personalization_hook"] = (
                                        f"I took a look at your website and noticed it looks like it {clean_reason}. "
                                        "Implementing an automated booking system or modern layout could capture these lost visitors and "
                                        "significantly increase your booking rate."
                                    )
                        records.append(lead)
                    except json.JSONDecodeError:
                        continue
            
            async with aiofiles.open(temp_path, "w", encoding="utf-8") as f:
                for record in records:
                    await f.write(json.dumps(record, ensure_ascii=False) + "\n")
            
            os.replace(temp_path, file_path)
            logger.info(f"Successfully migrated {len(records)} records in {file_path}.")
            
        except Exception as e:
            logger.error(f"Failed to migrate file {file_path}: {e}")
            if temp_path.exists():
                try:
                    os.remove(temp_path)
                except:
                    pass


async def main_async() -> None:
    """Async main routine."""
    # Run the existing files migration first to sanitize all records
    await migrate_existing_leads_files()
    args = parse_arguments()
    input_path = Path(args.input_file)
    tracker_path = Path(args.tracker_file)

    # 1. Check Credentials
    api_key, campaign_id = get_credentials()
    if not api_key or not campaign_id:
        logger.error(
            "INSTANTLY_API_KEY or INSTANTLY_CAMPAIGN_ID environment variable not found. "
            "Please configure them in your environment or a .env file."
        )
        print("Error: Instantly.ai credentials are not set. Check logs/outreach.log for details.", file=sys.stderr)
        sys.exit(1)

    # 2. Check Input File
    if not input_path.exists():
        logger.error(f"Enriched leads file not found: {input_path}")
        print(f"Error: Input file '{input_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    # 3. Load contacted history
    contacted_emails = await load_outreach_history(tracker_path)
    if contacted_emails:
        logger.info(f"Loaded {len(contacted_emails)} already contacted emails to skip duplicates.")

    # 4. Load and validate leads from input file
    leads_to_process: List[Dict[str, Any]] = []
    try:
        async with aiofiles.open(input_path, "r", encoding="utf-8") as f:
            async for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    lead = json.loads(line)
                    email = lead.get("contact_email")

                    # Skip empty/invalid emails
                    if not is_valid_email(email):
                        continue

                    email_clean = email.strip().lower()

                    # Skip previously contacted leads
                    if email_clean in contacted_emails:
                        continue

                    leads_to_process.append(lead)
                except json.JSONDecodeError:
                    continue
        logger.info(f"Parsed {len(leads_to_process)} new unique leads ready for outreach.")
    except Exception as e:
        logger.exception(f"Failed to read input leads file {input_path}: {e}")
        sys.exit(1)

    if not leads_to_process:
        logger.info("No new leads to contact. Exiting.")
        return

    # Respect the safety cap
    run_limit = min(args.limit, len(leads_to_process))
    leads_to_run = leads_to_process[:run_limit]
    logger.info(f"Starting outreach injection for {len(leads_to_run)} leads (Safety run limit: {args.limit})...")

    # 5. Process sequentially with httpx to track successful counts & manage backoff
    success_count = 0
    async with httpx.AsyncClient() as client:
        for idx, lead in enumerate(leads_to_run):
            email = lead["contact_email"]
            first_name = sanitize_first_name(lead.get("contact_first_name"))
            last_name = lead.get("contact_last_name") or ""
            company_name = lead.get("business_name") or "your business"
            personalization = generate_personalization(lead)

            lead_payload = {
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "company_name": company_name,
                "personalization": personalization
            }

            logger.info(f"({idx + 1}/{len(leads_to_run)}) Injecting lead: {email}...")

            # Add a small delay between lead injections to avoid hitting limits
            await asyncio.sleep(1.0)

            success = await inject_lead_with_backoff(client, campaign_id, api_key, lead_payload)
            if success:
                success_count += 1
                logger.info(f"[OUTREACH SUCCESS] Injected lead {email} successfully.")
                # Save outreach history immediately (crash-resilient)
                await save_outreach_history_async(tracker_path, email)
            else:
                logger.error(f"[OUTREACH ERROR] Failed to inject lead: {email}.")

    logger.info(f"Outreach processing finished. Successfully injected {success_count}/{len(leads_to_run)} leads.")


def main() -> None:
    """Sync wrapper for main."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Exiting.")
        sys.exit(0)


if __name__ == "__main__":
    main()
