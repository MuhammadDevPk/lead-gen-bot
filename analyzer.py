#!/usr/bin/env python3
"""
analyzer.py: Processes raw markdown scraped by crawler.py, sends it to OpenAI's
gpt-4o-mini with structured outputs to qualify B2B leads, and outputs the results in JSONL format.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Set, Optional, List, Dict, Any, Tuple
import re

import aiofiles
from dotenv import load_dotenv
from openai import AsyncOpenAI
from pydantic import BaseModel, Field

# Load environment variables from .env
load_dotenv()

# Ensure logs directory exists
Path("logs").mkdir(exist_ok=True)

# Configure Logging to file logs/analyzer.log and stdout
log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler("logs/analyzer.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("lead-analyzer")

# Set up active providers list dynamically
providers: List[Dict[str, Any]] = []

# Prioritize env key if set and valid
env_openai_key = os.getenv("OPENAI_API_KEY")
if env_openai_key and not env_openai_key.startswith("your_"):
    providers.append({
        "name": "OpenAI - Env Key",
        "base_url": None,
        "api_key": env_openai_key,
        "model": "gpt-4o-mini"
    })

# Check if data/keys.json exists
keys_file = Path("data/keys.json")
if keys_file.exists():
    try:
        with open(keys_file, "r", encoding="utf-8") as f:
            custom_list = json.load(f)
            if isinstance(custom_list, list):
                providers.extend(custom_list)
                logger.info(f"Loaded {len(custom_list)} API providers from {keys_file}")
    except Exception as e:
        logger.warning(f"Could not read keys file {keys_file}: {e}")

if not providers:
    logger.error("No API providers found. Please set OPENAI_API_KEY in .env or populate data/keys.json (which is ignored by Git).")
    print("Error: No API providers found. Check logs/analyzer.log for details.", file=sys.stderr)
    sys.exit(1)

# Key Rotation state
current_provider_index = 0
provider_lock = asyncio.Lock()


async def get_current_client_and_provider() -> Tuple[AsyncOpenAI, Dict[str, Any]]:
    """Retrieves the currently selected AsyncOpenAI client and provider config."""
    global current_provider_index
    async with provider_lock:
        provider = providers[current_provider_index]
        client = AsyncOpenAI(
            api_key=provider["api_key"],
            base_url=provider["base_url"]
        )
        return client, provider


async def rotate_provider() -> None:
    """Rotates the global provider index to the next configured provider."""
    global current_provider_index
    async with provider_lock:
        current_provider_index = (current_provider_index + 1) % len(providers)
        logger.info(f"Swapped to next API provider: {providers[current_provider_index]['name']}")


def extract_json(text: str) -> str:
    """Helper to extract JSON block from text if wrapped in markdown blocks."""
    text_clean = text.strip()
    
    # Try finding json code block first
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text_clean, re.DOTALL)
    if match:
        return match.group(1)
        
    # Fallback to finding first '{' and last '}'
    first_brace = text_clean.find("{")
    last_brace = text_clean.rfind("}")
    if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
        return text_clean[first_brace:last_brace + 1]
        
    return text_clean


class LeadQualification(BaseModel):
    """Pydantic model representing the required structured output schema for lead qualification."""
    is_qualified: bool = Field(
        ...,
        description="Whether the business is service-based AND has a static/broken website or lacks modern booking/automation forms."
    )
    reason: str = Field(
        ...,
        description="Explanation of why this business is or is not qualified, citing specific details from the website content."
    )
    business_name: str = Field(
        ...,
        description="The name of the business or organization."
    )
    contact_email: Optional[str] = Field(
        None,
        description="The contact email address found in the website content, or null/empty if none was found."
    )
    contact_emails: List[str] = Field(
        default_factory=list,
        description="A list of all unique contact email addresses found in the website content."
    )
    automation_score: int = Field(
        ...,
        description="An integer from 1 to 100 rating how automated their customer acquisition/booking flow is (1 = fully manual/static, 100 = highly automated with widgets/schedulers)."
    )


def parse_arguments() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="B2B Lead Qualifier using OpenAI gpt-4o-mini structured outputs."
    )
    parser.add_argument(
        "-i", "--input",
        type=str,
        default="data/leads.jsonl",
        help="Path to the input JSONL file containing scraped website content (default: data/leads.jsonl)."
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="data/qualified_leads.jsonl",
        help="Path to the output JSONL file to save qualified leads (default: data/qualified_leads.jsonl)."
    )
    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        default=3,
        help="Maximum concurrent OpenAI API requests (default: 3)."
    )
    return parser.parse_args()


async def load_already_analyzed_async(file_path: Path) -> Set[str]:
    """Loads URLs that have already been analyzed to prevent duplicate API requests."""
    if not file_path.exists():
        return set()

    analyzed_urls: Set[str] = set()
    try:
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            async for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    url = data.get("url")
                    if url:
                        analyzed_urls.add(url)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.error(f"Failed to load already analyzed URLs from {file_path}: {e}")
    return analyzed_urls


async def save_analyzed_lead_async(file_path: Path, url: str, qualification: Optional[LeadQualification], error: Optional[str] = None) -> None:
    """Appends an analyzed lead result immediately to the output JSONL file for crash-resilience."""
    file_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "url": url,
        "is_qualified": qualification.is_qualified if qualification else False,
        "reason": qualification.reason if qualification else f"Error: {error}",
        "business_name": qualification.business_name if qualification else "Unknown",
        "contact_email": qualification.contact_email if qualification else None,
        "contact_emails": qualification.contact_emails if qualification else [],
        "automation_score": qualification.automation_score if qualification else 0,
        "error": error
    }

    async with aiofiles.open(file_path, "a", encoding="utf-8") as f:
        await f.write(json.dumps(payload, ensure_ascii=False) + "\n")


async def analyze_single_lead(
    url: str,
    markdown_content: str,
    scraped_emails: List[str],
    output_file: Path,
    semaphore: asyncio.Semaphore
) -> None:
    """Qualifies a single lead by sending its markdown content to LLM with API key rotation."""
    async with semaphore:
        logger.info(f"Analyzing: {url}...")
        
        max_attempts = len(providers) * 2
        for attempt in range(max_attempts):
            client, provider = await get_current_client_and_provider()
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: Using provider '{provider['name']}' (Model: {provider['model']})")
            
            try:
                # Construct messages payload for JSON output compatibility
                messages = [
                    {
                        "role": "system",
                        "content": (
                            "You are a B2B Lead Qualifier. Your job is to analyze the text content of a business's website "
                            "(provided in markdown format) and qualify them for web development, booking systems, or booking automation services.\n\n"
                            "Criteria for Qualification:\n"
                            "1. The business must be service-based (e.g., local home services, medical, beauty, local coaching). "
                            "Product-only e-commerce shops, SaaS, news portals, and large enterprises are NOT qualified.\n"
                            "2. The business must meet at least one of these conditions:\n"
                            "   - Lacks a modern interactive booking/scheduling widget (like Calendly, Acuity, or custom booking forms). If they only have a "
                            "basic static form, a phone number, or an email link to schedule, they are qualified.\n"
                            "   - Has a broken, static, or outdated website.\n"
                            "   - Lacks a website (in this context, if the content is extremely minimal or broken, or shows it lacks dynamic features).\n\n"
                            "Email Harvesting Rules:\n"
                            "- Extract all valid email addresses found in the website content. Place them in the 'contact_emails' list.\n"
                            "- Set the primary 'contact_email' to the best direct contact email found. Categorize any valid business email (e.g., info@..., sales@..., hello@..., or staff members) as a valid contact target if a specific owner email is not explicitly present.\n\n"
                            "Return a JSON object conforming exactly to this schema:\n"
                            "{\n"
                            "  \"is_qualified\": boolean,\n"
                            "  \"reason\": \"Detailed explanation of why this business is or is not qualified, citing specific details from the website content.\",\n"
                            "  \"business_name\": \"The name of the business or organization.\",\n"
                            "  \"contact_email\": \"The best contact email address found in the website content, or null if none was found.\",\n"
                            "  \"contact_emails\": [\"list\", \"of\", \"all\", \"unique\", \"emails\", \"found\"],\n"
                            "  \"automation_score\": integer (1 to 100 rating how automated their booking flow is, where 1 = fully manual/static and 100 = highly automated)\n"
                            "}\n\n"
                            "CRITICAL: You must output ONLY the raw JSON object and nothing else."
                        )
                    },
                    {
                        "role": "user",
                        "content": f"URL: {url}\n\nWebsite Content (Markdown):\n{markdown_content[:25000]}"
                    }
                ]
                
                kwargs = {
                    "model": provider["model"],
                    "messages": messages,
                    "temperature": 0.2,
                    "response_format": {"type": "json_object"}
                }
                
                completion = await client.chat.completions.create(
                    timeout=30.0,
                    **kwargs
                )
                
                content = completion.choices[0].message.content
                if not content:
                    raise ValueError("Received empty content response from API.")
                
                # Extract and parse JSON
                json_str = extract_json(content)
                qualification_data = json.loads(json_str)
                
                # Parse and validate with Pydantic
                qualification = LeadQualification.model_validate(qualification_data)
                
                # Combine the emails found by the direct Regex scanner with any emails extracted by the LLM
                all_emails = set(scraped_emails)
                if qualification.contact_emails:
                    all_emails.update(qualification.contact_emails)
                if qualification.contact_email:
                    all_emails.add(qualification.contact_email)
                
                # Clean, filter, and set back
                from crawler import clean_and_filter_emails
                final_emails = clean_and_filter_emails(all_emails)
                qualification.contact_emails = final_emails
                qualification.contact_email = final_emails[0] if final_emails else None
                
                logger.info(f"[QUALIFICATION RESULT] {url} -> Qualified: {qualification.is_qualified} | Score: {qualification.automation_score} (via {provider['name']})")
                await save_analyzed_lead_async(output_file, url, qualification)
                return  # Success, exit function
                
            except Exception as e:
                logger.warning(f"Provider '{provider['name']}' failed for {url} with error: {e}. Rotating provider...")
                await rotate_provider()
                # Loop will retry with next provider
                
        # If all attempts fail
        error_msg = f"All {len(providers)} providers failed to analyze this lead."
        logger.error(f"[QUALIFICATION FAILED] {url} -> {error_msg}")
        await save_analyzed_lead_async(output_file, url, None, error=error_msg)


async def main_async() -> None:
    """Async main routine."""
    args = parse_arguments()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        print(f"Error: Scraped leads file '{input_path}' does not exist. Please run the crawler first.", file=sys.stderr)
        sys.exit(1)

    # 1. Load already analyzed URLs
    analyzed_urls = await load_already_analyzed_async(output_path)
    if analyzed_urls:
        logger.info(f"Loaded {len(analyzed_urls)} already analyzed URLs from {output_path}.")

    # 2. Read leads from leads.jsonl
    leads_to_process = []
    try:
        async with aiofiles.open(input_path, "r", encoding="utf-8") as f:
            async for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    url = data.get("url")
                    success = data.get("success", False)
                    markdown_data = data.get("markdown") or {}
                    raw_markdown = markdown_data.get("raw_markdown")

                    if not url:
                        continue

                    if url in analyzed_urls:
                        # Skip if already analyzed
                        continue

                    if not success or not raw_markdown:
                        logger.warning(f"Skipping analysis for {url} because crawl was unsuccessful or markdown is missing.")
                        # Save failed crawl as not qualified
                        await save_analyzed_lead_async(output_path, url, None, error="Unsuccessful crawl or missing markdown.")
                        continue

                    scraped_emails = data.get("scraped_emails") or []
                    leads_to_process.append((url, raw_markdown, scraped_emails))
                except json.JSONDecodeError:
                    logger.warning("Malformed JSON line skipped in input file.")
                    continue
    except Exception as e:
        logger.exception(f"Failed to read input leads file {input_path}: {e}")
        sys.exit(1)

    if not leads_to_process:
        logger.info("No new leads to analyze. Exiting.")
        return

    logger.info(f"Starting analysis of {len(leads_to_process)} leads with concurrency level {args.concurrency}...")

    # 3. Process concurrently using Semaphore
    semaphore = asyncio.Semaphore(args.concurrency)
    tasks = [
        analyze_single_lead(url, markdown, scraped_emails, output_path, semaphore)
        for url, markdown, scraped_emails in leads_to_process
    ]
    
    await asyncio.gather(*tasks)
    logger.info("Analysis completed successfully.")


def main() -> None:
    """Main entry wrapper."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Exiting.")
        sys.exit(0)


if __name__ == "__main__":
    main()
