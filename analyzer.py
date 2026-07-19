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
from typing import Set, Optional

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

# Initialize OpenAI Client (reads OPENAI_API_KEY automatically)
openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    logger.error("OPENAI_API_KEY environment variable not found. Please set it in your environment or a .env file.")
    print("Error: OPENAI_API_KEY is not set. Check logs/analyzer.log for details.", file=sys.stderr)
    sys.exit(1)

client = AsyncOpenAI(api_key=openai_api_key)


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
        "automation_score": qualification.automation_score if qualification else 0,
        "error": error
    }

    async with aiofiles.open(file_path, "a", encoding="utf-8") as f:
        await f.write(json.dumps(payload, ensure_ascii=False) + "\n")


async def analyze_single_lead(
    url: str,
    markdown_content: str,
    output_file: Path,
    semaphore: asyncio.Semaphore
) -> None:
    """Qualifies a single lead by sending its markdown content to OpenAI."""
    async with semaphore:
        logger.info(f"Analyzing: {url}...")
        try:
            # Call OpenAI Chat Completions API with Pydantic parsing
            completion = await client.beta.chat.completions.parse(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a B2B Lead Qualifier. Your job is to analyze the text content of a business's website "
                            "(provided in markdown format) and qualify them for web development, booking systems, or booking automation services.\n\n"
                            "Criteria for Qualification:\n"
                            "1. The business must be service-based (e.g., local home services, medical, beauty, professional services, local instruction/coaching). "
                            "Product-only e-commerce shops, SaaS, news portals, and large enterprises are NOT qualified.\n"
                            "2. The business must meet at least one of these conditions:\n"
                            "   - Lacks a modern interactive booking/scheduling widget (like Calendly, Acuity, or custom booking forms). If they only have a "
                            "basic static form, a phone number, or an email link to schedule, they are qualified.\n"
                            "   - Has a broken, static, or outdated website.\n"
                            "   - Lacks a website (in this context, if the content is extremely minimal or broken, or shows it lacks dynamic features).\n\n"
                            "Determine if they are qualified, extract their business name and contact email, and compute an automation score."
                        )
                    },
                    {
                        "role": "user",
                        "content": f"URL: {url}\n\nWebsite Content (Markdown):\n{markdown_content[:25000]}"
                    }
                ],
                response_format=LeadQualification,
            )

            qualification = completion.choices[0].message.parsed
            if qualification:
                logger.info(f"[QUALIFICATION RESULT] {url} -> Qualified: {qualification.is_qualified} | Score: {qualification.automation_score}")
                await save_analyzed_lead_async(output_file, url, qualification)
            else:
                logger.error(f"Failed to parse qualification result for {url} (Empty parsed response).")
                await save_analyzed_lead_async(output_file, url, None, error="Structured output parsing returned None.")

        except Exception as e:
            logger.error(f"Error during API call for {url}: {e}", exc_info=True)
            await save_analyzed_lead_async(output_file, url, None, error=str(e))


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

                    leads_to_process.append((url, raw_markdown))
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
        analyze_single_lead(url, markdown, output_path, semaphore)
        for url, markdown in leads_to_process
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
