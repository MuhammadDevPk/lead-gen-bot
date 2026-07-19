#!/usr/bin/env python3
"""
lead-gen-crawler: An async web crawler built with Crawl4AI and asyncio.
Takes URLs from command line, files, or searches directories, crawls them,
and saves the results into a crash-resilient JSONL format.
"""

import argparse
import asyncio
import datetime
import json
import logging
import re
import sys
from pathlib import Path
from typing import List, Set, Optional

import aiofiles
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, CrawlResult
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher, RateLimiter

# Configure Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("lead-gen-crawler")


def parse_arguments() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="Async web crawler using Crawl4AI to harvest fresh markdown and save to JSONL."
    )
    parser.add_argument(
        "urls",
        nargs="*",
        help="List of URLs to crawl directly from command line."
    )
    parser.add_argument(
        "-f", "--file",
        type=str,
        help="Path to a text file containing URLs (one per line)."
    )
    parser.add_argument(
        "-d", "--dir",
        type=str,
        help="Path to a directory to scan for URLs in text/JSON/CSV files."
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="data/leads.jsonl",
        help="Path to the output JSONL file (default: data/leads.jsonl)."
    )
    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        default=5,
        help="Maximum concurrent crawl sessions (default: 5)."
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry crawling URLs that previously failed (i.e. success=False in the output file)."
    )
    return parser.parse_args()


def extract_urls_from_file(file_path: str) -> List[str]:
    """Extracts URLs from a file. Handles one-url-per-line and general text searching."""
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    urls: List[str] = []
    url_pattern = re.compile(r'https?://[^\s,\"\'>]+')

    try:
        content = path.read_text(encoding='utf-8', errors='ignore')
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        for line in lines:
            if line.startswith('#'):
                continue
            if line.startswith('http://') or line.startswith('https://'):
                urls.append(line)
            else:
                found = url_pattern.findall(line)
                urls.extend(found)
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        raise

    # Clean and deduplicate while maintaining order
    seen: Set[str] = set()
    unique_urls: List[str] = []
    for url in urls:
        cleaned_url = url.rstrip(').,;!?')
        if cleaned_url not in seen:
            seen.add(cleaned_url)
            unique_urls.append(cleaned_url)

    return unique_urls


def extract_urls_from_directory(dir_path: str) -> List[str]:
    """Recursively scans a directory for files containing URLs and extracts them."""
    path = Path(dir_path)
    if not path.is_dir():
        raise ValueError(f"Provided path '{dir_path}' is not a directory.")

    urls: List[str] = []
    url_pattern = re.compile(r'https?://[^\s,\"\'>]+')
    supported_extensions = {'.txt', '.csv', '.md', '.json', '.jsonl', '.html', '.htm', '.xml'}

    for file_path in path.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in supported_extensions:
            try:
                content = file_path.read_text(encoding='utf-8', errors='ignore')
                found = url_pattern.findall(content)
                urls.extend(found)
            except Exception as e:
                logger.warning(f"Could not read file {file_path}: {e}")

    # Clean and deduplicate while maintaining order
    seen: Set[str] = set()
    unique_urls: List[str] = []
    for url in urls:
        cleaned_url = url.rstrip(').,;!?')
        if cleaned_url not in seen:
            seen.add(cleaned_url)
            unique_urls.append(cleaned_url)

    return unique_urls


async def load_already_crawled_async(file_path: Path, retry_failed: bool = False) -> Set[str]:
    """Loads URLs that have already been crawled from the output JSONL file."""
    if not file_path.exists():
        return set()

    crawled_urls: Set[str] = set()
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
            async for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    url = data.get('url')
                    success = data.get('success', False)
                    if url:
                        if retry_failed:
                            if success:
                                crawled_urls.add(url)
                        else:
                            crawled_urls.add(url)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.warning(f"Error reading existing output file {file_path}: {e}")
    return crawled_urls


async def save_crawl_result_async(file_path: Path, url: str, result: CrawlResult) -> None:
    """Saves a single crawl result to the output JSONL file immediately."""
    file_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "url": url,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "success": result.success,
        "error_message": result.error_message if not result.success else None,
        "status_code": getattr(result, "status_code", None),
        "markdown": {
            "raw_markdown": result.markdown.raw_markdown if result.success and result.markdown else None,
            "fit_markdown": result.markdown.fit_markdown if result.success and result.markdown else None,
            "markdown_with_citations": result.markdown.markdown_with_citations if result.success and result.markdown else None,
        } if result.success else None,
    }

    async with aiofiles.open(file_path, 'a', encoding='utf-8') as f:
        await f.write(json.dumps(payload, ensure_ascii=False) + '\n')


async def run_crawler(
    urls: List[str],
    output_path: str,
    concurrency: int = 5,
    retry_failed: bool = False
) -> None:
    """Runs the asynchronous web crawler using Crawl4AI."""
    output_file = Path(output_path)

    # 1. Load already crawled URLs to avoid duplicates
    already_crawled = await load_already_crawled_async(output_file, retry_failed=retry_failed)
    
    # Filter out already crawled URLs
    urls_to_crawl = [url for url in urls if url not in already_crawled]
    skipped_count = len(urls) - len(urls_to_crawl)
    
    if skipped_count > 0:
        logger.info(f"Skipping {skipped_count} already crawled URLs. (Total: {len(urls)})")
        
    if not urls_to_crawl:
        logger.info("No new URLs to crawl. Exiting.")
        return

    logger.info(f"Starting crawl for {len(urls_to_crawl)} URLs with concurrency={concurrency}...")

    # 2. Configure Crawler
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        stream=True,
        verbose=False
    )

    dispatcher = MemoryAdaptiveDispatcher(
        rate_limiter=RateLimiter(base_delay=(1.0, 3.0), max_retries=3),
        max_session_permit=concurrency
    )

    # 3. Visit each URL
    async with AsyncWebCrawler() as crawler:
        try:
            results_generator = await crawler.arun_many(
                urls=urls_to_crawl,
                config=run_config,
                dispatcher=dispatcher
            )
            
            success_count = 0
            failed_count = 0
            
            async for result in results_generator:
                url = result.url
                if result.success:
                    success_count += 1
                    logger.info(f"[SUCCESS] {success_count}/{len(urls_to_crawl)} - Crawled: {url}")
                else:
                    failed_count += 1
                    logger.error(f"[FAILED] {failed_count}/{len(urls_to_crawl)} - Crawled: {url} | Error: {result.error_message}")
                
                # Write output to local leads.jsonl file immediately (crash-resilient)
                await save_crawl_result_async(output_file, url, result)
                
            logger.info(f"Crawl completed. Success: {success_count}, Failed: {failed_count}.")
        except Exception as e:
            logger.exception(f"An unexpected error occurred during crawling: {e}")
            raise


async def main_async() -> None:
    """Async entry point."""
    args = parse_arguments()

    # Collect URLs from all inputs
    all_urls: List[str] = []

    # Positional args
    if args.urls:
        all_urls.extend(args.urls)

    # File input
    if args.file:
        try:
            file_urls = extract_urls_from_file(args.file)
            logger.info(f"Loaded {len(file_urls)} URLs from file: {args.file}")
            all_urls.extend(file_urls)
        except Exception as e:
            logger.error(f"Failed to load URLs from file {args.file}: {e}")
            sys.exit(1)

    # Directory search
    if args.dir:
        try:
            dir_urls = extract_urls_from_directory(args.dir)
            logger.info(f"Loaded {len(dir_urls)} URLs from searching directory: {args.dir}")
            all_urls.extend(dir_urls)
        except Exception as e:
            logger.error(f"Failed to load URLs from directory {args.dir}: {e}")
            sys.exit(1)

    # Deduplicate keeping order
    seen: Set[str] = set()
    final_urls: List[str] = []
    for url in all_urls:
        if url not in seen:
            seen.add(url)
            final_urls.append(url)

    if not final_urls:
        logger.error("No URLs provided. Please specify URLs as arguments, use --file, or use --dir.")
        sys.exit(1)

    logger.info(f"Total unique URLs to process: {len(final_urls)}")

    try:
        await run_crawler(
            urls=final_urls,
            output_path=args.output,
            concurrency=args.concurrency,
            retry_failed=args.retry_failed
        )
    except Exception as e:
        logger.error(f"Crawler failed: {e}")
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
