#!/usr/bin/env python3
"""
daemon.py: Infinite loop orchestrator for the B2B lead-gen pipeline.
Sequentially runs lead sourcing, crawling, qualifying, enrichment, and outreach.
Manages search queries via data/queries.txt and handles subprocess failures gracefully.
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Optional, List

import aiofiles

# Setup logs directory
Path("logs").mkdir(exist_ok=True)

# Configure Logging to file logs/daemon.log and stdout
log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler("logs/daemon.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("pipeline-daemon")


def parse_arguments() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        description="Infinite loop B2B lead-gen pipeline orchestrator daemon."
    )
    parser.add_argument(
        "-q", "--queries-file",
        type=str,
        default="data/queries.txt",
        help="Path to the queries text file (default: data/queries.txt)."
    )
    parser.add_argument(
        "-i", "--interval",
        type=float,
        default=86400.0,
        help="Sleep interval in seconds after finishing outreach (default: 86400.0 / 24 hours)."
    )
    parser.add_argument(
        "-c", "--check-interval",
        type=float,
        default=60.0,
        help="Interval in seconds to check for new queries when the queries file is empty (default: 60.0)."
    )
    return parser.parse_args()


async def pop_next_query(file_path: Path) -> Optional[str]:
    """Pops the top line off the queries file and saves the rest back to the file."""
    if not file_path.exists():
        return None

    try:
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            lines = await f.readlines()

        cleaned_lines = [line.strip() for line in lines if line.strip()]
        if not cleaned_lines:
            return None

        next_query = cleaned_lines[0]
        remaining_lines = cleaned_lines[1:]

        # Write remaining lines back to the file
        async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
            for line in remaining_lines:
                await f.write(line + "\n")

        return next_query
    except Exception as e:
        logger.error(f"Error managing queries file {file_path}: {e}")
        return None


async def execute_command(cmd: List[str]) -> bool:
    """Executes a subprocess command asynchronously, printing output and returning success status."""
    logger.info(f"Executing: {' '.join(cmd)}")
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Wait for command completion
        stdout, stderr = await process.communicate()
        
        # Log outputs
        if stdout:
            stdout_str = stdout.decode(errors="ignore").strip()
            if stdout_str:
                logger.info(f"Subprocess stdout:\n{stdout_str}")
        if stderr:
            stderr_str = stderr.decode(errors="ignore").strip()
            if stderr_str:
                logger.warning(f"Subprocess stderr:\n{stderr_str}")

        if process.returncode != 0:
            logger.error(f"Command failed with exit code {process.returncode}: {' '.join(cmd)}")
            return False

        logger.info(f"Command succeeded: {' '.join(cmd)}")
        return True
    except Exception as e:
        logger.error(f"Exception occurred while executing command {' '.join(cmd)}: {e}", exc_info=True)
        return False


async def run_pipeline_cycle(query: str) -> bool:
    """Runs one full sequential run of the B2B lead generation pipeline."""
    logger.info(f"--- STARTING NEW PIPELINE CYCLE FOR QUERY: '{query}' ---")

    # Pipeline commands
    pipeline_steps = [
        ["uv", "run", "python", "sourcer.py", query],
        ["uv", "run", "python", "main.py", "--file", "data/source_urls.txt"],
        ["uv", "run", "python", "analyzer.py"],
        ["uv", "run", "python", "enricher.py"],
        ["uv", "run", "python", "outreach.py"]
    ]

    for step in pipeline_steps:
        success = await execute_command(step)
        if not success:
            logger.error(f"Pipeline execution aborted at step: {' '.join(step)}")
            return False

    logger.info(f"--- PIPELINE CYCLE COMPLETED SUCCESSFULLY FOR QUERY: '{query}' ---")
    return True


async def main_async() -> None:
    """Async main routine."""
    args = parse_arguments()
    queries_file = Path(args.queries_file)

    # Ensure data directory exists
    queries_file.parent.mkdir(parents=True, exist_ok=True)

    logger.info("B2B Lead Generation Daemon initialized.")
    logger.info(f"Settings: queries_file={queries_file}, cycle_sleep={args.interval}s, check_sleep={args.check_interval}s")

    while True:
        # 1. Pop next query
        query = await pop_next_query(queries_file)

        if not query:
            # Alert the user
            msg = f"Alert: Queries file '{queries_file}' is empty or missing! Please add search queries (one per line) to resume."
            logger.warning(msg)
            print(f"\n{msg}\n", file=sys.stderr)
            
            # Wait before checking again
            await asyncio.sleep(args.check_interval)
            continue

        logger.info(f"Popped query: '{query}'")

        # 2. Execute pipeline step-by-step
        cycle_success = await run_pipeline_cycle(query)

        if cycle_success:
            logger.info("Outreach completed successfully. Entering sleep cycle.")
        else:
            logger.error("Pipeline cycle failed. Check logs for details. Entering sleep cycle before trying next query.")

        # 3. Pause for the configured interval (e.g. 24 hours) to reset rate limits
        logger.info(f"Sleeping for {args.interval} seconds...")
        await asyncio.sleep(args.interval)


def main() -> None:
    """Sync wrapper for main."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Daemon interrupted by user. Shutting down gracefully.")
        sys.exit(0)


if __name__ == "__main__":
    main()
