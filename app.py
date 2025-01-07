#!/usr/bin/env python3
"""
Slow Query Collector Application
This module serves as the main entry point for the slow query collector application.
It provides the basic async framework for managing multiple collectors.
"""

import asyncio
import logging
from typing import List, Optional, Dict
from pathlib import Path
from modules.time_utils import (
    get_current_utc,
    format_utc,
    get_date_range,
    to_utc,
    get_current_kst,
    format_kst
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class QueryCollectorApp:
    """Main application class for managing slow query collectors."""

    def __init__(self):
        self.collectors: Dict[str, "BaseCollector"] = {}
        self.is_running: bool = False
        self.start_time: Optional[datetime] = None

    async def register_collector(self, name: str, collector: "BaseCollector") -> None:
        """Register a new collector with the application.

        Args:
            name: Unique identifier for the collector
            collector: Instance of BaseCollector or its subclasses
        """
        if name in self.collectors:
            logger.warning(f"Collector {name} already exists. Replacing...")
        self.collectors[name] = collector
        logger.info(f"Registered collector: {name}")

    async def start(self) -> None:
        """Start all registered collectors."""
        if self.is_running:
            logger.warning("Application is already running")
            return

        self.is_running = True
        self.start_time = get_current_utc()
        logger.info(f"Starting Query Collector Application at {format_utc(self.start_time)}")

        # Create tasks for all collectors
        collector_tasks = [
            collector.start()
            for collector in self.collectors.values()
        ]

        try:
            await asyncio.gather(*collector_tasks)
        except Exception as e:
            logger.error(f"Error running collectors: {e}")
            raise

    async def stop(self) -> None:
        """Stop all registered collectors gracefully."""
        if not self.is_running:
            logger.warning("Application is not running")
            return

        logger.info("Stopping all collectors...")
        stop_tasks = [
            collector.stop()
            for collector in self.collectors.values()
        ]

        await asyncio.gather(*stop_tasks)
        self.is_running = False

    def get_uptime(self) -> Optional[float]:
        """Return application uptime in seconds."""
        if not self.start_time:
            return None
        return (get_current_utc() - self.start_time).total_seconds()


async def main():
    """Application entry point."""
    app = QueryCollectorApp()

    try:
        # TODO: Register collectors here
        # await app.register_collector("mysql", MySQLCollector())
        # await app.register_collector("postgres", PostgresCollector())

        await app.start()

        # Keep the application running
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await app.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application shutdown complete")