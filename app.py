#!/usr/bin/env python3
"""
Slow Query Collector Application
This module serves as the main entry point for the slow query collector application.
It automatically discovers and integrates API routers from the apis directory.
"""

import asyncio
import logging
import sys
import importlib
from datetime import datetime
from typing import Dict, Optional, List
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from modules.mongodb_connector import MongoDBConnector
from modules.common_logger import setup_logger
from modules.time_utils import (
    get_current_utc,
    format_utc
)

# Add project root to Python path
project_root = str(Path(__file__).parent)
if project_root not in sys.path:
    sys.path.append(project_root)

# Set up logging

setup_logger()
logger = logging.getLogger(__name__)

# Configure root logger with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True  # 기존 로깅 설정을 덮어씀
)

# Disable uvicorn access logger
logging.getLogger("uvicorn.access").disabled = True

# Set log level for specific loggers
logging.getLogger('collectors.my_process_scraper').setLevel(logging.INFO)
logging.getLogger('collectors').setLevel(logging.INFO)


class APIManager:
    """API 관리 클래스"""

    def __init__(self):
        self.apis: Dict[str, FastAPI] = {}
        self._base_path = Path(__file__).parent / 'apis'

    async def discover_apis(self) -> None:
        if not self._base_path.exists():
            logger.error(f"APIs directory not found: {self._base_path}")
            return

        api_files = [f for f in self._base_path.glob("*.py")
                     if f.is_file() and not f.name.startswith('__')]

        for api_file in api_files:
            module_name = f"apis.{api_file.stem}"
            try:
                module = importlib.import_module(module_name)

                # FastAPI 앱 찾기
                for attr_name, attr_value in module.__dict__.items():
                    if isinstance(attr_value, FastAPI):
                        # 프리픽스 없이 직접 FastAPI 앱 사용
                        self.apis[api_file.stem] = attr_value
                        logger.info(f"Loaded API: {api_file.stem}")
                        break

            except Exception as e:
                logger.error(f"Error loading API {module_name}: {e}")


class QueryCollectorApp:
    """Main application class for managing slow query collectors and APIs."""

    def __init__(self):
        self.api_manager = APIManager()
        self.is_running: bool = False
        self.start_time: Optional[datetime] = None
        self.app = FastAPI(
            title="Slow Query Collector",
            description="Integrated API for collecting and monitoring slow queries",
            version="1.0.0",
            lifespan=self.lifespan
        )

        # CORS 미들웨어 추가
        origins = [
            "https://mgmt.sql.devops.torder.tech",
            "http://localhost:5173",  # 개발 환경용
        ]

        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
            allow_headers=[
                "Content-Type",
                "Authorization",
                "Accept",
                "Origin",
                "X-Requested-With",
                "Access-Control-Request-Method",
                "Access-Control-Request-Headers"
            ],
            expose_headers=["*"],
            max_age=86400,  # preflight 요청 캐시 시간 (24시간)
        )

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Application lifespan manager"""
        try:
            # Startup
            await MongoDBConnector.initialize()
            logger.info("MongoDB connection initialized")
            await self.api_manager.discover_apis()
            self._mount_apis()
            logger.info("APIs mounted successfully")
            yield
        finally:
            # Shutdown
            await MongoDBConnector.close()
            logger.info("MongoDB connection closed")

    def _mount_apis(self) -> None:
        """Mount discovered APIs to the main application"""
        for api_name, api in self.api_manager.apis.items():
            for route in api.routes:
                self.app.routes.append(route)
            logger.info(f"Mounted routes for {api_name}")

    async def start(self) -> None:
        """Start the application server"""
        if self.is_running:
            logger.warning("Application is already running")
            return

        self.is_running = True
        self.start_time = get_current_utc()
        logger.info(f"Starting Query Collector Application at {format_utc(self.start_time)}")

        # Configure and start uvicorn server
        import uvicorn
        config = uvicorn.Config(
            app=self.app,
            host="0.0.0.0",
            port=8000,
            log_level="info",
            log_config=None,  # Disable uvicorn's default logging config
        )
        server = uvicorn.Server(config)
        await server.serve()

    async def stop(self) -> None:
        """Stop the application gracefully"""
        if not self.is_running:
            logger.warning("Application is not running")
            return

        logger.info("Stopping application...")
        self.is_running = False

    def get_uptime(self) -> Optional[float]:
        """Return application uptime in seconds"""
        if not self.start_time:
            return None
        return (get_current_utc() - self.start_time).total_seconds()


async def main():
    """Application entry point"""
    logger.info("Initializing Query Collector Application")
    app = QueryCollectorApp()

    try:
        await app.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await app.stop()
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application shutdown complete")
