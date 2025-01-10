#!/usr/bin/env python3
"""
Slow Query Collector Application
This module serves as the main entry point for the slow query collector application.
It automatically discovers and integrates API routers from the apis directory.
"""

import asyncio
import logging
import importlib
from datetime import datetime
from typing import Dict, Optional, Any
from modules.collectors import BaseCollector
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter
from modules.mongodb_connector import MongoDBConnector
from modules.time_utils import (get_current_utc, format_utc)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class APIManager:
    """API 관리 클래스"""

    def __init__(self):
        # FastAPI 인스턴스를 저장하는 딕셔너리임을 명시
        self.apis: Dict[str, FastAPI] = {}
        self._base_path = Path(__file__).parent / 'apis'

    async def discover_apis(self) -> None:
        """apis 디렉토리에서 API 모듈을 자동으로 발견하고 로드"""
        if not self._base_path.exists():
            logger.error(f"APIs directory not found: {self._base_path}")
            return

        # .py 파일 검색
        api_files = [f for f in self._base_path.glob("*.py")
                     if f.is_file() and not f.name.startswith('__')]

        for api_file in api_files:
            module_name = f"apis.{api_file.stem}"
            try:
                module = importlib.import_module(module_name)

                # FastAPI 앱 찾기
                for attr_name, attr_value in module.__dict__.items():
                    if isinstance(attr_value, FastAPI):
                        self.apis[api_file.stem] = attr_value  # FastAPI 타입으로 저장
                        logger.info(f"Loaded API: {api_file.stem}")
                        break

            except Exception as e:
                logger.error(f"Error loading API {module_name}: {e}")


class QueryCollectorApp:
    """Main application class for managing slow query collectors and APIs."""

    def __init__(self):
        self.collectors: Dict[str, Any] = {}
        self.api_manager = APIManager()
        self.is_running: bool = False
        self.start_time: Optional[datetime] = None
        self.app = FastAPI(
            title="Slow Query Collector",
            description="Integrated API for collecting and monitoring slow queries",
            version="1.0.0",
            lifespan=self.lifespan
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
            yield
        finally:
            # Shutdown
            await MongoDBConnector.close()
            logger.info("MongoDB connection closed")

    def _mount_apis(self) -> None:
        """Mount discovered APIs to the main application"""
        for api_name, api in self.api_manager.apis.items():
            # API의 모든 라우트를 메인 앱에 등록
            for route in api.routes:
                self.app.routes.append(route)

    async def start(self) -> None:
        """Start the application server"""
        if self.is_running:
            logger.warning("Application is already running")
            return

        self.is_running = True
        self.start_time = get_current_utc()
        logger.info(f"Starting Query Collector Application at {format_utc(self.start_time)}")

        # Uvicorn 서버 시작
        import uvicorn
        config = uvicorn.Config(
            app=self.app,
            host="0.0.0.0",
            port=8000,
            log_level="info"
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
    app = QueryCollectorApp()

    try:
        await app.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await app.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application shutdown complete")