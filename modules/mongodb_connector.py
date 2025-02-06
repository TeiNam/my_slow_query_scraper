"""
MongoDB Connector
Provides asynchronous access to MongoDB using Motor
"""

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from configs.mongo_conf import mongo_settings
import logging
from typing import Optional, Dict, Any
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

logger = logging.getLogger(__name__)


class MongoDBConnector:
    """MongoDB 연결 관리자"""

    _client: Optional[AsyncIOMotorClient] = None
    _db: Optional[AsyncIOMotorDatabase] = None

    @classmethod
    async def initialize(cls) -> None:
        """MongoDB 연결 초기화"""
        if cls._client is None:
            await cls._connect()

    @classmethod
    async def get_database(cls) -> AsyncIOMotorDatabase:
        """데이터베이스 인스턴스 반환"""
        if cls._client is None or not await cls._is_connected():
            await cls._connect()
        return cls._db

    @classmethod
    async def reconnect(cls) -> None:
        """MongoDB 재연결"""
        await cls.close()
        await cls._connect()

    @classmethod
    async def close(cls) -> None:
        """MongoDB 연결 종료"""
        if cls._client:
            cls._client.close()
        cls._client = None
        cls._db = None

    @classmethod
    async def _connect(cls) -> None:
        """MongoDB 연결 수립"""
        try:
            connection_kwargs: Dict[str, Any] = {
                "serverSelectionTimeoutMS": 5000,
                "directConnection": False
            }

            if mongo_settings.MONGO_TLS:
                connection_kwargs.update({
                    "tls": True,
                    "tlsAllowInvalidCertificates": True,
                    "tlsAllowInvalidHostnames": True
                })

            if "localhost" in mongo_settings.connection_uri:
                raise ValueError("MONGODB_URI should not be set to localhost in Docker environment.")

            cls._client = AsyncIOMotorClient(mongo_settings.connection_uri, **connection_kwargs)
            cls._db = cls._client[mongo_settings.MONGODB_DB_NAME]
            await cls._client.admin.command('ping')
            logger.info("MongoDB connection established successfully")

        except Exception as e:
            cls._client = None
            cls._db = None
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    @classmethod
    async def _is_connected(cls) -> bool:
        """MongoDB 연결 상태 확인"""
        try:
            await cls._client.admin.command('ping')
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.debug(f"MongoDB connection check failed: {str(e)}")
            return False


# 글로벌 인스턴스
mongodb = MongoDBConnector()