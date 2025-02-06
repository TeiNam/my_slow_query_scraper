"""
MongoDB Configuration
"""

from configs.base_config import config
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional
from urllib.parse import urlparse, urlunparse
import logging

logger = logging.getLogger(__name__)

class MongoSettings(BaseSettings):
    """MongoDB 설정 클래스"""

    def __init__(self, **kwargs):
        # 환경변수에서 MONGODB_URI를 먼저 확인
        mongodb_uri = config.get("MONGODB_URI")
        if not mongodb_uri:
            raise ValueError("MONGODB_URI is not set in environment variables")

        # localhost 체크
        if 'localhost' in mongodb_uri and os.getenv('DOCKER_ENV'):
            raise ValueError("MONGODB_URI should not be set to localhost in Docker environment.")

        super().__init__(**kwargs)

    # 기본 연결 설정
    MONGODB_URI: str
    MONGODB_DB_NAME: str = config.get("MONGODB_DB_NAME", "mgmt_rds_mysql")
    MONGODB_USER: Optional[str] = config.get("MONGODB_USER", "mgmt_monitor")
    MONGODB_PASSWORD: Optional[str] = config.get("MONGODB_PASSWORD")
    MONGO_TLS: bool = config.get("MONGO_TLS", "false").lower() == "true"

    # 컬렉션 이름 설정
    MONGO_RDS_INSTANCE_COLLECTION: str = "rds_mysql_instance"
    MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION: str = "rds_mysql_realtime_slow_query"
    MONGO_RDS_MYSQL_SLOW_SQL_PLAN_COLLECTION: str = "rds_mysql_slow_query_explain"
    MONGO_CW_MYSQL_SLOW_SQL_COLLECTION: str = "rds_mysql_cw_slow_query"

    class Config:
        env_prefix = ''
        case_sensitive = True

    @property
    def connection_uri(self) -> str:
        """MongoDB 연결 URI 생성"""
        try:
            uri = self.MONGODB_URI
            if self.MONGODB_USER and self.MONGODB_PASSWORD:
                parsed = urlparse(uri)
                if '@' not in parsed.netloc:
                    netloc = f"{self.MONGODB_USER}:{self.MONGODB_PASSWORD}@{parsed.netloc}"
                    parsed = parsed._replace(netloc=netloc)
                return urlunparse(parsed)
            return uri
        except Exception as e:
            logger.error(f"Error creating MongoDB connection URI: {e}")
            raise ValueError(f"Invalid MongoDB URI configuration: {e}")


@lru_cache()
def get_mongo_settings() -> MongoSettings:
    """MongoDB 설정 싱글톤 인스턴스 반환"""
    try:
        settings = MongoSettings()
        logger.info(f"MongoDB database name: {settings.MONGODB_DB_NAME}")
        return settings
    except Exception as e:
        logger.error(f"Error initializing MongoDB settings: {e}")
        raise

# Global instance
mongo_settings = get_mongo_settings()