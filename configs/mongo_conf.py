"""
MongoDB Configuration
Handles MongoDB connection and collection settings
"""

from configs.base_config import config
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional
from urllib.parse import urlparse, urlunparse, parse_qs
import logging

logger = logging.getLogger(__name__)


class MongoSettings(BaseSettings):
    """MongoDB 설정 클래스"""

    # 기본 연결 설정
    MONGODB_URI: Optional[str] = config.get("MONGODB_URI", "mongodb://localhost:27017")
    MONGODB_DB_NAME: str = config.get("MONGODB_DB_NAME", "mgmt_mysql")
    MONGODB_USER: Optional[str] = config.get("MONGODB_USER", "admin")
    MONGODB_PASSWORD: Optional[str] = config.get("MONGODB_PASSWORD", "admin")
    MONGO_TLS: bool = config.get("MONGO_TLS", "false").lower() == "true"

    # 컬렉션 이름 설정
    MONGO_RDS_INSTANCE_COLLECTION: str = config.get(
        "MONGO_RDS_MYSQL_INSTANCE_COLLECTION", "rds_mysql_instance"
    )
    MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION: str = config.get(
        "MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION", "rds_mysql_realtime_slow_query"
    )
    MONGO_RDS_MYSQL_SLOW_SQL_PLAN_COLLECTION: str = config.get(
        "MONGO_RDS_MYSQL_SLOW_SQL_PLAN_COLLECTION", "rds_mysql_slow_query_explain"
    )
    MONGO_CW_MYSQL_SLOW_SQL_COLLECTION: str = config.get(
        "MONGO_CW_MYSQL_SLOW_SQL_COLLECTION", "rds_mysql_cw_slow_query"
    )

    class Config:
        extra = "ignore"  # 추가 필드 허용

    @property
    def connection_uri(self) -> str:
        """MongoDB 연결 URI 생성"""
        try:
            uri = self.MONGODB_URI or "mongodb://localhost:27017"

            if self.MONGODB_USER and self.MONGODB_PASSWORD:
                # URI 파싱
                parsed = urlparse(uri)
                if '@' not in parsed.netloc:
                    # 인증 정보 추가
                    netloc = f"{self.MONGODB_USER}:{self.MONGODB_PASSWORD}@{parsed.netloc}"
                    parsed = parsed._replace(netloc=netloc)

                return urlunparse(parsed)

            return uri
        except Exception as e:
            logger.error(f"Error creating MongoDB connection URI: {e}")
            return "mongodb://localhost:27017"


@lru_cache()
def get_mongo_settings() -> MongoSettings:
    """MongoDB 설정 싱글톤 인스턴스 반환"""
    try:
        settings = MongoSettings()
        logger.info(f"MongoDB database name: {settings.MONGODB_DB_NAME}")
        logger.debug(f"MongoDB collections: {settings.MONGO_RDS_INSTANCE_COLLECTION}, "
                    f"{settings.MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION}")
        return settings
    except Exception as e:
        logger.error(f"Error initializing MongoDB settings: {e}")
        # 기본값으로 설정 생성
        return MongoSettings(
            MONGODB_URI="mongodb://localhost:27017",
            MONGODB_DB_NAME="mgmt_mysql",
            MONGODB_USER="admin",
            MONGODB_PASSWORD="admin"
        )


# Global instance
mongo_settings = get_mongo_settings()