"""
MongoDB Configuration
Handles MongoDB connection and collection settings
"""

from configs.base_config import config
from pydantic_settings import BaseSettings
from functools import lru_cache
from urllib.parse import urlparse, urlunparse, parse_qs


class MongoSettings(BaseSettings):
    """MongoDB 설정 클래스"""

    # 기본 연결 설정
    MONGODB_URI: str = config.get("MONGODB_URI")
    MONGODB_DB_NAME: str = config.get("MONGODB_DB_NAME", "mgmt_mysql")
    MONGODB_USER: str = config.get("MONGODB_USER")
    MONGODB_PASSWORD: str = config.get("MONGODB_PASSWORD")
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

    class Config:
        extra = "ignore"  # 추가 필드 허용

    @property
    def connection_uri(self) -> str:
        """MongoDB 연결 URI 생성"""
        if not self.MONGODB_URI:
            raise ValueError("MONGODB_URI is not set")

        if self.MONGODB_USER and self.MONGODB_PASSWORD:
            # URI 파싱
            parsed = urlparse(self.MONGODB_URI)
            if '@' not in parsed.netloc:
                # 인증 정보 추가
                netloc = f"{self.MONGODB_USER}:{self.MONGODB_PASSWORD}@{parsed.netloc}"
                parsed = parsed._replace(netloc=netloc)

            return urlunparse(parsed)

        return self.MONGODB_URI


@lru_cache()
def get_mongo_settings() -> MongoSettings:
    """MongoDB 설정 싱글톤 인스턴스 반환"""
    settings = MongoSettings()
    if not settings.MONGODB_URI:
        raise ValueError("MONGODB_URI is not set")
    if not settings.MONGODB_DB_NAME:
        raise ValueError("MONGODB_DB_NAME is not set")
    return settings


# Global instance
mongo_settings = get_mongo_settings()