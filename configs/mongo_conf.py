"""
MongoDB Configuration
"""

from configs.base_config import config
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from typing import Optional
from urllib.parse import urlparse, urlunparse
import os
import logging

logger = logging.getLogger(__name__)

class MongoSettings(BaseSettings):
    """MongoDB 설정 클래스"""

    # 기본 연결 설정
    MONGODB_URI: str = ""  # 필수 값이지만 기본값 설정
    MONGODB_DB_NAME: str = "mgmt_rds_mysql"
    MONGODB_USER: Optional[str] = "mgmt_monitor"
    MONGODB_PASSWORD: Optional[str] = None
    MONGO_TLS: bool = False

    # 컬렉션 이름 설정
    MONGO_RDS_INSTANCE_COLLECTION: str = "rds_mysql_instance"
    MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION: str = "rds_mysql_realtime_slow_query"
    MONGO_RDS_MYSQL_SLOW_SQL_PLAN_COLLECTION: str = "rds_mysql_slow_query_explain"
    MONGO_CW_MYSQL_SLOW_SQL_COLLECTION: str = "rds_mysql_cw_slow_query"

    model_config = SettingsConfigDict(
        env_prefix='',
        case_sensitive=True,
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    def model_post_init(self, _):
        """설정 검증"""
        # MONGODB_URI 확인
        self.MONGODB_URI = config.get("MONGODB_URI", self.MONGODB_URI)
        if not self.MONGODB_URI:
            raise ValueError("MONGODB_URI is not set in environment variables")

        # Docker 환경에서 localhost 체크
        if os.getenv('DOCKER_ENV') and 'localhost' in self.MONGODB_URI:
            raise ValueError("MONGODB_URI should not be set to localhost in Docker environment")

        # 환경변수에서 나머지 설정 로드
        self.MONGODB_DB_NAME = config.get("MONGODB_DB_NAME", self.MONGODB_DB_NAME)
        self.MONGODB_USER = config.get("MONGODB_USER", self.MONGODB_USER)
        self.MONGODB_PASSWORD = config.get("MONGODB_PASSWORD", self.MONGODB_PASSWORD)
        self.MONGO_TLS = str(config.get("MONGO_TLS", "false")).lower() == "true"

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

    def dict(self, *args, **kwargs):
        """설정값을 딕셔너리로 변환 (비밀번호 마스킹)"""
        d = super().dict(*args, **kwargs)
        if d.get('MONGODB_PASSWORD'):
            d['MONGODB_PASSWORD'] = '****'
        return d

@lru_cache()
def get_mongo_settings() -> MongoSettings:
    """MongoDB 설정 싱글톤 인스턴스 반환"""
    try:
        settings = MongoSettings()
        logger.info(f"MongoDB configuration loaded: {settings.dict()}")
        return settings
    except Exception as e:
        logger.error(f"Error initializing MongoDB settings: {e}")
        raise

# Global instance
mongo_settings = get_mongo_settings()