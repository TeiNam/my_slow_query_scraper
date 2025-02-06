"""
MySQL Configuration
Handles MySQL connection and collection settings
"""

from configs.base_config import config
from dataclasses import dataclass
from typing import Optional
import logging

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class MySQLSettings:
    """MySQL 설정 데이터클래스"""
    host: str
    port: int
    user: str
    password: str
    database: str = 'mysql'
    connect_timeout: int = 10
    pool_size: int = 1

class MySQLConfig:
    """MySQL 설정 관리자"""

    MIN_EXEC_TIME = 1  # 최소 실행 시간 (초)

    @staticmethod
    def get_exec_time() -> int:
        """슬로우 쿼리 기준 시간(초) 반환"""
        return max(int(config.get('MYSQL_EXEC_TIME', 2)), MySQLConfig.MIN_EXEC_TIME)

    @staticmethod
    def create_settings(
            host: str,
            user: str,
            password: str,
            database: str = 'mysql',
            port: Optional[int] = None,
            connect_timeout: Optional[int] = None,
            pool_size: Optional[int] = None
    ) -> MySQLSettings:
        """MySQL 설정 생성"""
        return MySQLSettings(
            host=host,
            port=port or int(config.get('MYSQL_PORT', 3306)),
            user=user,
            password=password,
            database=database,
            connect_timeout=connect_timeout or int(config.get('MYSQL_CONNECT_TIMEOUT', 10)),
            pool_size=pool_size or int(config.get('MYSQL_POOL_SIZE', 1))
        )

    @classmethod
    def get_management_settings(cls) -> MySQLSettings:
        """관리자 계정 MySQL 설정 반환"""
        mgmt_user = config.get('MGMT_USER')
        mgmt_password = config.get('MGMT_USER_PASS')

        if not mgmt_user or not mgmt_password:
            raise ValueError("MGMT_USER and MGMT_USER_PASS must be set in environment variables")

        return cls.create_settings(
            host=config.get('MYSQL_HOST', 'localhost'),
            user=mgmt_user,
            password=mgmt_password,
            database=config.get('MYSQL_DATABASE', 'mysql')
        )

    @classmethod
    def get_default_settings(cls) -> MySQLSettings:
        """기본 MySQL 설정 반환 (일반적인 접속용)"""
        return cls.create_settings(
            host=config.get('MYSQL_HOST', 'localhost'),
            user=config.get('MYSQL_USER', 'root'),
            password=config.get('MYSQL_PASSWORD', ''),
            database=config.get('MYSQL_DATABASE', 'mysql')
        )

# 전역 인스턴스들
mysql_config = MySQLConfig()
mysql_settings = mysql_config.get_management_settings()  # 모니터링용으로 관리자 계정 사용