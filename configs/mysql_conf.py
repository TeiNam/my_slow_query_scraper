"""
MySQL Configuration
Handles MySQL related settings for slow query analysis
"""

from configs.base_config import config
from dataclasses import dataclass


@dataclass(frozen=True)
class MySQLSettings:
    """MySQL 설정 데이터클래스"""
    host: str
    port: int
    user: str
    password: str
    database: str = 'mysql'  # 기본값 mysql
    connect_timeout: int = 10
    pool_size: int = 1


class MySQLConfig:
    """MySQL 설정 관리자"""

    DEFAULT_PORT = 3306
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
            port: int = None,
            connect_timeout: int = None,
            pool_size: int = None
    ) -> MySQLSettings:
        """MySQL 설정 생성"""
        return MySQLSettings(
            host=host,
            port=port or int(config.get('MYSQL_PORT', MySQLConfig.DEFAULT_PORT)),
            user=user,
            password=password,
            database=database,
            connect_timeout=connect_timeout or int(config.get('MYSQL_CONNECT_TIMEOUT', 10)),
            pool_size=pool_size or int(config.get('MYSQL_POOL_SIZE', 1))
        )


# Global instance
mysql_config = MySQLConfig()