"""
MySQL Configuration Manager
통합된 MySQL 설정 관리
"""

from typing import Dict, Any, Set, Optional
from dataclasses import dataclass
from configs.base_config import config
import logging

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class MySQLConnectionSettings:
    """MySQL 연결 설정"""
    host: str
    port: int
    user: str
    password: str
    database: str = 'mysql'
    connect_timeout: int = 10
    pool_size: int = 1

@dataclass(frozen=True)
class MySQLMonitoringSettings:
    """MySQL 모니터링 설정"""
    exec_time: int
    excluded_dbs: Set[str]
    excluded_users: Set[str]
    monitoring_interval: int

class MySQLSettingsManager:
    """MySQL 설정 통합 관리자"""

    # 상수 정의
    DEFAULT_PORT = 3306
    MIN_EXEC_TIME = 1
    DEFAULT_EXEC_TIME = 2
    DEFAULT_MONITORING_INTERVAL = 1
    DEFAULT_CONNECT_TIMEOUT = 10
    DEFAULT_POOL_SIZE = 1

    DEFAULT_EXCLUDED_DBS = {'information_schema', 'mysql', 'performance_schema'}
    DEFAULT_EXCLUDED_USERS = {'monitor', 'rdsadmin', 'system user', 'mysql_mgmt'}

    @classmethod
    def create_connection_settings(
        cls,
        host: str,
        user: str,
        password: str,
        database: str = 'mysql',
        port: Optional[int] = None,
        connect_timeout: Optional[int] = None,
        pool_size: Optional[int] = None
    ) -> MySQLConnectionSettings:
        """MySQL 연결 설정 생성"""
        return MySQLConnectionSettings(
            host=host,
            port=port or int(config.get('MYSQL_PORT', cls.DEFAULT_PORT)),
            user=user,
            password=password,
            database=database,
            connect_timeout=connect_timeout or int(config.get('MYSQL_CONNECT_TIMEOUT', cls.DEFAULT_CONNECT_TIMEOUT)),
            pool_size=pool_size or int(config.get('MYSQL_POOL_SIZE', cls.DEFAULT_POOL_SIZE))
        )

    @classmethod
    def create_monitoring_settings(cls) -> MySQLMonitoringSettings:
        """MySQL 모니터링 설정 생성"""
        return MySQLMonitoringSettings(
            exec_time=cls._get_exec_time(),
            excluded_dbs=cls._get_excluded_databases(),
            excluded_users=cls._get_excluded_users(),
            monitoring_interval=cls._get_monitoring_interval()
        )

    @classmethod
    def _get_exec_time(cls) -> int:
        """실행 시간 임계값 조회"""
        try:
            exec_time = int(config.get('MYSQL_EXEC_TIME', cls.DEFAULT_EXEC_TIME))
            return max(exec_time, cls.MIN_EXEC_TIME)
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid MYSQL_EXEC_TIME value, using default: {e}")
            return cls.DEFAULT_EXEC_TIME

    @classmethod
    def _get_excluded_databases(cls) -> Set[str]:
        """모니터링 제외 데이터베이스 목록 조회"""
        try:
            excluded_dbs = config.get('MYSQL_EXCLUDED_DBS')
            if excluded_dbs:
                return set(db.strip() for db in excluded_dbs.split(','))
            return cls.DEFAULT_EXCLUDED_DBS
        except Exception as e:
            logger.warning(f"Error processing excluded databases, using defaults: {e}")
            return cls.DEFAULT_EXCLUDED_DBS

    @classmethod
    def _get_excluded_users(cls) -> Set[str]:
        """모니터링 제외 사용자 목록 조회"""
        try:
            excluded_users = config.get('MYSQL_EXCLUDED_USERS')
            if excluded_users:
                return set(user.strip() for user in excluded_users.split(','))
            return cls.DEFAULT_EXCLUDED_USERS
        except Exception as e:
            logger.warning(f"Error processing excluded users, using defaults: {e}")
            return cls.DEFAULT_EXCLUDED_USERS

    @classmethod
    def _get_monitoring_interval(cls) -> int:
        """모니터링 간격 조회"""
        try:
            interval = int(config.get('MYSQL_MONITORING_INTERVAL', cls.DEFAULT_MONITORING_INTERVAL))
            return max(interval, 1)  # 최소 1초
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid monitoring interval, using default: {e}")
            return cls.DEFAULT_MONITORING_INTERVAL

# 글로벌 인스턴스
mysql_settings = MySQLSettingsManager()