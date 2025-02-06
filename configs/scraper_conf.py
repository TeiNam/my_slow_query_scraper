"""
Scraper Configuration
MySQL Process Scraper를 위한 설정 관리
"""

from typing import Dict, Any, Set
from dataclasses import dataclass, asdict
import logging
from configs.base_config import config

logger = logging.getLogger(__name__)

@dataclass
class ScraperSettings:
    """스크래퍼 설정 데이터클래스"""
    exec_time: int
    monitoring_interval: int
    excluded_dbs: Set[str]
    excluded_users: Set[str]

    def as_dict(self) -> Dict[str, Any]:
        """설정을 딕셔너리로 변환"""
        return asdict(self)

    def __getitem__(self, key: str) -> Any:
        """딕셔너리 스타일 접근 지원"""
        return getattr(self, key)

class ScraperConfigManager:
    """MySQL Process Scraper 설정 관리자"""

    # 기본값 정의
    DEFAULT_EXEC_TIME = 2
    DEFAULT_MONITORING_INTERVAL = 1
    DEFAULT_EXCLUDED_DBS = {'information_schema', 'mysql', 'performance_schema'}
    DEFAULT_EXCLUDED_USERS = {'monitor', 'rdsadmin', 'system user', 'mysql_mgmt'}

    def get_exec_time(self) -> int:
        """MySQL 실행 시간 임계값 조회"""
        try:
            return int(config.get('MYSQL_EXEC_TIME', self.DEFAULT_EXEC_TIME))
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid MYSQL_EXEC_TIME value, using default value {self.DEFAULT_EXEC_TIME}: {e}")
            return self.DEFAULT_EXEC_TIME

    def get_monitoring_interval(self) -> int:
        """모니터링 간격(초) 조회"""
        try:
            interval = int(config.get('MYSQL_MONITORING_INTERVAL', self.DEFAULT_MONITORING_INTERVAL))
            return max(1, interval)  # 최소 1초 보장
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid monitoring interval, using default value {self.DEFAULT_MONITORING_INTERVAL}: {e}")
            return self.DEFAULT_MONITORING_INTERVAL

    def get_excluded_databases(self) -> Set[str]:
        """모니터링 제외 데이터베이스 목록 조회"""
        try:
            excluded_dbs = config.get('MYSQL_EXCLUDED_DBS')
            if excluded_dbs:
                return set(item.strip() for item in excluded_dbs.split(','))
            return self.DEFAULT_EXCLUDED_DBS
        except Exception as e:
            logger.warning(f"Error processing excluded databases, using defaults: {e}")
            return self.DEFAULT_EXCLUDED_DBS

    def get_excluded_users(self) -> Set[str]:
        """모니터링 제외 사용자 목록 조회"""
        try:
            excluded_users = config.get('MYSQL_EXCLUDED_USERS')
            if excluded_users:
                return set(item.strip() for item in excluded_users.split(','))
            return self.DEFAULT_EXCLUDED_USERS
        except Exception as e:
            logger.warning(f"Error processing excluded users, using defaults: {e}")
            return self.DEFAULT_EXCLUDED_USERS

    def get_settings(self) -> ScraperSettings:
        """모든 스크래퍼 설정 조회"""
        settings = ScraperSettings(
            exec_time=self.get_exec_time(),
            monitoring_interval=self.get_monitoring_interval(),
            excluded_dbs=self.get_excluded_databases(),
            excluded_users=self.get_excluded_users()
        )
        logger.debug(f"Generated scraper settings: {settings.as_dict()}")
        return settings

# 전역 인스턴스
scraper_config = ScraperConfigManager()
scraper_settings = scraper_config.get_settings()