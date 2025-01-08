"""
Scraper Configuration
Manages configuration settings for MySQL Process Scraper
"""

from typing import Dict, Any, Set
import logging
from configs.base_config import config

logger = logging.getLogger(__name__)


class ScraperConfig:
    """MySQL Process Scraper Configuration Manager"""

    DEFAULT_EXEC_TIME = 2
    DEFAULT_MONITORING_INTERVAL = 1
    DEFAULT_EXCLUDED_DBS = {'information_schema', 'mysql', 'performance_schema'}
    DEFAULT_EXCLUDED_USERS = {'monitor', 'rdsadmin', 'system user', 'mysql_mgmt'}

    @staticmethod
    def get_mysql_exec_time() -> int:
        """Get MySQL execution time threshold"""
        try:
            exec_time = config.get('MYSQL_EXEC_TIME', ScraperConfig.DEFAULT_EXEC_TIME)
            return int(exec_time)
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid MYSQL_EXEC_TIME value, using default value {ScraperConfig.DEFAULT_EXEC_TIME}: {e}")
            return ScraperConfig.DEFAULT_EXEC_TIME

    @staticmethod
    def get_excluded_databases() -> Set[str]:
        """Get list of databases to exclude from monitoring"""
        try:
            excluded_dbs = config.get('MYSQL_EXCLUDED_DBS')
            if excluded_dbs:
                return set(excluded_dbs.split(','))
            return ScraperConfig.DEFAULT_EXCLUDED_DBS
        except Exception as e:
            logger.warning(f"Error processing excluded databases, using defaults: {e}")
            return ScraperConfig.DEFAULT_EXCLUDED_DBS

    @staticmethod
    def get_excluded_users() -> Set[str]:
        """Get list of users to exclude from monitoring"""
        try:
            excluded_users = config.get('MYSQL_EXCLUDED_USERS')
            if excluded_users:
                return set(excluded_users.split(','))
            return ScraperConfig.DEFAULT_EXCLUDED_USERS
        except Exception as e:
            logger.warning(f"Error processing excluded users, using defaults: {e}")
            return ScraperConfig.DEFAULT_EXCLUDED_USERS

    @staticmethod
    def get_monitoring_interval() -> int:
        """Get monitoring interval in seconds"""
        try:
            interval = config.get('MYSQL_MONITORING_INTERVAL', ScraperConfig.DEFAULT_MONITORING_INTERVAL)
            return int(interval)
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid monitoring interval, using default value {ScraperConfig.DEFAULT_MONITORING_INTERVAL}: {e}")
            return ScraperConfig.DEFAULT_MONITORING_INTERVAL

    @staticmethod
    def get_scraper_settings() -> Dict[str, Any]:
        """Get all scraper related settings"""
        return {
            'exec_time': ScraperConfig.get_mysql_exec_time(),
            'excluded_dbs': ScraperConfig.get_excluded_databases(),
            'excluded_users': ScraperConfig.get_excluded_users(),
            'monitoring_interval': ScraperConfig.get_monitoring_interval(),
        }


# Initialize settings
scraper_settings = ScraperConfig.get_scraper_settings()