"""
Base configuration manager for handling environment variables and AWS Secrets Manager.
"""

import os
import json
import logging
from .base_path import get_project_root  # 상대 경로로 수정
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class ConfigurationManager:
    """Configuration manager that handles both local and AWS environments."""

    def __init__(self):
        self._config: Dict[str, Any] = {}

        logger.debug("Initializing ConfigurationManager...")

        # 1. .env 파일 로드
        self._load_env_file()

        # 2. 시스템 환경변수 저장
        self._system_env = dict(os.environ)
        logger.debug(f"Available environment variables: {list(self._system_env.keys())}")

        # 3. 환경 설정
        self._environment = self._system_env.get('APP_ENV', 'dev').lower()
        if self._environment not in ['dev', 'prd']:
            logger.warning(f"Invalid APP_ENV: {self._environment}. Using 'dev' as default")
            self._environment = 'dev'

        # 4. 중요 환경변수 확인
        self._check_critical_vars()

    def _check_critical_vars(self) -> None:
        """중요 환경변수 존재 여부 확인"""
        critical_vars = ['MGMT_USER', 'MGMT_USER_PASS']
        for var in critical_vars:
            value = self.get(var)
            if value:
                logger.debug(f"{var} is set with length: {len(value)}")
            else:
                logger.warning(f"{var} is not set!")

    def _load_env_file(self) -> None:
        """환경 설정을 위한 .env 파일 로드"""
        try:
            env_path = get_project_root() / '.env'
            logger.debug(f"Looking for .env file at: {env_path}")

            if env_path.exists():
                logger.debug("Found .env file, loading...")
                load_dotenv(env_path, override=True)  # 시스템 환경변수보다 .env 우선
                logger.debug("Loaded .env file successfully")
            else:
                logger.warning(f".env file not found at {env_path}")
        except Exception as e:
            logger.error(f"Error loading .env file: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """
        설정값 조회 (우선순위: 시스템 환경변수 > .env 파일 > 기본값)
        """
        value = self._system_env.get(key) or os.getenv(key) or self._config.get(key) or default
        return value

    def is_development(self) -> bool:
        """개발 환경 여부 확인"""
        return self._environment == 'dev'

    def is_production(self) -> bool:
        """운영 환경 여부 확인"""
        return self._environment == 'prd'

    def get_environment(self) -> str:
        """현재 환경 반환"""
        return self._environment

    def __getattr__(self, name: str) -> Any:
        """속성 스타일 접근 지원"""
        value = self.get(name)
        if value is not None:
            return value
        raise AttributeError(f"Configuration has no attribute '{name}'")

# Global configuration instance
config = ConfigurationManager()