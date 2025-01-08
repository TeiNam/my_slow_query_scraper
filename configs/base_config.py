"""
Base configuration manager for handling environment variables and AWS Secrets Manager.
Provides basic functionality for both local development (.env) and AWS environments.
"""

import os
import json
import logging
from base_path import get_project_root
from typing import Dict, Any
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class ConfigurationManager:
    """Configuration manager that handles both local and AWS environments."""

    def __init__(self):
        self._config: Dict[str, Any] = {}

        # base_path를 사용하여 .env 파일 로드
        self._load_env_file()
        self._environment = os.getenv('APP_ENV', 'dev').lower()

        if self._environment not in ['dev', 'prd']:
            logger.warning(f"Invalid APP_ENV: {self._environment}. Using 'dev' as default")
            self._environment = 'dev'

        self._init_config()

    def is_production(self) -> bool:
        """현재 환경이 production인지 확인"""
        return self._environment == 'prd'

    def is_development(self) -> bool:
        """현재 환경이 development인지 확인"""
        return self._environment == 'dev'

    @staticmethod
    def _load_aws_secrets(secret_name: str) -> Dict[str, Any]:
        """AWS Secrets Manager에서 시크릿 로드"""
        try:
            region = os.getenv('AWS_DEFAULT_REGION', 'ap-northeast-2')

            session = boto3.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region
            )

            response = client.get_secret_value(SecretId=secret_name)
            if 'SecretString' in response:
                return json.loads(response['SecretString'])

            logger.error("No SecretString found in the response")
            return {}

        except ClientError as e:
            logger.error(f"Failed to load AWS secrets: {str(e)}")
            return {}

    def _load_env_file(self) -> None:
        """환경 설정을 위한 .env 파일 로드"""
        env_path = get_project_root() / '.env'
        if env_path.exists():
            load_dotenv(env_path, override=True)
            logger.debug(f"Loaded .env file from {env_path}")
        else:
            logger.warning(f".env file not found at {env_path}")

    def _init_config(self) -> None:
        """APP_ENV에 따른 설정 초기화"""
        if self.is_production():
            # Production 환경에서는 Secrets Manager 사용
            secret_name = os.getenv('APP_SECRET_NAME', 'slow-query-collector-secret')
            self._config = self._load_aws_secrets(secret_name)
            logger.info("Production environment: Loaded configuration from AWS Secrets Manager")
        else:
            # Development 환경에서는 이미 로드된 .env 파일 사용
            logger.info("Development environment: Using configuration from .env file")

    def get(self, key: str, default: Any = None) -> Any:
        """설정값 조회"""
        return os.getenv(key) or self._config.get(key, default)

    def __getattr__(self, name: str) -> Any:
        """속성 스타일 접근 지원"""
        value = self.get(name)
        if value is not None:
            return value
        raise AttributeError(f"Configuration has no attribute '{name}'")


# Global configuration instance
config = ConfigurationManager()