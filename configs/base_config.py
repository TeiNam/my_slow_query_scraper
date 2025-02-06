"""
Base configuration manager for handling environment variables and AWS Secrets Manager.
"""

import os
import json
import logging
from base_path import get_project_root
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class ConfigurationManager:
    """Configuration manager that handles both local and AWS environments."""

    def __init__(self):
        self._config: Dict[str, Any] = {}

        # 1. 먼저 시스템 환경변수 저장
        self._system_env = dict(os.environ)

        # 2. .env 파일 로드 (있는 경우에만)
        self._load_env_file()

        # 3. 환경 설정
        self._environment = self.get('APP_ENV', 'dev').lower()
        if self._environment not in ['dev', 'prd']:
            logger.warning(f"Invalid APP_ENV: {self._environment}. Using 'dev' as default")
            self._environment = 'dev'

        # 4. AWS Secrets 로드 (프로덕션인 경우)
        self._init_config()

    def get(self, key: str, default: Any = None) -> Any:
        """
        설정값 조회 (우선순위: 시스템 환경변수 > AWS Secrets > .env 파일 > 기본값)
        """
        # 1. 시스템 환경변수 확인
        value = self._system_env.get(key)
        if value is not None:
            return value

        # 2. AWS Secrets/설정된 값 확인
        value = self._config.get(key)
        if value is not None:
            return value

        # 3. 기본값 반환
        return default

    def _load_env_file(self) -> None:
        """환경 설정을 위한 .env 파일 로드"""
        env_path = get_project_root() / '.env'
        if env_path.exists():
            # .env 파일은 기존 환경변수를 덮어쓰지 않도록 설정
            load_dotenv(env_path, override=False)
            logger.debug(f"Loaded .env file from {env_path}")
        else:
            logger.warning(f".env file not found at {env_path}")

    def _init_config(self) -> None:
        """APP_ENV에 따른 설정 초기화"""
        if self.is_production():
            secret_name = self.get('APP_SECRET_NAME', 'slow-query-collector-secret')
            self._config.update(self._load_aws_secrets(secret_name))
            logger.info("Production environment: Loaded configuration from AWS Secrets Manager")
        else:
            logger.info("Development environment: Using configuration from environment")

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

    def is_production(self) -> bool:
        """현재 환경이 production인지 확인"""
        return self._environment == 'prd'

    def is_development(self) -> bool:
        """현재 환경이 development인지 확인"""
        return self._environment == 'dev'

# Global configuration instance
config = ConfigurationManager()