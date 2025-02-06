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

        # 1. 시스템 환경변수 저장
        self._system_env = dict(os.environ)

        # 2. .env 파일 로드 (시스템 환경변수 우선)
        self._load_env_file()

        # 3. 환경 설정 (docker 환경에서는 기본적으로 dev로 설정)
        self._environment = self._system_env.get('APP_ENV', 'dev').lower()
        if self._environment not in ['dev', 'prd']:
            logger.warning(f"Invalid APP_ENV: {self._environment}. Using 'dev' as default")
            self._environment = 'dev'

        # 4. AWS Secrets는 명시적으로 요청된 경우에만 로드
        if self._system_env.get('USE_AWS_SECRETS') == 'true':
            self._init_aws_secrets()
        else:
            logger.info("AWS Secrets disabled. Using environment variables only.")

    def _load_env_file(self) -> None:
        """환경 설정을 위한 .env 파일 로드"""
        env_path = get_project_root() / '.env'
        if env_path.exists():
            load_dotenv(env_path, override=False)  # 시스템 환경변수 우선
            logger.debug(f"Loaded .env file from {env_path}")
        else:
            logger.warning(f".env file not found at {env_path}")

    def _init_aws_secrets(self) -> None:
        """AWS Secrets Manager 초기화"""
        if self.is_production():
            try:
                secret_name = self.get('APP_SECRET_NAME', 'slow-query-collector-secret')
                self._config.update(self._load_aws_secrets(secret_name))
                logger.info("Production environment: Loaded configuration from AWS Secrets Manager")
            except Exception as e:
                logger.error(f"Failed to load AWS secrets: {e}")
                logger.info("Falling back to environment variables")

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
            logger.error(f"Failed to load AWS secrets: {e}")
            return {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        설정값 조회 (우선순위: 시스템 환경변수 > AWS Secrets > .env 파일 > 기본값)
        """
        return (
            self._system_env.get(key) or
            self._config.get(key) or
            default
        )

    def is_production(self) -> bool:
        """현재 환경이 production인지 확인"""
        return self._environment == 'prd'

    def is_development(self) -> bool:
        """현재 환경이 development인지 확인"""
        return self._environment == 'dev'

    def __getattr__(self, name: str) -> Any:
        """속성 스타일 접근 지원"""
        value = self.get(name)
        if value is not None:
            return value
        raise AttributeError(f"Configuration has no attribute '{name}'")

# Global configuration instance
config = ConfigurationManager()