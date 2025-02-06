"""
Base configuration manager for handling environment variables and AWS Secrets Manager.
"""

import os
import json
import logging
from .base_path import get_project_root
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

        # 2. 시스템 환경변수 저장 (우선순위 높음)
        self._system_env = dict(os.environ)
        logger.debug(f"Available environment variables: {list(self._system_env.keys())}")

        # 3. 환경 설정
        self._environment = self._system_env.get('APP_ENV', 'dev').lower()
        if self._environment not in ['dev', 'prd']:
            logger.warning(f"Invalid APP_ENV: {self._environment}. Using 'dev' as default")
            self._environment = 'dev'

        # 4. APP_SECRET_NAME이 있으면 시크릿 매니저 로드 시도
        self._try_load_secrets()

    def _try_load_secrets(self) -> None:
        """시크릿 매니저에서 설정 로드 시도"""
        secret_name = self.get('APP_SECRET_NAME')
        if not secret_name:
            logger.debug("APP_SECRET_NAME not set, skipping secrets load")
            return

        # AWS 자격 증명 확인
        try:
            session = boto3.Session()
            sts = session.client('sts')
            sts.get_caller_identity()
        except Exception as e:
            logger.warning(f"AWS credentials not found or invalid, skipping secrets load: {e}")
            return

        logger.info(f"Found APP_SECRET_NAME: {secret_name}, attempting to load secrets")
        try:
            secrets = self._load_aws_secrets(secret_name)
            if secrets:
                # 기존 환경변수를 유지하면서 시크릿 값 추가
                for key, value in secrets.items():
                    if key not in self._system_env:  # 기존 환경변수가 없는 경우에만 추가
                        if value is not None:
                            self._config[key] = str(value)
                            logger.debug(f"Loaded secret: {key}")
            else:
                logger.warning("No secrets loaded from AWS Secrets Manager")

        except Exception as e:
            logger.error(f"Failed to load secrets: {e}")
            return  # 시크릿 로드 실패 시 계속 진행

    def _load_env_file(self) -> None:
        """환경 설정을 위한 .env 파일 로드"""
        try:
            env_path = get_project_root() / '.env'
            logger.debug(f"Looking for .env file at: {env_path}")

            if env_path.exists():
                logger.debug("Found .env file, loading...")
                load_dotenv(env_path, override=False)  # 시스템 환경변수 우선
                logger.debug("Loaded .env file successfully")
            else:
                logger.warning(f".env file not found at {env_path}")
        except Exception as e:
            logger.error(f"Error loading .env file: {e}")

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
        설정값 조회 (우선순위: 시스템 환경변수 > .env 파일 > 시크릿 매니저 > 기본값)
        """
        return (
            self._system_env.get(key) or  # Docker run -e 또는 export로 설정된 환경변수
            os.getenv(key) or             # .env 파일에서 로드된 환경변수
            self._config.get(key) or      # 시크릿 매니저에서 로드된 값
            default
        )

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