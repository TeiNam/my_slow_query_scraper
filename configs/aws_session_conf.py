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
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class AWSCredentials:
    """AWS 인증 정보 데이터클래스"""
    access_key_id: str
    secret_access_key: str
    session_token: Optional[str] = None

@dataclass(frozen=True)
class AWSSSOSettings:
    """AWS SSO 설정 데이터클래스"""
    sso_start_url: str
    sso_region: str
    role_name: str
    default_profile: str

@dataclass(frozen=True)
class AWSSettings:
    """AWS 통합 설정 데이터클래스"""
    region: str
    credentials: Optional[AWSCredentials] = None
    sso_settings: Optional[AWSSSOSettings] = None
    profile: Optional[str] = None
    account_id: Optional[str] = None

class ConfigurationManager:
    """Configuration manager that handles both local and AWS environments."""

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._aws_settings: Optional[AWSSettings] = None

        # 1. 시스템 환경변수 저장
        self._system_env = dict(os.environ)

        # 2. .env 파일 로드 (시스템 환경변수 우선)
        self._load_env_file()

        # 3. 환경 설정
        self._environment = self.get('APP_ENV', 'dev').lower()
        if self._environment not in ['dev', 'prd']:
            logger.warning(f"Invalid APP_ENV: {self._environment}. Using 'dev' as default")
            self._environment = 'dev'

        # 4. AWS 기본 설정 로드
        self._load_aws_settings()

        # 5. AWS Secrets 로드 (프로덕션인 경우)
        self._init_config()

    def _load_aws_settings(self) -> None:
        """AWS 설정 로드"""
        credentials = self._load_aws_credentials()
        sso_settings = self._load_sso_settings()

        self._aws_settings = AWSSettings(
            region=self.get_aws_region(),
            credentials=credentials,
            sso_settings=sso_settings,
            profile=self.get('AWS_PROFILE'),
            account_id=self.get('AWS_ACCOUNT_ID')
        )

    def _load_aws_credentials(self) -> Optional[AWSCredentials]:
        """AWS 인증 정보 로드"""
        required_keys = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
        if all(self.get(key) for key in required_keys):
            return AWSCredentials(
                access_key_id=self.get('AWS_ACCESS_KEY_ID'),
                secret_access_key=self.get('AWS_SECRET_ACCESS_KEY'),
                session_token=self.get('AWS_SESSION_TOKEN')
            )
        return None

    def _load_sso_settings(self) -> Optional[AWSSSOSettings]:
        """SSO 설정 로드"""
        if self.get('AWS_SSO_START_URL'):
            return AWSSSOSettings(
                sso_start_url=self.get('AWS_SSO_START_URL', ''),
                sso_region=self.get('AWS_SSO_REGION', self.get_aws_region()),
                role_name=self.get('AWS_ROLE_NAME', self.get('AWS_DEFAULT_PROFILE', 'AdministratorAccess')),
                default_profile=self.get('AWS_DEFAULT_PROFILE', 'AdministratorAccess')
            )
        return None

    def get_aws_region(self) -> str:
        """AWS 리전 반환"""
        return (
            self.get('AWS_REGION') or
            self.get('AWS_DEFAULT_REGION') or
            'ap-northeast-2'
        )

    def get(self, key: str, default: Any = None) -> Any:
        """
        설정값 조회 (우선순위: 시스템 환경변수 > AWS Secrets > .env 파일 > 기본값)
        """
        return (
            self._system_env.get(key) or
            self._config.get(key) or
            default
        )

    def _load_env_file(self) -> None:
        """환경 설정을 위한 .env 파일 로드"""
        env_path = get_project_root() / '.env'
        if env_path.exists():
            load_dotenv(env_path, override=False)  # 시스템 환경변수 우선
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

    @property
    def aws_settings(self) -> AWSSettings:
        """AWS 설정 반환"""
        return self._aws_settings

    def __getattr__(self, name: str) -> Any:
        """속성 스타일 접근 지원"""
        value = self.get(name)
        if value is not None:
            return value
        raise AttributeError(f"Configuration has no attribute '{name}'")

# Global configuration instance
config = ConfigurationManager()