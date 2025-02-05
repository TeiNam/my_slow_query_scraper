"""
AWS Session configuration manager.
Handles AWS configuration with multiple authentication methods.
"""

import os
import json
import logging
from dataclasses import dataclass
from typing import Optional
from configs.base_config import config

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

class AWSSessionConfig:
    """AWS Session 설정 관리자"""

    DEFAULT_REGION = 'ap-northeast-2'
    DEFAULT_PROFILE = 'AdministratorAccess'

    def __init__(self):
        self._settings = self._load_settings()

    def _load_settings(self) -> AWSSettings:
        """AWS 설정 로드"""
        return AWSSettings(
            region=self.get_aws_region(),
            credentials=self._load_env_credentials(),
            sso_settings=self._load_sso_settings(),
            profile=config.get('AWS_PROFILE'),
            account_id=config.get('AWS_ACCOUNT_ID')
        )

    def _load_env_credentials(self) -> Optional[AWSCredentials]:
        """환경변수에서 AWS 인증 정보 로드"""
        if all(key in os.environ for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']):
            return AWSCredentials(
                access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                session_token=os.environ.get('AWS_SESSION_TOKEN')
            )
        return None

    def _load_sso_settings(self) -> Optional[AWSSSOSettings]:
        """SSO 설정 로드"""
        if config.get('AWS_SSO_START_URL'):
            return AWSSSOSettings(
                sso_start_url=config.get('AWS_SSO_START_URL', ''),
                sso_region=config.get('AWS_SSO_REGION', self.DEFAULT_REGION),
                role_name=config.get('AWS_ROLE_NAME', self.DEFAULT_PROFILE),
                default_profile=config.get('AWS_DEFAULT_PROFILE', self.DEFAULT_PROFILE)
            )
        return None

    @staticmethod
    def is_eks() -> bool:
        """EKS 환경 여부 확인"""
        return os.path.exists("/var/run/secrets/kubernetes.io")

    def get_aws_region(self) -> str:
        """현재 AWS 리전 반환

        우선순위:
        1. AWS_REGION 환경변수
        2. AWS_DEFAULT_REGION 환경변수
        3. 기본값 (ap-northeast-2)

        Returns:
            str: AWS 리전 코드
        """
        return (
            config.get('AWS_REGION') or
            config.get('AWS_DEFAULT_REGION') or
            self.DEFAULT_REGION
        )

    @property
    def settings(self) -> AWSSettings:
        """통합 AWS 설정 반환"""
        return self._settings

# Global instance
aws_session_config = AWSSessionConfig()