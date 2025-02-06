"""
AWS Session configuration manager.
"""

import os
import json
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any
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

    def __init__(self):
        self.settings = self._load_settings()

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
                sso_region=config.get('AWS_SSO_REGION', self.get_aws_region()),
                role_name=config.get('AWS_ROLE_NAME', config.get('AWS_DEFAULT_PROFILE', 'AdministratorAccess')),
                default_profile=config.get('AWS_DEFAULT_PROFILE', 'AdministratorAccess')
            )
        return None

    def get_aws_region(self) -> str:
        """현재 AWS 리전 반환"""
        return (
            config.get('AWS_REGION') or
            config.get('AWS_DEFAULT_REGION') or
            'ap-northeast-2'
        )

    @staticmethod
    def is_eks() -> bool:
        """EKS 환경 여부 확인"""
        return os.path.exists("/var/run/secrets/kubernetes.io")

# 전역 인스턴스
aws_session_config = AWSSessionConfig()