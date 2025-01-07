"""
AWS Session configuration manager.
Handles AWS SSO and session related configurations.
"""

import os
from configs.base_config import config
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AWSSSOSettings:
    """AWS SSO 설정 데이터클래스"""
    sso_start_url: str
    sso_region: str
    default_region: str
    role_name: str


class AWSSessionConfig:
    """AWS Session 설정 관리자"""

    def __init__(self):
        self._sso_settings = self._load_sso_settings()

    @staticmethod
    def _load_sso_settings() -> AWSSSOSettings:
        """SSO 설정 로드"""
        return AWSSSOSettings(
            sso_start_url=config.get('AWS_SSO_START_URL'),
            sso_region=config.get('AWS_SSO_REGION', 'ap-northeast-2'),
            default_region=config.get('AWS_DEFAULT_REGION', 'ap-northeast-2'),
            role_name=config.get('AWS_ROLE_NAME', 'AdministratorAccess')
        )

    @staticmethod
    def is_eks() -> bool:
        """EKS 환경 여부 확인"""
        return os.path.exists("/var/run/secrets/kubernetes.io")

    @property
    def sso_settings(self) -> AWSSSOSettings:
        """SSO 설정 반환"""
        return self._sso_settings

    @staticmethod
    def get_aws_account_id() -> Optional[str]:
        """현재 AWS 계정 ID 반환"""
        return config.get('AWS_ACCOUNT_ID')


# Global instance
aws_session_config = AWSSessionConfig()