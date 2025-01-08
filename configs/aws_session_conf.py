"""
AWS Session configuration manager.
Handles AWS SSO and session related configurations.
"""

import os
import logging
from configs.base_config import config
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class AWSSSOSettings:
    """AWS SSO 설정 데이터클래스"""
    sso_start_url: str
    sso_region: str
    default_region: str
    role_name: str


class AWSSessionConfig:
    """AWS Session 설정 관리자"""

    DEFAULT_REGION = 'ap-northeast-2'

    def __init__(self):
        self._sso_settings = self._load_sso_settings()

    @staticmethod
    def _load_sso_settings() -> AWSSSOSettings:
        """SSO 설정 로드"""
        return AWSSSOSettings(
            sso_start_url=config.get('AWS_SSO_START_URL', ''),
            sso_region=config.get('AWS_SSO_REGION', AWSSessionConfig.DEFAULT_REGION),
            default_region=config.get('AWS_DEFAULT_REGION', AWSSessionConfig.DEFAULT_REGION),
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

    def get_aws_region(self) -> str:
        """현재 AWS 리전 반환

        우선순위:
        1. AWS_REGION 환경변수
        2. AWS_DEFAULT_REGION 환경변수
        3. SSO 설정의 default_region
        4. 기본값 (ap-northeast-2)

        Returns:
            str: AWS 리전 코드
        """
        return (
            config.get('AWS_REGION') or
            config.get('AWS_DEFAULT_REGION') or
            self.sso_settings.default_region or
            self.DEFAULT_REGION
        )


# Global instance
aws_session_config = AWSSessionConfig()