"""
AWS Session Manager
로컬 환경에서는 SSO를, EC2/EKS 환경에서는 IAM을 사용하여 AWS 세션을 관리
"""

import boto3
import botocore.config
from typing import Optional, Dict, Any
import subprocess
import logging
from configs.aws_session_conf import aws_session_config
from botocore.exceptions import NoCredentialsError, ClientError

logger = logging.getLogger(__name__)


class AWSSessionManager:
    def __init__(self):
        self.session: Optional[boto3.Session] = None
        self._initialize_session()

    def _initialize_session(self) -> None:
        """환경에 따른 AWS 세션 초기화"""
        try:
            if aws_session_config.is_eks():
                # EKS/EC2 환경에서는 IAM 역할 사용
                self.session = boto3.Session(
                    region_name=aws_session_config.sso_settings.default_region
                )
            else:
                # 로컬 환경에서는 SSO 사용
                self._ensure_sso_login()
                self.session = boto3.Session(
                    region_name=aws_session_config.sso_settings.default_region
                )

            # 세션 유효성 검증
            self.session.client('sts').get_caller_identity()
            logger.info("AWS session initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize AWS session: {e}")
            raise

    @staticmethod
    def _ensure_sso_login() -> None:
        """SSO 로그인 상태 확인 및 필요시 로그인 수행"""
        try:
            # 현재 세션으로 권한 확인 시도
            session = boto3.Session(
                region_name=aws_session_config.sso_settings.default_region
            )
            session.client('sts').get_caller_identity()
            logger.debug("Using existing SSO credentials")

        except (NoCredentialsError, ClientError) as e:
            # 권한 없음 - SSO 로그인 필요
            logger.info(f"AWS SSO login required: {str(e)}")
            try:
                subprocess.run(
                    ["aws", "sso", "login"],
                    check=True
                )
                logger.info("AWS SSO login successful")
            except subprocess.CalledProcessError as e:
                logger.error(f"AWS SSO login failed: {str(e)}")
                raise

    def get_client(self, service_name: str, region: Optional[str] = None) -> Any:
        """AWS 서비스 클라이언트 반환"""
        if not self.session:
            raise ValueError("AWS session is not initialized")

        config = botocore.config.Config(
            max_pool_connections=50,
            retries=dict(
                max_attempts=3
            ),
            connect_timeout=5,
            read_timeout=60,
            tcp_keepalive=True
        )

        return self.session.client(
            service_name,
            region_name=region or aws_session_config.sso_settings.default_region,
            config=config
        )

    def get_resource(self, service_name: str, region: Optional[str] = None) -> Any:
        """AWS 서비스 리소스 반환"""
        if not self.session:
            raise ValueError("AWS session is not initialized")

        return self.session.resource(
            service_name,
            region_name=region or aws_session_config.sso_settings.default_region
        )


# Global instance
aws_session = AWSSessionManager()