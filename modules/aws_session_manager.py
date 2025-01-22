"""
AWS Session Manager
로컬 환경에서는 SSO를, EC2/EKS 환경에서는 IAM을 사용하여 AWS 세션을 관리
"""

import boto3
import botocore.config
from typing import Optional, Dict, Any
import logging
from configs.aws_session_conf import aws_session_config
from botocore.exceptions import NoCredentialsError, ClientError, ProfileNotFound

logger = logging.getLogger(__name__)

class AWSSessionManager:
    """AWS Session Manager"""
    DEFAULT_PROFILE = "AdministratorAccess-488659748805"

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
                logger.info("Using EKS/EC2 IAM role")
            else:
                # 사용 가능한 프로파일 확인
                available_profiles = boto3.Session().available_profiles
                logger.info(f"Available AWS profiles: {', '.join(available_profiles)}")

                # 로컬 환경에서는 지정된 프로파일 사용
                self.session = boto3.Session(
                    profile_name=self.DEFAULT_PROFILE,
                    region_name=aws_session_config.sso_settings.default_region
                )
                logger.info(f"Using AWS profile: {self.DEFAULT_PROFILE}")

            # 세션 유효성 검증
            identity = self.session.client('sts').get_caller_identity()
            logger.info(f"AWS session initialized successfully (Account: {identity['Account']})")

        except ProfileNotFound:
            available_profiles = boto3.Session().available_profiles
            error_msg = f"""
                Profile '{self.DEFAULT_PROFILE}' not found
                Available profiles: {', '.join(available_profiles)}
                Please ensure you have the correct profile name or run 'aws sso login'
                """
            logger.error(error_msg)
            raise
        except (NoCredentialsError, ClientError) as e:
            logger.error(f"AWS credentials not found or invalid. Please run 'aws sso login': {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize AWS session: {e}")
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


# Export the class and instance explicitly
__all__ = ['AWSSessionManager', 'aws_session']

# Global instance
aws_session = AWSSessionManager()