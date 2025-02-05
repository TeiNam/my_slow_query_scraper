"""
AWS Session Manager
환경변수 -> 시크릿 매니저 -> SSO -> IAM Role 순서로 인증을 시도하여 AWS 세션을 관리
"""

import boto3
import botocore.config
from typing import Optional, Dict, Any
import logging
from configs.aws_session_conf import aws_session_config
from botocore.exceptions import NoCredentialsError, ClientError, ProfileNotFound

logger = logging.getLogger(__name__)

class AWSSessionManager:
    """AWS 세션 관리자"""

    def __init__(self):
        self.session: Optional[boto3.Session] = None
        self._initialize_session()

    def _initialize_session(self) -> None:
        """
        다음 우선순위로 세션 초기화 시도:
        1. 환경변수 credentials
        2. 시크릿 매니저
        3. SSO
        4. IAM Role (EKS/EC2)
        """
        try:
            # 1. 환경변수 credentials 확인
            if aws_session_config.settings.credentials:
                self._initialize_with_credentials()
                logger.info("환경변수 credentials로 세션이 초기화되었습니다.")
                return

            # 2. 시크릿 매니저 시도
            if self._try_secret_manager_session():
                logger.info("시크릿 매니저 credentials로 세션이 초기화되었습니다.")
                return

            # 3. SSO 설정 확인
            if aws_session_config.settings.sso_settings and not aws_session_config.is_eks():
                self._initialize_with_sso()
                logger.info("SSO 프로필로 세션이 초기화되었습니다.")
                return

            # 4. IAM Role 사용 (EKS/EC2)
            self._initialize_with_iam_role()
            logger.info("IAM Role로 세션이 초기화되었습니다.")

            # 세션 유효성 검증
            identity = self.session.client('sts').get_caller_identity()
            logger.info(f"AWS 세션 초기화 완료 (Account: {identity['Account']})")

        except Exception as e:
            logger.error(f"AWS 세션 초기화 실패: {str(e)}")
            raise

    def _initialize_with_credentials(self) -> None:
        """환경변수 credentials로 세션 초기화"""
        creds = aws_session_config.settings.credentials
        self.session = boto3.Session(
            aws_access_key_id=creds.access_key_id,
            aws_secret_access_key=creds.secret_access_key,
            aws_session_token=creds.session_token,
            region_name=aws_session_config.settings.region
        )

    def _try_secret_manager_session(self) -> bool:
        """시크릿 매니저에서 credentials 조회 시도"""
        try:
            temp_session = boto3.Session(region_name=aws_session_config.settings.region)
            secrets = temp_session.client('secretsmanager')
            response = secrets.get_secret_value(SecretId='dev/aws/credentials')

            import json
            secret = json.loads(response['SecretString'])

            self.session = boto3.Session(
                aws_access_key_id=secret['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=secret['AWS_SECRET_ACCESS_KEY'],
                aws_session_token=secret.get('AWS_SESSION_TOKEN'),
                region_name=aws_session_config.settings.region
            )
            return True

        except Exception as e:
            logger.debug(f"시크릿 매니저 접근 실패: {str(e)}")
            return False

    def _initialize_with_sso(self) -> None:
        """SSO 프로필로 세션 초기화"""
        sso_settings = aws_session_config.settings.sso_settings
        available_profiles = boto3.Session().available_profiles
        logger.debug(f"사용 가능한 AWS 프로필: {', '.join(available_profiles)}")

        self.session = boto3.Session(
            profile_name=sso_settings.default_profile,
            region_name=aws_session_config.settings.region
        )

    def _initialize_with_iam_role(self) -> None:
        """IAM Role로 세션 초기화"""
        self.session = boto3.Session(
            region_name=aws_session_config.settings.region
        )

    def get_client(self, service_name: str, region: Optional[str] = None) -> Any:
        """AWS 서비스 클라이언트 반환"""
        if not self.session:
            raise ValueError("AWS session이 초기화되지 않았습니다")

        config = botocore.config.Config(
            max_pool_connections=50,
            retries=dict(max_attempts=3),
            connect_timeout=5,
            read_timeout=60,
            tcp_keepalive=True
        )

        return self.session.client(
            service_name,
            region_name=region or aws_session_config.settings.region,
            config=config
        )

    def get_resource(self, service_name: str, region: Optional[str] = None) -> Any:
        """AWS 서비스 리소스 반환"""
        if not self.session:
            raise ValueError("AWS session이 초기화되지 않았습니다")

        return self.session.resource(
            service_name,
            region_name=region or aws_session_config.settings.region
        )

# Global instance
aws_session = AWSSessionManager()