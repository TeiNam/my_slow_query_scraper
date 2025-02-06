"""
AWS Session Manager
개발 환경: SSO -> 환경변수
운영 환경: IAM Role -> 시크릿 매니저 -> 환경변수
"""

import boto3
import botocore.config
from typing import Optional, Dict, Any
import logging
from configs.aws_session_conf import aws_session_config
from botocore.exceptions import NoCredentialsError, ClientError, ProfileNotFound
from configs.base_config import config

logger = logging.getLogger(__name__)

class AWSSessionManager:
    """AWS 세션 관리자"""

    def __init__(self):
        self.session: Optional[boto3.Session] = None
        self._initialize_session()

    def _initialize_session(self) -> None:
        """
        환경에 따른 세션 초기화:
        개발 환경:
            1. SSO
            2. 환경변수 credentials
        운영 환경:
            1. IAM Role
            2. 시크릿 매니저의 환경변수
            3. .env 파일의 환경변수
        """
        try:
            if config.is_development():
                self._initialize_development()
            else:
                self._initialize_production()

            # 세션 유효성 검증
            if self.session:
                identity = self.session.client('sts').get_caller_identity()
                logger.info(f"AWS 세션 초기화 완료 (Account: {identity['Account']})")
            else:
                raise ValueError("세션 초기화 실패")

        except Exception as e:
            logger.error(f"AWS 세션 초기화 실패: {str(e)}")
            raise

    def _initialize_production(self) -> None:
        """운영 환경 세션 초기화"""
        try:
            # 1. IAM Role 시도
            self._initialize_with_iam_role()
            logger.info("IAM Role로 세션이 초기화되었습니다.")

            # 2. 시크릿 매니저에서 환경변수 로드
            if self._load_env_from_secrets():
                logger.info("시크릿 매니저에서 환경변수를 로드했습니다.")
            else:
                logger.info("시크릿 매니저에서 환경변수를 찾을 수 없어 .env 파일의 값을 사용합니다.")

        except Exception as e:
            logger.error(f"운영 환경 세션 초기화 실패: {e}")
            raise

    def _load_env_from_secrets(self) -> bool:
        """시크릿 매니저에서 환경변수 로드"""
        try:
            if not self.session:
                return False

            secrets = self.session.client('secretsmanager')
            response = secrets.get_secret_value(SecretId='slow-query-collector-secret')

            import json
            secret = json.loads(response['SecretString'])

            # 환경변수 업데이트
            if isinstance(secret, dict):
                for key, value in secret.items():
                    if value is not None:
                        os.environ[key] = str(value)
                return True

        except Exception as e:
            logger.debug(f"시크릿 매니저에서 환경변수 로드 실패: {str(e)}")
            return False

        return False

    def _initialize_development(self) -> None:
        """개발 환경 세션 초기화"""
        # 1. SSO 시도
        if aws_session_config.settings.sso_settings:
            try:
                self._initialize_with_sso()
                logger.info("SSO 프로필로 세션이 초기화되었습니다.")
                return
            except Exception as e:
                logger.debug(f"SSO 초기화 실패: {e}")

        # 2. 환경변수 credentials 시도
        if aws_session_config.settings.credentials:
            self._initialize_with_credentials()
            logger.info("환경변수 credentials로 세션이 초기화되었습니다.")
            return

        raise ValueError("개발 환경에서 유효한 AWS 인증 방식을 찾을 수 없습니다.")

    def _initialize_with_credentials(self) -> None:
        """환경변수 credentials로 세션 초기화"""
        creds = aws_session_config.settings.credentials
        self.session = boto3.Session(
            aws_access_key_id=creds.access_key_id,
            aws_secret_access_key=creds.secret_access_key,
            aws_session_token=creds.session_token,
            region_name=aws_session_config.settings.region
        )

    def _initialize_with_sso(self) -> None:
        """SSO 프로필로 세션 초기화"""
        sso_settings = aws_session_config.settings.sso_settings
        if not sso_settings:
            raise ValueError("SSO 설정이 없습니다.")

        available_profiles = boto3.Session().available_profiles
        logger.debug(f"사용 가능한 AWS 프로필: {', '.join(available_profiles)}")

        if sso_settings.default_profile not in available_profiles:
            raise ValueError(f"프로필을 찾을 수 없습니다: {sso_settings.default_profile}")

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