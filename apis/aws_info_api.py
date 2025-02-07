"""
AWS Info API
현재 AWS 계정 정보와 리전 정보를 제공하는 API
"""

import logging
from fastapi import FastAPI, HTTPException
import boto3
from typing import Dict, Any

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="AWS Account & Region Info")


@app.get("/aws-info", response_model=Dict[str, str])
async def get_aws_info() -> Dict[str, str]:
    """
    현재 AWS 계정 번호와 리전 정보를 반환

    Returns:
        Dict[str, str]: AWS 계정 정보
        {
            "account": str,  # AWS 계정 번호
            "region": str    # AWS 리전
        }
    """
    try:
        session = boto3.Session()
        sts = session.client('sts')
        caller_identity = sts.get_caller_identity()

        info = {
            "account": caller_identity["Account"],
            "region": session.region_name or "ap-northeast-2"
        }

        logger.info(f"AWS 계정 정보 조회 성공: {info}")
        return info

    except Exception as e:
        logger.error(f"AWS 정보 조회 실패: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"AWS 정보를 가져오는데 실패했습니다: {str(e)}"
        )