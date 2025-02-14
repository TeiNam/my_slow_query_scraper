"""
AWS Info API
현재 AWS 계정 정보와 리전 정보를 제공하는 API
"""

import logging
from fastapi import FastAPI, HTTPException
import boto3
from typing import Dict, Any
from modules.mongodb_connector import mongodb
from configs.mongo_conf import mongo_settings

# 로깅 설정
logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="AWS Account & Region Info", tags=["AWS Info"])

AWS_INFO_COLLECTION = mongo_settings.MONGO_AWS_INFO_COLLECTION

@app.post("/aws/collect", response_model=Dict[str, str],tags=["AWS Info"])
async def collect_aws_info() -> Dict[str, str]:
   """
   현재 AWS 계정 정보를 수집하여 MongoDB에 저장
   """
   try:
       session = boto3.Session()
       sts = session.client('sts')
       caller_identity = sts.get_caller_identity()

       info = {
           "account": caller_identity["Account"],
           "region": session.region_name or "ap-northeast-2"
       }

       # MongoDB에 저장
       db = await mongodb.get_database()
       collection = db[AWS_INFO_COLLECTION]

       # 기존 데이터 삭제 후 새로운 데이터 저장
       await collection.delete_many({})
       await collection.insert_one(info)

       logger.info(f"AWS 계정 정보 수집 및 저장 성공: {info}")
       return info

   except Exception as e:
       logger.error(f"AWS 정보 수집 실패: {str(e)}")
       raise HTTPException(
           status_code=500,
           detail=f"AWS 정보 수집에 실패했습니다: {str(e)}"
       )

@app.get("/aws/info", response_model=Dict[str, str], tags=["AWS Info"])
async def get_aws_info() -> Dict[str, str]:
   """
   저장된 AWS 계정 정보 조회

   Returns:
       Dict[str, str]: AWS 계정 정보
       {
           "account": str,  # AWS 계정 번호
           "region": str    # AWS 리전
       }
   """
   try:
       db = await mongodb.get_database()
       collection = db[mongo_settings.MONGO_AWS_INFO_COLLECTION]  # 설정에서 컬렉션명 가져오기

       info = await collection.find_one({}, {'_id': 0})

       if not info:  # 여기서 저장된 정보가 없으면
           logger.info("저장된 AWS 정보가 없어 수집을 시작합니다.")
           return await collect_aws_info()  # collect API를 실행

       logger.info(f"저장된 AWS 계정 정보 조회 성공: {info}")
       return info

   except Exception as e:
       logger.error(f"AWS 정보 조회 실패: {str(e)}")
       raise HTTPException(
           status_code=500,
           detail=f"AWS 정보를 가져오는데 실패했습니다: {str(e)}"
       )