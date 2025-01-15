"""
RDS Instance API
현재 RDS 인스턴스 목록을 조회하고 수집된 인스턴스 정보를 제공하는 API
"""

from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException, status
from starlette.responses import JSONResponse

from collectors.rds_instance_collector import MySQLInstanceCollector
from modules.mongodb_connector import mongodb
from configs.mongo_conf import mongo_settings
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/collectors/rds-instances", response_model=Dict[str, Any])
async def collect_rds_instances() -> Dict[str, Any]:
    """
    현재 AWS RDS의 MySQL/Aurora MySQL 인스턴스 정보를 수집하여 DB에 저장

    Returns:
        Dict[str, Any]: 수집 결과 정보
        {
            "status": "success" | "error",
            "message": str,
            "collected_count": int,
            "details": Dict[str, Any]
        }
    """
    try:
        collector = MySQLInstanceCollector()
        instances = await collector.get_mysql_instances()

        if instances:
            await collector.save_to_mongodb(instances)
            return {
                "status": "success",
                "message": "RDS instances collected successfully",
                "collected_count": len(instances),
                "details": {
                    "instance_ids": [inst["DBInstanceIdentifier"] for inst in instances]
                }
            }
        else:
            return {
                "status": "success",
                "message": "No instances found to collect",
                "collected_count": 0,
                "details": {}
            }

    except Exception as e:
        logger.error(f"Error collecting RDS instances: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to collect RDS instances: {str(e)}"
        )


@router.get("/rds-instances", response_model=List[Dict[str, Any]])
async def get_collected_instances() -> JSONResponse:
    """
    MongoDB에 저장된 RDS 인스턴스 목록을 조회

    Returns:
        List[Dict[str, Any]]: 수집된 인스턴스 정보 목록
    """
    try:
        await mongodb.initialize()
        db = await mongodb.get_database()
        collection = db[mongo_settings.MONGO_RDS_INSTANCE_COLLECTION]

        # 최신 수집 데이터 조회 (updateTime 기준 내림차순)
        cursor = collection.find(
            {},
            {'_id': 0}  # _id 필드 제외
        ).sort('updateTime', -1)

        instances = await cursor.to_list(length=None)

        if not instances:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"message": "No instances found in database"}
            )

        return instances

    except Exception as e:
        logger.error(f"Error fetching RDS instances from database: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch RDS instances: {str(e)}"
        )
    finally:
        await mongodb.close()