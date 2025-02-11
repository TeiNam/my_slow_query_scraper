"""
SQL Statistics API
SQL 통계 정보 제공 API
"""

import logging
from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict, Any, Optional
from modules.mongodb_connector import mongodb
from modules.sql_statistics import SQLStatisticsCalculator
from configs.mongo_conf import mongo_settings
from datetime import datetime

logger = logging.getLogger(__name__)

app = FastAPI(title="SQL Statistics API")

@app.post("/sql/statistics/calculate/{year_month}")
async def calculate_statistics(year_month: str) -> Dict[str, Any]:
    """월간 SQL 통계 계산 및 저장"""
    try:
        # 년월 형식 검증
        try:
            datetime.strptime(year_month, "%Y-%m")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="올바른 년월 형식이 아닙니다. YYYY-MM 형식을 사용하세요."
            )

        calculator = SQLStatisticsCalculator()
        stats = await calculator.calculate_monthly_statistics(year_month)

        return {
            "status": "success",
            "message": f"{year_month} 통계 계산 완료",
            "count": len(stats)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"통계 계산 실패 ({year_month}): {e}")
        raise HTTPException(
            status_code=500,
            detail=f"통계 계산 실패: {str(e)}"
        )

@app.get("/sql/statistics/{year_month}")
async def get_statistics(
    year_month: str,
    instance_ids: Optional[List[str]] = Query(None, description="RDS 인스턴스 ID 목록")
) -> List[Dict[str, Any]]:
    """
    월간 SQL 통계 조회

    Args:
        year_month: YYYY-MM 형식의 년월
        instance_ids: 선택적 인스턴스 ID 목록
    """
    try:
        # 년월 형식 검증
        try:
            datetime.strptime(year_month, "%Y-%m")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="올바른 년월 형식이 아닙니다. YYYY-MM 형식을 사용하세요."
            )

        db = await mongodb.get_database()
        collection = db[mongo_settings.MONGO_CW_SQL_STATISTICS]

        # 쿼리 필터 구성
        query = {"month": year_month}
        if instance_ids:
            query["instance_id"] = {"$in": instance_ids}

        # 저장된 통계 조회 (ObjectId 제외하고 반환)
        cursor = collection.find(query, {'_id': 0})
        stats = await cursor.to_list(None)

        if not stats:
            raise HTTPException(
                status_code=404,
                detail=f"{year_month} 통계를 찾을 수 없습니다. 먼저 /sql/statistics/calculate/{year_month}를 실행하세요."
            )

        return stats

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"통계 조회 실패 ({year_month}): {e}")
        raise HTTPException(
            status_code=500,
            detail=f"통계 조회 실패: {str(e)}"
        )


@app.post("/sql/statistics/users/calculate/{year_month}")
async def calculate_user_statistics(year_month: str) -> Dict[str, Any]:
    """월간 사용자별 SQL 통계 계산 및 저장"""
    try:
        calculator = SQLStatisticsCalculator()
        stats = await calculator.calculate_user_statistics(year_month)

        return {
            "status": "success",
            "message": f"{year_month} 사용자별 통계 계산 완료",
            "count": len(stats)
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"사용자별 통계 계산 실패: {str(e)}"
        )


@app.get("/sql/statistics/users/{year_month}")
async def get_user_statistics(
        year_month: str,
        instance_ids: Optional[List[str]] = Query(None, description="RDS 인스턴스 ID 목록")
) -> List[Dict[str, Any]]:
    """
    월간 사용자별 SQL 통계 조회
    """
    try:
        db = await mongodb.get_database()
        collection = db[mongo_settings.MONGO_CW_SQL_USER_STATISTICS]

        # 쿼리 필터 구성
        query = {"month": year_month}
        if instance_ids:
            query["instance_id"] = {"$in": instance_ids}

        # 저장된 통계 조회
        cursor = collection.find(query, {'_id': 0})
        stats = await cursor.to_list(None)

        if not stats:
            raise HTTPException(
                status_code=404,
                detail=f"{year_month} 사용자별 통계를 찾을 수 없습니다."
            )

        return stats

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"사용자별 통계 조회 실패: {str(e)}"
        )