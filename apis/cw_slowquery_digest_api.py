"""
CloudWatch Slow Query Digest API
CloudWatch에서 수집된 MySQL 슬로우 쿼리 통계 정보를 조회하는 API
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, status
from starlette.responses import JSONResponse

from modules.mongodb_connector import mongodb
from configs.mongo_conf import mongo_settings
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="CloudWatch Slow Query Digest", tags=["CloudWatch Slow Query Digest"])


async def get_previous_month_range() -> tuple[datetime, datetime]:
    """이전 달의 시작일과 종료일을 반환합니다."""
    today = datetime.now()
    first_day = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    last_day = today.replace(day=1) - timedelta(days=1)

    return first_day, last_day

@app.get("/cw-slowquery/digest/stats", response_model=Dict[str, Any],tags=["CloudWatch Slow Query Digest"])
async def get_slow_query_stats() -> JSONResponse:
    """
    인스턴스별, 쿼리 다이제스트별 슬로우 쿼리 통계를 조회합니다.

    Returns:
        Dict[str, Any]: 슬로우 쿼리 통계 정보
        {
            "month": str,
            "stats": List[{
                "instance_id": str,
                "digest_query": str,
                "avg_stats": {
                    "avg_lock_time": float,
                    "avg_rows_examined": float,
                    "avg_rows_sent": float,
                    "avg_time": float
                },
                "sum_stats": {
                    "execution_count": int,
                    "total_time": float
                }
            }]
        }
    """
    try:
        await mongodb.initialize()
        db = await mongodb.get_database()
        collection = db[mongo_settings.MONGO_CW_MYSQL_SLOW_SQL_COLLECTION]

        start_date, end_date = await get_previous_month_range()

        # MongoDB Aggregation Pipeline
        pipeline = [
            {
                "$match": {
                    "date": {
                        "$gte": start_date.strftime("%Y-%m-%d"),
                        "$lte": end_date.strftime("%Y-%m-%d")
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "instance_id": "$instance_id",
                        "digest_query": "$digest_query"
                    },
                    "users": {"$first": "$users"},  # users 필드를 그룹핑할 때 포함
                    "avg_lock_time": {"$avg": "$avg_lock_time"},
                    "avg_rows_examined": {"$avg": "$avg_rows_examined"},
                    "avg_rows_sent": {"$avg": "$avg_rows_sent"},
                    "avg_time": {"$avg": "$avg_time"},
                    "total_execution_count": {"$sum": "$execution_count"},
                    "total_time": {"$sum": "$total_time"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "instance_id": "$_id.instance_id",
                    "digest_query": "$_id.digest_query",
                    "user": {"$arrayElemAt": ["$users", 0]},
                    "avg_stats": {
                        "avg_lock_time": "$avg_lock_time",
                        "avg_rows_examined": "$avg_rows_examined",
                        "avg_rows_sent": "$avg_rows_sent",
                        "avg_time": "$avg_time"
                    },
                    "sum_stats": {
                        "execution_count": "$total_execution_count",
                        "total_time": "$total_time"
                    }
                }
            },
            {
                "$sort": {
                    "sum_stats.execution_count": -1
                }
            }
        ]

        results = await collection.aggregate(pipeline).to_list(length=None)

        if not results:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"message": "해당 기간의 슬로우 쿼리 통계 정보를 찾을 수 없습니다."}
            )

        response_data = {
            "month": start_date.strftime("%Y-%m"),
            "stats": results
        }

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response_data
        )

    except Exception as e:
        logger.error(f"슬로우 쿼리 통계 조회 중 오류 발생: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"슬로우 쿼리 통계 조회 실패: {str(e)}"
        )
    finally:
        await mongodb.close()