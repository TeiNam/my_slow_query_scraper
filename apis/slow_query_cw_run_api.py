"""
FastAPI endpoint for managing CloudWatch slow query collection
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime, date
import pytz
from typing import Optional, Dict
import logging
from collectors.cloudwatch_slowquery_collector import collect_slow_queries, RDSCloudWatchSlowQueryCollector
from modules.mongodb_connector import MongoDBConnector

# 로깅 설정
logger = logging.getLogger(__name__)
kst = pytz.timezone('Asia/Seoul')

# 글로벌 상태 관리
running_collectors: Dict[str, RDSCloudWatchSlowQueryCollector] = {}

class CollectionRequest(BaseModel):
    """수집 요청 모델"""
    target_date: Optional[date] = None

class CollectionResponse(BaseModel):
    """수집 응답 모델"""
    status: str
    message: str
    target_date: str
    timestamp: datetime

app = FastAPI(title="CloudWatch Slow Query Collector")

async def run_collection(target_date: datetime) -> None:
    """백그라운드에서 수집 작업 실행"""
    try:
        await collect_slow_queries(target_date)
        logger.info(f"Collection completed for date: {target_date.strftime('%Y-%m-%d')}")
    except Exception as e:
        logger.error(f"Error during collection: {e}")

@app.post("/cloudwatch/run", response_model=CollectionResponse)
async def start_collection(
    request: CollectionRequest,
    background_tasks: BackgroundTasks
) -> CollectionResponse:
    """
    CloudWatch 슬로우 쿼리 수집 시작

    Args:
        request: 수집 요청 정보
        background_tasks: FastAPI 백그라운드 태스크

    Returns:
        CollectionResponse: 수집 시작 결과
    """
    try:
        target_date = request.target_date
        if target_date is None:
            target_date = datetime.now(kst).date()

        target_datetime = datetime.combine(target_date, datetime.min.time())
        target_datetime = target_datetime.replace(tzinfo=kst)

        # 백그라운드에서 수집 작업 실행
        background_tasks.add_task(run_collection, target_datetime)

        return CollectionResponse(
            status="started",
            message=f"Collection started for date: {target_date.strftime('%Y-%m-%d')}",
            target_date=target_date.strftime('%Y-%m-%d'),
            timestamp=datetime.now(kst)
        )

    except Exception as e:
        logger.error(f"Error starting collection: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cloudwatch/status/{target_date}", response_model=CollectionResponse)
async def get_collection_status(target_date: str) -> CollectionResponse:
    """
    특정 날짜의 수집 상태 조회

    Args:
        target_date: 조회할 날짜 (YYYY-MM-DD 형식)

    Returns:
        CollectionResponse: 수집 상태
    """
    try:
        date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()

        # MongoDB에서 해당 날짜의 수집 결과 확인
        db = await MongoDBConnector.get_database()
        collection = db[RDSCloudWatchSlowQueryCollector().collection_name]

        result = await collection.find_one({"date": target_date})

        if result:
            status = "completed"
            message = f"Collection completed for date: {target_date}"
        else:
            status = "not_found"
            message = f"No collection data found for date: {target_date}"

        return CollectionResponse(
            status=status,
            message=message,
            target_date=target_date,
            timestamp=datetime.now(kst)
        )

    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use YYYY-MM-DD"
        )
    except Exception as e:
        logger.error(f"Error checking collection status: {e}")
        raise HTTPException(status_code=500, detail=str(e))