"""
FastAPI endpoint for managing CloudWatch slow query collection
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, validator
from datetime import datetime, date, time, timedelta
import pytz
from typing import Optional, Dict, List, Tuple
import logging
from modules.mongodb_connector import MongoDBConnector
from collectors.cloudwatch_slowquery_collector import RDSCloudWatchSlowQueryCollector

# 로깅 설정
logger = logging.getLogger(__name__)
kst = pytz.timezone('Asia/Seoul')

class DateRangeRequest(BaseModel):
    """날짜 범위 수집 요청 모델"""
    start_date: date
    end_date: date

    @validator('end_date')
    def validate_date_range(cls, end_date, values):
        """날짜 범위 유효성 검증"""
        if 'start_date' in values and end_date < values['start_date']:
            raise ValueError("종료일이 시작일보다 이전일 수 없습니다")
        return end_date

class CollectionResponse(BaseModel):
    """수집 응답 모델"""
    status: str
    message: str
    target_date: str
    timestamp: datetime

class SlowQueryData(BaseModel):
    """슬로우 쿼리 데이터 모델"""
    date: str
    instance_id: str
    digest_query: str
    execution_count: int
    avg_time: float
    total_time: float
    avg_lock_time: float
    avg_rows_sent: float
    avg_rows_examined: float
    users: List[str]
    hosts: List[str]
    first_seen: str
    last_seen: str

class SlowQueryResponse(BaseModel):
    """슬로우 쿼리 조회 응답 모델"""
    queries: List[SlowQueryData]
    total_count: int
    page: int
    page_size: int

app = FastAPI(title="CloudWatch Slow Query Collector")

@app.post("/cloudwatch/collect", response_model=CollectionResponse)
async def start_collection(
    request: DateRangeRequest,
    background_tasks: BackgroundTasks
) -> CollectionResponse:
    """
    날짜 범위 기준 CloudWatch 슬로우 쿼리 수집 시작

    Args:
        request: 날짜 범위 수집 요청 정보
        background_tasks: FastAPI 백그라운드 태스크

    Returns:
        CollectionResponse: 수집 시작 결과
    """
    try:
        # 시작일과 종료일에 시간 정보 추가
        start_datetime = datetime.combine(request.start_date, datetime.min.time())
        end_datetime = datetime.combine(request.end_date, datetime.max.time())

        # KST 타임존 적용
        start_datetime = start_datetime.replace(tzinfo=kst)
        end_datetime = end_datetime.replace(tzinfo=kst)

        # 백그라운드에서 수집 작업 실행
        background_tasks.add_task(
            run_collection,
            start_datetime,
            end_datetime
        )

        return CollectionResponse(
            status="started",
            message=(
                f"Collection started for date range: "
                f"{request.start_date.strftime('%Y-%m-%d')} ~ "
                f"{request.end_date.strftime('%Y-%m-%d')}"
            ),
            target_date=(
                f"{request.start_date.strftime('%Y-%m-%d')} ~ "
                f"{request.end_date.strftime('%Y-%m-%d')}"
            ),
            timestamp=datetime.now(kst)
        )

    except Exception as e:
        logger.error(f"Error starting collection: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cloudwatch/queries", response_model=SlowQueryResponse)
async def get_slow_queries(
    start_date: str,
    end_date: str,
    instance_id: Optional[str] = None,
    page: int = 1,
    page_size: int = 20
) -> SlowQueryResponse:
    """
    수집된 슬로우 쿼리 데이터 조회

    Args:
        start_date (str): 시작일 (YYYY-MM-DD)
        end_date (str): 종료일 (YYYY-MM-DD)
        instance_id (str, optional): 인스턴스 ID
        page (int): 페이지 번호 (기본값: 1)
        page_size (int): 페이지 크기 (기본값: 20)

    Returns:
        SlowQueryResponse: 슬로우 쿼리 데이터 목록
    """
    try:
        # 날짜 형식 검증
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )

        # 날짜 범위 검증
        if end < start:
            raise HTTPException(
                status_code=400,
                detail="End date must be greater than or equal to start date"
            )

        # MongoDB 쿼리 필터 생성
        query_filter = {
            "date": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        if instance_id:
            query_filter["instance_id"] = instance_id

        # MongoDB에서 데이터 조회
        db = await MongoDBConnector.get_database()
        collection = db[RDSCloudWatchSlowQueryCollector().collection_name]

        # 전체 건수 조회
        total_count = await collection.count_documents(query_filter)

        ## 페이징 처리 - 정렬 조건 수정
        skip = (page - 1) * page_size
        cursor = collection.find(query_filter) \
            .sort([
                ("date", 1),             # date 기준 오름차순
                ("created_at", -1)       # created_at 기준 내림차순
            ]) \
            .skip(skip) \
            .limit(page_size)

        queries = []
        async for doc in cursor:
            queries.append(SlowQueryData(
                date=doc["date"],
                instance_id=doc["instance_id"],
                digest_query=doc["digest_query"],
                execution_count=doc["execution_count"],
                avg_time=doc["avg_time"],
                total_time=doc["total_time"],
                avg_lock_time=doc["avg_lock_time"],
                avg_rows_sent=doc["avg_rows_sent"],
                avg_rows_examined=doc["avg_rows_examined"],
                users=doc["users"],
                hosts=doc["hosts"],
                first_seen=doc["first_seen"],
                last_seen=doc["last_seen"]
            ))

        return SlowQueryResponse(
            queries=queries,
            total_count=total_count,
            page=page,
            page_size=page_size
        )

    except Exception as e:
        logger.error(f"Error fetching slow queries: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def run_collection(start_date: datetime, end_date: datetime) -> None:
    """
    날짜 범위 수집 실행

    Args:
        start_date (datetime): 수집 시작일
        end_date (datetime): 수집 종료일
    """
    try:
        await MongoDBConnector.initialize()
        collector = RDSCloudWatchSlowQueryCollector()
        await collector.initialize()

        try:
            await collector.collect_metrics_by_range(start_date, end_date)

            logger.info(
                f"날짜 범위 수집 완료: "
                f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}"
            )

        except Exception as e:
            logger.error(f"슬로우 쿼리 수집 중 오류 발생: {e}")
            raise

    except Exception as e:
        logger.error(f"수집 프로세스 초기화 중 오류 발생: {e}")
        raise
    finally:
        try:
            await MongoDBConnector.close()
        except Exception as e:
            logger.error(f"MongoDB 연결 종료 중 오류 발생: {e}")