"""
FastAPI endpoint for CloudWatch slow query collection
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Tuple, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket
from pydantic import BaseModel

from collectors.cloudwatch_slowquery_collector import RDSCloudWatchSlowQueryCollector
from configs.mongo_conf import mongo_settings
from modules.mongodb_connector import MongoDBConnector
from modules.time_utils import get_current_kst, format_kst
from modules.websocket_manager import websocket_manager

# 로깅 설정
logger = logging.getLogger(__name__)

class CollectionResponse(BaseModel):
    """수집 응답 모델"""
    status: str
    message: str
    target_date: str
    timestamp: datetime
    collection_id: str

app = FastAPI(title="CloudWatch Slow Query Collector")

def get_last_month_range() -> Tuple[datetime, datetime]:
    """전월의 시작일과 종료일 계산 (KST 기준)"""
    current_kst = get_current_kst()
    first_day_current_month = current_kst.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month_first_day = (first_day_current_month - timedelta(days=1)).replace(day=1)
    next_month = (last_month_first_day + timedelta(days=32)).replace(day=1)
    last_month_last_day = next_month - timedelta(seconds=1)

    return last_month_first_day, last_month_last_day

@app.websocket("/ws/collection/{collection_id}")
async def websocket_endpoint(websocket: WebSocket, collection_id: str):
    """웹소켓 연결 엔드포인트"""
    try:
        await websocket_manager.connect(websocket, collection_id)
        while True:
            try:
                await websocket.receive_text()
            except Exception:
                break
    finally:
        await websocket_manager.disconnect(websocket, collection_id)

@app.post("/cloudwatch/run", response_model=CollectionResponse)
async def run_last_month_collection(
    background_tasks: BackgroundTasks
) -> CollectionResponse:
    """전월 CloudWatch 슬로우 쿼리 수집 실행"""
    try:
        await MongoDBConnector.initialize()

        try:
            # RDS 인스턴스 목록 조회
            db = await MongoDBConnector.get_database()
            instance_collection = db[mongo_settings.MONGO_RDS_INSTANCE_COLLECTION]

            cursor = instance_collection.find()
            instances = await cursor.to_list(length=None)

            if not instances:
                raise HTTPException(
                    status_code=404,
                    detail="수집 대상 인스턴스가 없습니다."
                )

            # 전월 날짜 범위 계산 (KST 기준)
            start_datetime, end_datetime = get_last_month_range()
            collection_id = f"collect_{start_datetime.strftime('%Y%m')}"

            target_instances = [inst.get("instance_name", inst.get("_id")) for inst in instances]

            # 수집 시작 상태 업데이트
            await websocket_manager.update_status(
                collection_id=collection_id,
                status="started",
                details={
                    "start_date": format_kst(start_datetime),
                    "end_date": format_kst(end_datetime),
                    "target_instances": target_instances,
                    "instance_count": len(target_instances)
                }
            )

            await websocket_manager.broadcast_log(
                collection_id=collection_id,
                message=(
                    f"전월 데이터 수집 시작\n"
                    f"기간: {format_kst(start_datetime)} ~ {format_kst(end_datetime)}\n"
                    f"대상 인스턴스: {len(target_instances)}개"
                )
            )

            # 백그라운드 작업 시작
            background_tasks.add_task(
                run_collection,
                start_datetime,
                end_datetime,
                collection_id
            )

            return CollectionResponse(
                status="started",
                message=(
                    f"전월({start_datetime.strftime('%Y-%m')}) "
                    f"슬로우 쿼리 수집이 시작되었습니다. "
                    f"대상 인스턴스: {len(target_instances)}개"
                ),
                target_date=(
                    f"{start_datetime.strftime('%Y-%m-%d')} ~ "
                    f"{end_datetime.strftime('%Y-%m-%d')}"
                ),
                timestamp=get_current_kst(),
                collection_id=collection_id
            )

        finally:
            await MongoDBConnector.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"전월 데이터 수집 시작 중 오류 발생: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def run_collection(
    start_date: datetime,
    end_date: datetime,
    collection_id: str
) -> None:
    """날짜 범위 수집 실행"""
    try:
        await MongoDBConnector.initialize()
        collector = RDSCloudWatchSlowQueryCollector()
        db = await MongoDBConnector.get_database()

        total_queries = 0
        processed_instances = 0
        total_instances = 0
        error_message = None

        # 수집 상태 업데이트를 위한 콜백 함수
        async def progress_callback(progress: Optional[float], message: Optional[str], level: str = "info"):
            nonlocal total_queries, processed_instances
            if message and "슬로우 쿼리 분석 완료" in message:
                processed_instances += 1
                if match := re.search(r'(\d+)개의 슬로우 쿼리', message):
                    total_queries += int(match.group(1))

            await websocket_manager.update_status(
                collection_id=collection_id,
                status="in_progress",
                details={
                    "progress": progress if progress is not None else 0,
                    "message": message,
                    "processed_instances": processed_instances,
                    "total_queries": total_queries
                }
            )
            if message:
                await websocket_manager.broadcast_log(
                    collection_id=collection_id,
                    message=message,
                    level=level
                )

        try:
            await collector.initialize()
            total_instances = len(collector._target_instances)

            await collector.collect_metrics_by_range(
                start_date,
                end_date,
                progress_callback
            )

            # 수집 완료 상태 및 결과 전송
            result_details = {
                "status": "completed",
                "progress": 100,
                "completed_at": get_current_kst().isoformat(),
                "total_queries": total_queries,
                "processed_instances": processed_instances,
                "total_instances": total_instances,
                "period": {
                    "start_date": format_kst(start_date),
                    "end_date": format_kst(end_date)
                }
            }

            await websocket_manager.update_status(
                collection_id=collection_id,
                status="completed",
                details=result_details
            )

            await websocket_manager.broadcast_log(
                collection_id=collection_id,
                message=(
                    f"수집 완료\n"
                    f"- 처리된 인스턴스: {processed_instances}/{total_instances}\n"
                    f"- 수집된 쿼리 수: {total_queries}"
                )
            )

        except Exception as e:
            error_message = str(e)
            await websocket_manager.update_status(
                collection_id=collection_id,
                status="error",
                details={
                    "error": error_message,
                    "processed_instances": processed_instances,
                    "total_queries": total_queries
                }
            )
            await websocket_manager.broadcast_log(
                collection_id=collection_id,
                message=f"수집 중 오류 발생: {error_message}",
                level="error"
            )
            raise

    except Exception as e:
        logger.error(f"수집 초기화 중 오류 발생: {str(e)}")
        raise

    finally:
        await MongoDBConnector.close()