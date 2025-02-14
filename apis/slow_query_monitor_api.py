"""
FastAPI endpoint for managing slow query monitoring
"""

import asyncio
import logging
import pytz
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Response
from typing import Optional, List
from datetime import datetime
from collectors.explain_collector import ExplainCollector
from collectors.my_process_scraper import SlowQueryMonitor, initialize_monitors, run_monitors
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
from modules.common_logger import setup_logger
import sqlparse
from pydantic import BaseModel




# 로깅 설정
setup_logger()
logger = logging.getLogger('api.slow_query_monitor')

# Global state management
monitor_running: bool = False
monitoring_task: Optional[asyncio.Task] = None
monitors: List[SlowQueryMonitor] = []

class SlowQuery(BaseModel):
    """Slow Query Data Model"""
    pid: int
    instance: str
    db: str
    user: str
    host: str
    time: float
    sql_text: str
    start: datetime
    end: Optional[datetime]

class SlowQueryResponse(BaseModel):
    """Slow Query List Response Model"""
    total: int
    page: int
    page_size: int
    items: List[SlowQuery]

class ExplainResponse(BaseModel):
    """Response model for explain collection results"""
    status: str
    message: str
    pid: int
    instance_name: str
    timestamp: datetime

class ExplainPlanResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: List[dict]

class MonitorResponse(BaseModel):
    """Response model for monitor status"""
    status: str
    message: str
    timestamp: datetime

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    try:
        logger.info("Starting application lifespan...")
        yield
    except Exception as e:
        logger.error(f"Error during application lifecycle: {e}")
        raise
    finally:
        if monitor_running:
            logger.info("Stopping monitoring during shutdown...")
            await stop_monitoring()
        logger.info("Application shutdown complete")

app = FastAPI(
    title="MySQL Slow Query Monitor",
    lifespan=lifespan,
    tags=["MySQL Slow Query Monitor"],
)


async def run_all_monitors():
    """Run monitoring for all instances"""
    global monitors
    try:
        await MongoDBConnector.initialize()

        monitors = await initialize_monitors()
        await run_monitors(monitors)

    except asyncio.CancelledError:
        logger.info("Monitoring cancelled")
        raise
    except Exception as e:
        logger.error(f"Error in monitoring: {e}")
    finally:
        for monitor in monitors:
            await monitor.stop()
        await MongoDBConnector.close()

@app.post("/mysql/start", response_model=MonitorResponse,tags=["MySQL Slow Query Monitor"])
async def start_monitoring():
    """Start monitoring for all instances"""
    global monitor_running, monitoring_task, monitors

    if monitor_running:
        raise HTTPException(
            status_code=400,
            detail="Monitoring is already running"
        )

    try:
        monitor_running = True
        monitoring_task = asyncio.create_task(run_all_monitors())

        return MonitorResponse(
            status="started",
            message="Monitoring started for all instances",
            timestamp=datetime.now(pytz.utc)
        )
    except Exception as e:
        monitor_running = False
        logger.error(f"Error starting monitoring: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/mysql/stop", response_model=MonitorResponse,tags=["MySQL Slow Query Monitor"])
async def stop_monitoring():
    """Stop monitoring for all instances"""
    global monitor_running, monitoring_task, monitors

    if not monitor_running:
        raise HTTPException(
            status_code=400,
            detail="Monitoring is not running"
        )

    try:
        monitor_running = False
        if monitoring_task:
            monitoring_task.cancel()
            try:
                await monitoring_task
            except asyncio.CancelledError:
                pass

        for monitor in monitors:
            await monitor.stop()
        monitors.clear()

        return MonitorResponse(
            status="stopped",
            message="Monitoring stopped for all instances",
            timestamp=datetime.now(pytz.utc)
        )
    except Exception as e:
        logger.error(f"Error stopping monitoring: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/mysql/status", response_model=MonitorResponse,tags=["MySQL Slow Query Monitor"])
async def get_monitor_status():
    """Get monitoring status"""
    return MonitorResponse(
        status="running" if monitor_running else "stopped",
        message=f"Monitoring is {'running' if monitor_running else 'stopped'}",
        timestamp=datetime.now(pytz.utc)
    )


@app.post("/mysql/explain/{pid}", response_model=ExplainResponse, tags=["MySQL Slow Query Monitor"])
async def collect_query_explain(pid: int, background_tasks: BackgroundTasks):
    """Collect execution plan for a specific slow query PID"""
    try:
        mongodb_conn = await MongoDBConnector.get_database()
        collection = mongodb_conn[mongo_settings.MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION]
        query_doc = await collection.find_one({"pid": pid})

        if not query_doc:
            raise HTTPException(
                status_code=404,
                detail=f"No slow query found for PID {pid}"
            )

        # SQL 타입 체크
        sql_text = query_doc['sql_text'].lower().strip()
        if sql_text.startswith('update'):
            return ExplainResponse(
                status="manual_check_required",
                message="UPDATE 쿼리는 직접 SELECT 쿼리로 변환해서 확인하는 작업이 필요합니다.",
                pid=pid,
                instance_name=query_doc['instance'],
                timestamp=datetime.now(pytz.utc)
            )

        if sql_text.startswith('delete'):
            return ExplainResponse(
                status="manual_check_required",
                message="DELETE 쿼리는 직접 SELECT 쿼리로 변환해서 확인하는 작업이 필요합니다.",
                pid=pid,
                instance_name=query_doc['instance'],
                timestamp=datetime.now(pytz.utc)
            )

        if sql_text.startswith("select") and "into" in sql_text:
            return ExplainResponse(
                status="manual_check_required",
                message="SELECT INTO 쿼리는 프로시저의 내용을 직접 확인하는 과정이 필요합니다.",
                pid=pid,
                instance_name=query_doc['instance'],
                timestamp=datetime.now(pytz.utc)
            )

        collector = ExplainCollector()
        background_tasks.add_task(collector.collect_explain_by_pid, pid)

        return ExplainResponse(
            status="The execution plan has been successfully saved.",
            message=f"Explain collection started for PID {pid}",
            pid=pid,
            instance_name=query_doc['instance'],
            timestamp=datetime.now(pytz.utc)
        )

    except Exception as e:
        logger.error(f"Error collecting explain for PID {pid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/mysql/explain/{pid}/markdown", response_model=ExplainResponse, tags=["MySQL Slow Query Monitor"])
async def get_explain_markdown(pid: int):
    """Get explain data as markdown file"""
    try:
        mongodb_conn = await MongoDBConnector.get_database()
        plan_collection = mongodb_conn[mongo_settings.MONGO_RDS_MYSQL_SLOW_SQL_PLAN_COLLECTION]
        slow_query_collection = mongodb_conn[mongo_settings.MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION]

        query = await slow_query_collection.find_one({"pid": pid})
        if not query:
            raise HTTPException(
                status_code=404,
                detail=f"No slow query found for PID {pid}"
            )

        plan = await plan_collection.find_one({"pid": pid})
        if not plan:
            raise HTTPException(
                status_code=404,
                detail=f"No explain plan found for PID {pid}"
            )

        # SQL 포매팅
        formatted_sql = sqlparse.format(
            query['sql_text'],
            reindent=True,
            keyword_case='upper'
        )

        markdown_content = f"""### 인스턴스: {query['instance']}

- 데이터베이스: {query['db']}
- PID: {query['pid']}
- 사용자: {query['user']}
- 실행시간: {query['time']}

- SQL TEXT:
```sql
{formatted_sql}
```
- Explain Tree:
```tree
{plan['explain_result']['tree']}
```
- Explain JSON:
```json
{json.dumps(plan['explain_result']['json'], indent=4)}
"""
        headers = {
            'Content-Disposition': f'attachment; filename="Slow_Query_{pid}.md"'
        }

        return Response(
            content=markdown_content,
            media_type='text/markdown',
            headers=headers
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating markdown for PID {pid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/mysql/queries", response_model=SlowQueryResponse, tags=["MySQL Slow Query Monitor"])
async def get_slow_queries(
        page: int = Query(1, gt=0, description="페이지 번호"),
        page_size: int = Query(20, gt=0, le=100, description="페이지당 항목 수"),
        start_date: Optional[datetime] = Query(None, description="시작 날짜"),
        end_date: Optional[datetime] = Query(None, description="종료 날짜"),
        instance: Optional[str] = Query(None, description="인스턴스 이름")
):
    """
    슬로우 쿼리 목록 조회
    - 페이지네이션 지원
    - 날짜 범위 필터링
    - 인스턴스 필터링
    """
    try:
        mongodb_conn = await MongoDBConnector.get_database()
        collection = mongodb_conn[mongo_settings.MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION]

        # 필터 조건 구성
        filter_query = {}

        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = start_date
            if end_date:
                date_filter["$lte"] = end_date
            if date_filter:
                filter_query["start"] = date_filter

        if instance:
            filter_query["instance"] = instance

        # 전체 문서 수 조회
        total_count = await collection.count_documents(filter_query)

        # 페이지네이션 적용하여 데이터 조회
        skip = (page - 1) * page_size
        cursor = collection.find(
            filter_query,
            {'_id': 0}  # _id 필드 제외
        ).sort('start', -1).skip(skip).limit(page_size)

        items = [SlowQuery(**doc) async for doc in cursor]

        return SlowQueryResponse(
            total=total_count,
            page=page,
            page_size=page_size,
            items=items
        )

    except Exception as e:
        logger.error(f"Error fetching slow queries: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/explain/plans", response_model=ExplainPlanResponse, tags=["MySQL Slow Query Monitor"])
async def get_explain_plans(
       page: int = Query(1, gt=0, description="페이지 번호"),
       page_size: int = Query(20, gt=0, le=100, description="페이지당 항목 수"),
       start_date: Optional[datetime] = Query(None, description="시작 날짜"),
       end_date: Optional[datetime] = Query(None, description="종료 날짜"),
       instance: Optional[str] = Query(None, description="인스턴스 이름")
):
   """
   실행 계획 목록 조회
   - 페이지네이션 지원
   - 날짜 범위 필터링
   - 인스턴스 필터링
   """
   try:
       mongodb_conn = await MongoDBConnector.get_database()
       collection = mongodb_conn[mongo_settings.MONGO_RDS_MYSQL_SLOW_SQL_PLAN_COLLECTION]

       # 필터 조건 구성
       filter_query = {}

       if start_date or end_date:
           date_filter = {}
           if start_date:
               date_filter["$gte"] = start_date
           if end_date:
               date_filter["$lte"] = end_date
           if date_filter:
               filter_query["created_at"] = date_filter

       if instance:
           filter_query["instance"] = instance

       # 전체 문서 수 조회
       total_count = await collection.count_documents(filter_query)

       # 페이지네이션 적용하여 데이터 조회
       skip = (page - 1) * page_size
       cursor = collection.find(
           filter_query,
           {'_id': 0}  # _id 필드 제외
       ).sort('created_at', -1).skip(skip).limit(page_size)

       items = [doc async for doc in cursor]

       return {
           "total": total_count,
           "page": page,
           "page_size": page_size,
           "items": items
       }

   except Exception as e:
       logger.error(f"Error fetching explain plans: {e}")
       raise HTTPException(status_code=500, detail=str(e))