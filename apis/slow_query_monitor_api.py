"""
FastAPI endpoint for managing slow query monitoring
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import asyncio
from typing import Optional, List
import logging
from datetime import datetime
import pytz
from collectors.explain_collector import ExplainCollector
from collectors.my_process_scraper import SlowQueryMonitor, initialize_monitors, run_monitors
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
from modules.common_logger import setup_logger

# 로깅 설정
setup_logger()
logger = logging.getLogger('api.slow_query_monitor')

# Global state management
monitor_running: bool = False
monitoring_task: Optional[asyncio.Task] = None
monitors: List[SlowQueryMonitor] = []

class ExplainResponse(BaseModel):
    """Response model for explain collection results"""
    status: str
    message: str
    pid: int
    instance_name: str
    timestamp: datetime

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
    lifespan=lifespan
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

@app.post("/mysql/start", response_model=MonitorResponse)
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

@app.post("/mysql/stop", response_model=MonitorResponse)
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

@app.get("/mysql/status", response_model=MonitorResponse)
async def get_monitor_status():
    """Get monitoring status"""
    return MonitorResponse(
        status="running" if monitor_running else "stopped",
        message=f"Monitoring is {'running' if monitor_running else 'stopped'}",
        timestamp=datetime.now(pytz.utc)
    )


@app.post("/mysql/explain/{pid}", response_model=ExplainResponse)
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
            status="processing",
            message=f"Explain collection started for PID {pid}",
            pid=pid,
            instance_name=query_doc['instance'],
            timestamp=datetime.now(pytz.utc)
        )

    except Exception as e:
        logger.error(f"Error collecting explain for PID {pid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/mysql/explain/{pid}", response_model=ExplainResponse)
async def get_explain_status(pid: int):
    """Get status of execution plan collection for a specific PID"""
    try:
        mongodb_conn = await MongoDBConnector.get_database()
        plan_collection = mongodb_conn[mongo_settings.MONGO_SLOW_LOG_PLAN_COLLECTION]
        slow_query_collection = mongodb_conn[mongo_settings.MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION]

        query = await slow_query_collection.find_one({"pid": pid})
        if not query:
            raise HTTPException(
                status_code=404,
                detail=f"No slow query found for PID {pid}"
            )

        plan = await plan_collection.find_one({"pid": pid})
        status = "completed" if plan else "pending"
        status_msg = "Explain collection completed" if plan else "Explain collection pending or in progress"

        return ExplainResponse(
            status=status,
            message=status_msg,
            pid=pid,
            instance_name=query['instance'],
            timestamp=datetime.now(pytz.utc)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking explain status for PID {pid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))