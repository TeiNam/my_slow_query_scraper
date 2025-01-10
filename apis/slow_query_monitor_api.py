"""
FastAPI endpoint for managing slow query monitoring
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import asyncio
from typing import Dict, List
import logging
from datetime import datetime
import pytz
from collectors.explain_collector import ExplainCollector
from collectors.my_process_scraper import SlowQueryMonitor
from modules.load_instance import InstanceLoader
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings

logger = logging.getLogger(__name__)

# Global state management
running_monitors: Dict[str, SlowQueryMonitor] = {}
monitor_tasks: Dict[str, asyncio.Task] = {}

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
    instance_name: str
    timestamp: datetime

app = FastAPI(title="MySQL Slow Query Monitor")

async def run_monitor(instance_name: str, monitor: SlowQueryMonitor):
    """Run monitor in background with error handling"""
    try:
        await monitor.initialize()
        await monitor.run_mysql_slow_queries()
    except Exception as e:
        logger.error(f"Error in monitor for {instance_name}: {e}")
        await stop_monitor(instance_name)
    finally:
        if instance_name in running_monitors:
            await stop_monitor(instance_name)

@app.post("/mysql/start/{instance_name}", response_model=MonitorResponse)
async def start_monitor(instance_name: str, background_tasks: BackgroundTasks):
    """Start slow query monitoring for a specific instance"""
    if instance_name in running_monitors:
        raise HTTPException(
            status_code=400,
            detail=f"Monitor for instance {instance_name} is already running"
        )

    try:
        instance_loader = InstanceLoader()
        realtime_instances = await instance_loader.load_realtime_instances()
        instance_info = next(
            (inst for inst in realtime_instances if inst['instance_name'] == instance_name),
            None
        )

        if not instance_info:
            raise HTTPException(
                status_code=404,
                detail=f"Instance {instance_name} not found or not configured for real-time monitoring"
            )

        mysql_conn = await SlowQueryMonitor.create_mysql_connector(instance_info)
        monitor = SlowQueryMonitor(mysql_conn)
        running_monitors[instance_name] = monitor

        task = asyncio.create_task(run_monitor(instance_name, monitor))
        monitor_tasks[instance_name] = task

        return MonitorResponse(
            status="started",
            message=f"Monitoring started for instance {instance_name}",
            instance_name=instance_name,
            timestamp=datetime.now(pytz.utc)
        )

    except Exception as e:
        logger.error(f"Error starting monitor for {instance_name}: {e}")
        if instance_name in running_monitors:
            await stop_monitor(instance_name)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/mysql/stop/{instance_name}", response_model=MonitorResponse)
async def stop_monitor(instance_name: str):
    """Stop slow query monitoring for a specific instance"""
    if instance_name not in running_monitors:
        raise HTTPException(
            status_code=404,
            detail=f"No running monitor found for instance {instance_name}"
        )

    try:
        monitor = running_monitors[instance_name]
        await monitor.stop()

        if instance_name in monitor_tasks:
            task = monitor_tasks[instance_name]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            monitor_tasks.pop(instance_name)

        running_monitors.pop(instance_name)

        return MonitorResponse(
            status="stopped",
            message=f"Monitoring stopped for instance {instance_name}",
            instance_name=instance_name,
            timestamp=datetime.now(pytz.utc)
        )

    except Exception as e:
        logger.error(f"Error stopping monitor for {instance_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/mysql/status", response_model=List[MonitorResponse])
async def get_monitor_status():
    """Get status of all running monitors"""
    current_time = datetime.now(pytz.utc)
    return [
        MonitorResponse(
            status="running",
            message="Monitor is running",
            instance_name=instance_name,
            timestamp=current_time
        )
        for instance_name in running_monitors.keys()
    ]

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