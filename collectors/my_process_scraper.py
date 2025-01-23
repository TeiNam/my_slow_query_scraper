"""
MySQL Process Scraper
Collects slow query information from MySQL performance_schema
"""

import asyncio
import pytz
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from modules.mongodb_connector import mongodb
from modules.mysql_connector import MySQLConnector
from modules.load_instance import InstanceLoader
from configs.mysql_conf import MySQLSettings
from configs.mongo_conf import mongo_settings
from configs.scraper_conf import scraper_settings
import logging
from modules.common_logger import setup_logger

# 로깅 설정
setup_logger()
logger = logging.getLogger('collectors.my_process_scraper')

# 설정 값 가져오기
EXEC_TIME = scraper_settings['exec_time']
EXCLUDED_DBS = scraper_settings['excluded_dbs']
EXCLUDED_USERS = scraper_settings['excluded_users']
MONITORING_INTERVAL = scraper_settings['monitoring_interval']

@dataclass
class QueryDetails:
    instance: str
    db: str
    pid: int
    user: str
    host: str
    time: int
    sql_text: str
    start: datetime
    end: Optional[datetime] = None


class SlowQueryMonitor:
    def __init__(self, mysql_connector: MySQLConnector):
        self.pid_time_cache: Dict[int, Dict[str, Any]] = {}
        self.mysql_connector = mysql_connector
        self._stop_event = asyncio.Event()
        self.mongodb = None
        self.collection = None
        # 초기화 시점 확인을 위한 로그 추가
        logger.warning(f"Created SlowQueryMonitor instance for {mysql_connector.instance_name}")

    @staticmethod
    async def create_mysql_connector(instance_info: Dict[str, Any]) -> MySQLConnector:
        """인스턴스 정보로 MySQL 커넥터 생성"""
        settings = MySQLSettings(
            host=instance_info['host'],
            port=instance_info['port'],
            user=instance_info['mgmt_user'],
            password=instance_info['mgmt_password'],
            database='performance_schema'
        )

        mysql_conn = MySQLConnector(instance_info['instance_name'])
        await mysql_conn.create_pool(settings)
        return mysql_conn

    async def stop(self):
        self._stop_event.set()
        logger.info(f"Stopping SlowQueryMonitor for {self.mysql_connector.instance_name}")

    async def initialize(self):
        db = await mongodb.get_database()
        self.collection = db[mongo_settings.MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION]
        # 로그 레벨을 DEBUG로 변경하여 더 자세한 정보 출력
        logger.debug("Initializing SlowQueryMonitor...")
        logger.info(f"Initialized SlowQueryMonitor for instance: {self.mysql_connector.instance_name}")
        # 로그가 실제로 출력되는지 확인하기 위한 추가 로그
        logger.warning(f"Monitor initialization complete for {self.mysql_connector.instance_name}")

    async def query_mysql_instance(self) -> None:
        try:
            excluded_dbs = ','.join(f"'{db}'" for db in EXCLUDED_DBS)
            excluded_users = ','.join(f"'{user}'" for user in EXCLUDED_USERS)

            sql_query = f"""
                SELECT `ID`, `DB`, `USER`, `HOST`, `TIME`, `INFO`
                FROM `performance_schema`.`processlist`
                WHERE info IS NOT NULL
                AND DB not in ({excluded_dbs})
                AND USER not in ({excluded_users})
                ORDER BY `TIME` DESC"""

            result = await self.mysql_connector.execute_query(sql_query)

            current_pids = set()
            for row in result:
                try:
                    time_value = float(row['TIME'])
                    await self.process_query_result(row, current_pids)
                except (ValueError, TypeError) as e:
                    logger.error(f"Failed to process TIME value: {row['TIME']}, Error: {e}")

            await self.handle_finished_queries(current_pids)

        except Exception as e:
            logger.error(f"Error querying MySQL instance {self.mysql_connector.instance_name}: {e}")

    async def process_query_result(self, row: Dict[str, Any], current_pids: set) -> None:
        pid, db, user, host, time, info = row['ID'], row['DB'], row['USER'], row['HOST'], row['TIME'], row['INFO']
        current_pids.add(pid)

        try:
            time_value = float(time)
            if time_value >= EXEC_TIME:
                logger.info(f"[CACHE] Processing slow query - DB: {db}, PID: {pid}, Time: {time_value}s")

                # 단순화된 캐시 처리
                cache_data = self.pid_time_cache.setdefault(pid, {'max_time': 0})
                cache_data['max_time'] = max(cache_data['max_time'], time_value)

                if 'start' not in cache_data:
                    utc_now = datetime.now(pytz.utc)
                    utc_start_timestamp = int((utc_now - timedelta(seconds=EXEC_TIME)).timestamp())
                    utc_start_datetime = datetime.fromtimestamp(utc_start_timestamp, pytz.utc)
                    cache_data['start'] = utc_start_datetime

                info_cleaned = re.sub(' +', ' ', info).encode('utf-8', 'ignore').decode('utf-8')
                info_cleaned = re.sub(r'[\n\t\r]+', ' ', info_cleaned).strip()

                cache_data['details'] = QueryDetails(
                    instance=self.mysql_connector.instance_name,
                    db=db,
                    pid=pid,
                    user=user,
                    host=host,
                    time=time_value,
                    sql_text=info_cleaned,
                    start=cache_data['start']
                )

                #logger.info(
                #    f"[CACHE] Cached slow query - Instance: {self.mysql_connector.instance_name}, "
                #    f"PID: {pid}, DB: {db}, Time: {time_value}s"
                #)
        except (ValueError, TypeError) as e:
            logger.error(f"[ERROR] Failed to process query result - PID: {pid}, Time value: {time}, Error: {e}")

    async def handle_finished_queries(self, current_pids: set) -> None:
        try:
            if not mongodb._client or mongodb._db is None:
                await mongodb.initialize()
                db = await mongodb.get_database()
                self.collection = db[mongo_settings.MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION]

            for pid, cache_data in list(self.pid_time_cache.items()):
                if pid not in current_pids:
                    try:
                        data_to_insert = vars(cache_data['details'])
                        data_to_insert['time'] = cache_data['max_time']
                        data_to_insert['end'] = datetime.now(pytz.utc)

                        existing_query = await self.collection.find_one({
                            'pid': data_to_insert['pid'],
                            'instance': data_to_insert['instance'],
                            'db': data_to_insert['db'],
                            'start': data_to_insert['start']
                        })

                        if not existing_query:
                            await self.collection.insert_one(data_to_insert)

                        del self.pid_time_cache[pid]

                    except Exception as e:
                        logger.error(f"Error handling finished query - Instance: {self.mysql_connector.instance_name}, "
                                     f"PID: {pid}, Error: {str(e)}")
        except Exception as e:
            logger.error(f"MongoDB connection error in handle_finished_queries: {str(e)}")

    async def run_mysql_slow_queries(self) -> None:
        try:
            logger.info(f"Starting slow query monitoring for {self.mysql_connector.instance_name}")

            while not self._stop_event.is_set():
                await self.query_mysql_instance()
                await asyncio.sleep(MONITORING_INTERVAL)

        except asyncio.CancelledError:
            logger.info(f"Slow query monitoring task was cancelled for {self.mysql_connector.instance_name}")
        except Exception as e:
            logger.error(
                f"An error occurred in slow query monitoring for {self.mysql_connector.instance_name}: {e}")
        finally:
            logger.info(f"Slow query monitoring stopped for {self.mysql_connector.instance_name}")

monitors: List[SlowQueryMonitor] = []

async def initialize_monitors() -> List[SlowQueryMonitor]:
    """Initialize monitors for all instances"""
    global monitors
    monitors = []  # 전역 변수 초기화

    instance_loader = InstanceLoader()
    realtime_instances = await instance_loader.load_realtime_instances()

    if not realtime_instances:
        logger.warning("No real-time monitoring instances found")
        return monitors

    logger.info(f"Found {len(realtime_instances)} instances for real-time monitoring")

    for instance in realtime_instances:
        try:
            mysql_conn = await SlowQueryMonitor.create_mysql_connector(instance)
            monitor = SlowQueryMonitor(mysql_conn)
            await monitor.initialize()
            monitors.append(monitor)
            logger.info(f"Initialized monitor for {instance['instance_name']}")
        except Exception as e:
            logger.error(f"Failed to initialize monitor for {instance['instance_name']}: {e}")
            continue

    return monitors

async def run_monitors(monitor_instances: List[SlowQueryMonitor]):
    """Run all initialized monitors"""
    if not monitor_instances:
        logger.error("No monitors could be initialized")
        return

    tasks = [monitor.run_mysql_slow_queries() for monitor in monitor_instances]
    await asyncio.gather(*tasks)


async def main():
    global monitors
    monitors = []  # 전역 변수 초기화

    try:
        await mongodb.initialize()
        monitors = await initialize_monitors()
        await run_monitors(monitors)
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        for monitor in monitors:
            await monitor.stop()
        await mongodb.close()

if __name__ == '__main__':
    asyncio.run(main())