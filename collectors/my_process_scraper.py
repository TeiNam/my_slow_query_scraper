"""
MySQL Process Scraper
Collects slow query information from MySQL performance_schema
"""

import asyncio
import pytz
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass
from threading import Lock
from modules.mongodb_connector import mongodb
from modules.mysql_connector import MySQLConnector
from configs.mysql_conf import MySQLSettings
from configs.mongo_conf import mongo_settings
from configs.base_config import config
from configs.scraper_conf import scraper_settings
import logging

logger = logging.getLogger(__name__)

# Get configuration values from scraper settings
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


class ProcessListCache:
    def __init__(self):
        self._cache: Dict[int, Dict[str, Any]] = {}
        self._lock = Lock()

    def get(self, pid: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._cache.get(pid)

    def set(self, pid: int, data: Dict[str, Any]) -> None:
        with self._lock:
            self._cache[pid] = data

    def pop(self, pid: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._cache.pop(pid, None)

    def keys(self):
        with self._lock:
            return list(self._cache.keys())


class SlowQueryMonitor:
    def __init__(self, mysql_connector: MySQLConnector):
        self.pid_time_cache = ProcessListCache()
        self.logger = logging.getLogger(__name__)
        self.mysql_connector = mysql_connector
        self._stop_event = asyncio.Event()
        self.collection = None

    @staticmethod
    async def create_mysql_connector(endpoint: Dict[str, Any], instance_name: str) -> MySQLConnector:
        """RDS 엔드포인트 정보로 MySQL 커넥터 생성"""
        settings = MySQLSettings(
            host=endpoint['Address'],
            port=endpoint['Port'],
            user=config.get('MGMT_USER', 'mysql_mgmt'),
            password=config.get('MGMT_USER_PASS'),
            database='performance_schema'
        )

        mysql_conn = MySQLConnector(instance_name)
        await mysql_conn.create_pool(settings)
        return mysql_conn

    async def stop(self):
        self._stop_event.set()
        logger.info(f"Stopping SlowQueryMonitor for {self.mysql_connector.instance_name}")

    async def initialize(self):
        db = await mongodb.get_database()
        self.collection = db[mongo_settings.MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION]
        logger.info(f"Initialized SlowQueryMonitor for {self.mysql_connector.instance_name}")

    async def query_mysql_instance(self) -> None:
        try:
            # SQL 쿼리에 설정값 적용
            excluded_dbs = ','.join(f"'{db}'" for db in EXCLUDED_DBS)
            excluded_users = ','.join(f"'{user}'" for user in EXCLUDED_USERS)

            sql_query = f"""
                SELECT `ID`, `DB`, `USER`, `HOST`, `TIME`, `INFO`
                FROM `performance_schema`.`processlist`
                WHERE info IS NOT NULL
                AND DB not in ({excluded_dbs})
                AND USER not in ({excluded_users})
                ORDER BY `TIME` DESC"""

            result = await self.mysql_connector.fetch_all(sql_query)

            current_pids = set()
            for row in result:
                await self.process_query_result(row, current_pids)

            await self.handle_finished_queries(current_pids)

        except Exception as e:
            self.logger.error(f"Error querying MySQL instance {self.mysql_connector.instance_name}: {e}")

    async def process_query_result(self, row: Dict[str, Any], current_pids: set) -> None:
        pid, db, user, host, time, info = row['ID'], row['DB'], row['USER'], row['HOST'], row['TIME'], row['INFO']
        current_pids.add(pid)

        if time >= EXEC_TIME:
            existing_cache = self.pid_time_cache.get(pid)

            new_cache = {
                'max_time': max(existing_cache['max_time'], time) if existing_cache else time
            }

            if existing_cache and 'start' in existing_cache:
                new_cache['start'] = existing_cache['start']
            else:
                utc_now = datetime.now(pytz.utc)
                utc_start_timestamp = int((utc_now - timedelta(seconds=EXEC_TIME)).timestamp())
                utc_start_datetime = datetime.fromtimestamp(utc_start_timestamp, pytz.utc)
                new_cache['start'] = utc_start_datetime

            info_cleaned = re.sub(' +', ' ', info).encode('utf-8', 'ignore').decode('utf-8')
            info_cleaned = re.sub(r'[\n\t\r]+', ' ', info_cleaned).strip()

            new_cache['details'] = QueryDetails(
                instance=self.mysql_connector.instance_name,
                db=db,
                pid=pid,
                user=user,
                host=host,
                time=time,
                sql_text=info_cleaned,
                start=new_cache['start']
            )

            self.pid_time_cache.set(pid, new_cache)

    async def handle_finished_queries(self, current_pids: set) -> None:
        for pid in self.pid_time_cache.keys():
            if pid not in current_pids:
                cache_data = self.pid_time_cache.pop(pid)
                if cache_data:
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
                        self.logger.info(
                            f"Inserted slow query data: instance={self.mysql_connector.instance_name}, "
                            f"DB={data_to_insert['db']}, PID={pid}, execution_time={data_to_insert['time']}s"
                        )

    async def run_mysql_slow_queries(self) -> None:
        try:
            self.logger.info(f"Starting slow query monitoring for {self.mysql_connector.instance_name}")

            while not self._stop_event.is_set():
                await self.query_mysql_instance()
                # 설정된 모니터링 간격 사용
                await asyncio.sleep(MONITORING_INTERVAL)

        except asyncio.CancelledError:
            self.logger.info(f"Slow query monitoring task was cancelled for {self.mysql_connector.instance_name}")
        except Exception as e:
            self.logger.error(
                f"An error occurred in slow query monitoring for {self.mysql_connector.instance_name}: {e}")
        finally:
            self.logger.info(f"Slow query monitoring stopped for {self.mysql_connector.instance_name}")


async def main():
    monitors = []
    try:
        await mongodb.initialize()

        db = await mongodb.get_database()
        collection = db[mongo_settings.MONGO_RDS_INSTANCE_COLLECTION]
        latest_instances = await collection.find_one(
            sort=[('timestamp', -1)]
        )

        if not latest_instances:
            logger.error("No RDS instances found in MongoDB")
            return

        for instance in latest_instances['instances']:
            if instance.get('Endpoint'):
                try:
                    mysql_conn = await SlowQueryMonitor.create_mysql_connector(
                        instance['Endpoint'],
                        instance['DBInstanceIdentifier']
                    )
                    monitor = SlowQueryMonitor(mysql_conn)
                    await monitor.initialize()
                    monitors.append(monitor)
                except Exception as e:
                    logger.error(f"Failed to initialize monitor for {instance['DBInstanceIdentifier']}: {e}")
                    continue

        if not monitors:
            logger.error("No monitors could be initialized")
            return

        tasks = [monitor.run_mysql_slow_queries() for monitor in monitors]
        await asyncio.gather(*tasks)

    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        for monitor in monitors:
            await monitor.stop()
        await mongodb.close()


if __name__ == '__main__':
    asyncio.run(main())