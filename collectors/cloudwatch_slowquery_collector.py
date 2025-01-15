# collectors/cloudwatch_slowquery_collector.py

import logging
import re
import os
import sys
import pytz
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Pattern
from modules.load_instance import InstanceLoader
from modules.aws_session_manager import aws_session
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
kst = pytz.timezone('Asia/Seoul')


class RDSCloudWatchSlowQueryCollector:
    """RDS CloudWatch 슬로우 쿼리 수집기"""

    def __init__(self):
        """초기화"""
        self._instance_loader: InstanceLoader = InstanceLoader()
        self._query_pattern: Pattern = re.compile(
            r"# User@Host: (?P<user>.*?)\[.*?\] @ (?P<host>.*?)"
            r"# Query_time: (?P<query_time>\d+\.\d+)\s+"
            r"Lock_time: (?P<lock_time>\d+\.\d+)\s+"
            r"Rows_sent: (?P<rows_sent>\d+)\s+"
            r"Rows_examined: (?P<rows_examined>\d+)"
            r".*?SET timestamp=(?P<timestamp>\d+);"
            r"(?P<query>.*?)(?=# User@Host:|$)",
            re.DOTALL
        )
        self._target_instances: Optional[List[str]] = None

    @property
    def collection_name(self) -> str:
        """MongoDB 컬렉션 이름 반환"""
        return mongo_settings.MONGO_CW_MYSQL_SLOW_SQL_COLLECTION

    async def initialize(self) -> None:
        """초기화 작업 수행"""
        # 모든 인스턴스 로드
        instances = await self._instance_loader.load_all_instances()
        self._target_instances = [inst['instance_name'] for inst in instances]

        if self._target_instances:
            logger.info(f"수집 대상 인스턴스: {', '.join(self._target_instances)}")
        else:
            logger.warning("수집 대상 인스턴스가 설정되지 않았습니다.")

    @staticmethod
    async def _get_slow_query_logs(
            instance_name: str,
            region: str,
            start_date: datetime,
            end_date: datetime,
            batch_size: int = 10000,
            max_concurrent_streams: int = 5
    ) -> List[Dict]:
        try:
            logs_client = aws_session.get_client('logs', region)
            log_group_name = f"/aws/rds/instance/{instance_name}/slowquery"

            # KST를 UTC로 변환
            utc_start = start_date.astimezone(pytz.UTC)
            utc_end = end_date.astimezone(pytz.UTC)

            logger.info(
                f"로그 조회 시작 - UTC 시간: {utc_start.strftime('%Y-%m-%d %H:%M:%S')} ~ "
                f"{utc_end.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            try:
                streams_response = await asyncio.to_thread(
                    logs_client.describe_log_streams,
                    logGroupName=log_group_name,
                    orderBy='LastEventTime',
                    descending=True,
                    limit=50
                )
            except Exception as e:
                logger.warning(f"로그 스트림 조회 실패 ({log_group_name}): {e}")
                return []

            log_streams = streams_response.get('logStreams', [])
            if not log_streams:
                logger.info(f"로그 스트림이 없습니다: {log_group_name}")
                return []

            async def process_stream(stream: Dict) -> List[Dict]:
                """단일 로그 스트림 처리"""
                stream_events = []
                next_token = None

                while True:
                    try:
                        params = {
                            'logGroupName': log_group_name,
                            'logStreamName': stream['logStreamName'],
                            'startTime': int(utc_start.timestamp() * 1000),  # UTC 시간으로 변경
                            'endTime': int(utc_end.timestamp() * 1000),  # UTC 시간으로 변경
                            'limit': batch_size
                        }

                        if next_token:
                            params['nextToken'] = next_token

                        events_response = await asyncio.to_thread(
                            logs_client.get_log_events,
                            **params
                        )

                        if events := events_response.get('events'):
                            stream_events.extend(events)

                        next_token = events_response.get('nextForwardToken')
                        if (not events or
                                next_token == params.get('nextToken') or
                                len(stream_events) >= batch_size):
                            break

                        # 메모리 사용량 모니터링
                        if len(stream_events) > batch_size * 2:
                            logger.warning(
                                f"스트림 {stream['logStreamName']}의 "
                                f"이벤트가 많습니다. 처리를 중단합니다."
                            )
                            break

                    except Exception as e:
                        logger.error(
                            f"스트림 {stream['logStreamName']} 처리 중 오류 발생: {e}"
                        )
                        break

                return stream_events

            # 스트림 병렬 처리
            all_events = []
            for i in range(0, len(log_streams), max_concurrent_streams):
                batch_streams = log_streams[i:i + max_concurrent_streams]
                tasks = [process_stream(stream) for stream in batch_streams]

                try:
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                    for result in batch_results:
                        if isinstance(result, Exception):
                            logger.error(f"스트림 처리 중 오류 발생: {result}")
                            continue
                        if isinstance(result, list):
                            all_events.extend(result)
                        else:
                            logger.error(f"예상치 못한 결과 타입: {type(result)}")
                except Exception as e:
                    logger.error(f"배치 처리 중 오류 발생: {e}")
                    continue

            # 시간순 정렬
            all_events.sort(key=lambda x: x.get('timestamp', 0))

            logger.info(
                f"총 {len(all_events)}개의 로그 이벤트 수집 완료 "
                f"(스트림 {len(log_streams)}개)"
            )
            return all_events

        except Exception as e:
            logger.error(f"로그 조회 중 오류 발생: {e}")
            return []

    def _analyze_slow_queries(self, logs: List[Dict]) -> List[Dict]:
        """슬로우 쿼리 로그 분석"""
        query_stats = {}
        total_query_count = len(logs)
        processed_query_count = 0
        excluded_users = {'rdsadmin', 'event_scheduler'}

        for log in logs:
            match = self._query_pattern.search(log.get('message', ''))
            if not match:
                continue

            data = match.groupdict()
            if any(user in data['user'].lower() for user in excluded_users):
                continue

            normalized_query = self._normalize_query(data['query'])
            processed_query_count += 1

            if normalized_query not in query_stats:
                query_stats[normalized_query] = {
                    'digest_query': normalized_query,
                    'example_queries': set(),
                    'execution_count': 0,
                    'total_time': 0.0,
                    'lock_time': 0.0,
                    'rows_sent': 0,
                    'rows_examined': 0,
                    'users': set(),
                    'hosts': set(),
                    'first_seen': None,
                    'last_seen': None
                }

            stats = query_stats[normalized_query]
            stats['execution_count'] += 1
            stats['total_time'] += float(data['query_time'])
            stats['lock_time'] += float(data['lock_time'])
            stats['rows_sent'] += int(data['rows_sent'])
            stats['rows_examined'] += int(data['rows_examined'])
            stats['users'].add(data['user'])
            stats['hosts'].add(data['host'])

            if len(stats['example_queries']) < 10:
                stats['example_queries'].add(data['query'].strip())

            timestamp = datetime.fromtimestamp(int(data['timestamp']))
            if not stats['first_seen'] or timestamp < stats['first_seen']:
                stats['first_seen'] = timestamp
            if not stats['last_seen'] or timestamp > stats['last_seen']:
                stats['last_seen'] = timestamp

        system_query_count = total_query_count - processed_query_count
        logger.info(
            f"전체 쿼리 수: {total_query_count}, "
            f"시스템 계정 쿼리 수: {system_query_count}, "
            f"처리된 쿼리 수: {processed_query_count}, "
            f"고유 다이제스트 수: {len(query_stats)}"
        )

        results = []
        for stats in query_stats.values():
            results.append({
                'digest_query': stats['digest_query'],
                'example_queries': list(stats['example_queries']),
                'execution_count': stats['execution_count'],
                'avg_time': stats['total_time'] / stats['execution_count'],
                'total_time': stats['total_time'],
                'avg_lock_time': stats['lock_time'] / stats['execution_count'],
                'avg_rows_sent': stats['rows_sent'] / stats['execution_count'],
                'avg_rows_examined': stats['rows_examined'] / stats['execution_count'],
                'users': list(stats['users']),
                'hosts': list(stats['hosts']),
                'first_seen': stats['first_seen'].isoformat(),
                'last_seen': stats['last_seen'].isoformat()
            })

        return sorted(results, key=lambda x: x['avg_time'], reverse=True)

    @staticmethod
    def _normalize_query(query: str) -> str:
        """
        쿼리 정규화 (변수 값을 플레이스홀더로 대체)

        Args:
            query (str): 정규화할 원본 쿼리문

        Returns:
            str: 정규화된 쿼리문
        """
        # 문자열 리터럴 제거
        query = re.sub(r"'[^']*'", "?", query)
        query = re.sub(r'"[^"]*"', "?", query)

        # 숫자 리터럴 제거
        query = re.sub(r'\b\d+\b', "?", query)

        # 불필요한 공백 제거
        query = " ".join(query.split())

        return query

    async def collect_metrics_by_range(
            self,
            start_date: datetime,
            end_date: datetime,
            chunk_size: int = 5
    ) -> Dict[str, List[Dict]]:
        """
        날짜 범위의 슬로우 쿼리 수집

        Args:
            start_date (datetime): 수집 시작일
            end_date (datetime): 수집 종료일
            chunk_size (int): 동시 처리할 인스턴스 수

        Returns:
            Dict[str, List[Dict]]: 인스턴스별 수집된 슬로우 쿼리
        """
        try:
            if not self._target_instances:
                logger.warning("수집 대상 인스턴스가 없습니다")
                return {}

            logger.info(
                f"슬로우 쿼리 수집 시작 - "
                f"기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}"
            )

            all_instances = await self._instance_loader.load_all_instances()
            target_instances = [
                inst for inst in all_instances
                if inst['instance_name'] in self._target_instances and inst.get('region')
            ]

            if not target_instances:
                logger.warning("유효한 수집 대상 인스턴스가 없습니다")
                return {}

            # 인스턴스 청크 단위로 병렬 처리
            instances_data = {}
            for i in range(0, len(target_instances), chunk_size):
                chunk = target_instances[i:i + chunk_size]

                async def process_instance(instance: Dict) -> Tuple[str, List[Dict]]:
                    """단일 인스턴스 처리"""
                    try:
                        logs = await self._get_slow_query_logs(
                            instance_name=instance['instance_name'],
                            region=instance['region'],
                            start_date=start_date,
                            end_date=end_date
                        )

                        if not logs:
                            logger.info(
                                f"- {instance['instance_name']}: "
                                f"수집된 슬로우 쿼리 없음"
                            )
                            return instance['instance_name'], []

                        analyzed_queries = self._analyze_slow_queries(logs)
                        if analyzed_queries:
                            logger.info(
                                f"✓ {instance['instance_name']}: "
                                f"{len(analyzed_queries)}개의 슬로우 쿼리 분석 완료"
                            )
                            return instance['instance_name'], analyzed_queries

                        return instance['instance_name'], []

                    except Exception as e:
                        logger.error(
                            f"인스턴스 {instance['instance_name']} 처리 중 오류: {e}"
                        )
                        return instance['instance_name'], []

                # 청크 단위 병렬 처리
                tasks = [process_instance(instance) for instance in chunk]
                try:
                    results = await asyncio.gather(*tasks)
                    for instance_name, queries in results:
                        if queries:
                            instances_data[instance_name] = queries
                except Exception as e:
                    logger.error(f"청크 처리 중 오류 발생: {e}")
                    continue

            if not instances_data:
                logger.warning("수집된 슬로우 쿼리가 없습니다")
                return {}

            # 날짜별 데이터 저장
            current_date = start_date
            while current_date <= end_date:
                try:
                    await self._save_metrics(
                        instances_data=instances_data,
                        target_date=current_date.date()
                    )
                    logger.info(f"{current_date.strftime('%Y-%m-%d')} 데이터 저장 완료")
                except Exception as e:
                    logger.error(
                        f"{current_date.strftime('%Y-%m-%d')} "
                        f"데이터 저장 중 오류: {e}"
                    )
                current_date += timedelta(days=1)

            return instances_data

        except Exception as e:
            logger.error(f"슬로우 쿼리 수집 중 오류 발생: {e}")
            raise

    async def _save_metrics(
            self,
            instances_data: Dict[str, List[Dict]],
            target_date: datetime.date
    ) -> None:
        """
        슬로우 쿼리 데이터를 MongoDB에 저장
        digest_query를 기준으로 각각의 도큐먼트로 저장

        Args:
            instances_data (Dict[str, List[Dict]]): 인스턴스별 슬로우 쿼리 데이터
            target_date (datetime.date): 수집 대상 날짜
        """
        try:
            db = await MongoDBConnector.get_database()
            collection = db[self.collection_name]

            for instance_id, queries in instances_data.items():
                # digest_query별로 도큐먼트 생성
                for query_data in queries:
                    document = {
                        "date": target_date.strftime('%Y-%m-%d'),
                        "instance_id": instance_id,
                        "created_at": datetime.now(kst).isoformat(),
                        "digest_query": query_data['digest_query'],
                        "example_queries": query_data['example_queries'],
                        "execution_count": query_data['execution_count'],
                        "avg_time": query_data['avg_time'],
                        "total_time": query_data['total_time'],
                        "avg_lock_time": query_data['avg_lock_time'],
                        "avg_rows_sent": query_data['avg_rows_sent'],
                        "avg_rows_examined": query_data['avg_rows_examined'],
                        "users": query_data['users'],
                        "hosts": query_data['hosts'],
                        "first_seen": query_data['first_seen'],
                        "last_seen": query_data['last_seen']
                    }

                    filter_doc = {
                        "date": document["date"],
                        "instance_id": instance_id,
                        "digest_query": query_data['digest_query']
                    }

                    try:
                        result = await collection.update_one(
                            filter_doc,
                            {"$set": document},
                            upsert=True
                        )

                        operation = "업데이트" if result.modified_count else "생성"
                        logger.debug(
                            f"인스턴스 {instance_id}의 "
                            f"digest_query 도큐먼트 {operation} 완료"
                        )

                    except Exception as e:
                        logger.error(f"인스턴스 {instance_id}의 슬로우 쿼리 저장 실패: {str(e)}")
                        continue

        except Exception as e:
            logger.error(f"MongoDB 저장 중 오류 발생: {e}")
            raise