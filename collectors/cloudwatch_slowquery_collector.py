# collectors/cloudwatch_slowquery_collector.py

import logging
import re
import os
import sys
import pytz
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
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
        self._instance_loader = InstanceLoader()
        self._query_pattern = re.compile(
            r"# User@Host: (?P<user>.*?)\[.*?\] @ (?P<host>.*?)"
            r"# Query_time: (?P<query_time>\d+\.\d+)\s+"
            r"Lock_time: (?P<lock_time>\d+\.\d+)\s+"
            r"Rows_sent: (?P<rows_sent>\d+)\s+"
            r"Rows_examined: (?P<rows_examined>\d+)"
            r".*?SET timestamp=(?P<timestamp>\d+);"
            r"(?P<query>.*?)(?=# User@Host:|$)",
            re.DOTALL
        )
        self.target_instances = None

    @property
    def collection_name(self) -> str:
        """MongoDB 컬렉션 이름 반환"""
        return mongo_settings.MONGO_CW_MYSQL_SLOW_SQL_COLLECTION

    async def initialize(self) -> None:
        """초기화 작업 수행"""
        # 모든 인스턴스 로드
        instances = await self._instance_loader.load_all_instances()
        self.target_instances = [inst['instance_name'] for inst in instances]

        if self.target_instances:
            logger.info(f"수집 대상 인스턴스: {', '.join(self.target_instances)}")
        else:
            logger.warning("수집 대상 인스턴스가 설정되지 않았습니다.")

    async def collect_metrics(
            self,
            collect_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        일 단위 슬로우 쿼리 수집

        Args:
            collect_date (datetime.datetime | None): 수집 대상 날짜 (기본값: 어제)

        Returns:
            Dict[str, Any]: 인스턴스별 수집된 슬로우 쿼리
        """
        try:
            if not self.target_instances:
                logger.warning("수집 대상 인스턴스가 없습니다")
                return {}

            if collect_date is None:
                collect_date = datetime.now(kst).date() - timedelta(days=1)
            elif isinstance(collect_date, datetime):
                collect_date = collect_date.date()

            start_date = datetime.combine(collect_date, datetime.min.time())
            end_date = datetime.combine(collect_date, datetime.max.time())

            logger.info(f"{collect_date.strftime('%Y-%m-%d')} 슬로우 쿼리 수집 시작")

            instances_data = {}
            all_instances = await self._instance_loader.load_all_instances()

            for instance in all_instances:
                if instance['instance_name'] not in self.target_instances:
                    continue

                if not instance.get('region'):
                    logger.error(f"인스턴스 {instance['instance_name']}의 region 정보가 없습니다")
                    continue

                logger.info(f"인스턴스 {instance['instance_name']} 슬로우 쿼리 수집 시작")

                try:
                    logs = await self._get_slow_query_logs(
                        instance_name=instance['instance_name'],
                        region=instance['region'],
                        start_date=start_date,
                        end_date=end_date
                    )

                    if logs:
                        analyzed_queries = self._analyze_slow_queries(logs)
                        if analyzed_queries:
                            instances_data[instance['instance_name']] = analyzed_queries
                            logger.info(
                                f"✓ {instance['instance_name']}: "
                                f"{len(analyzed_queries)} 개의 슬로우 쿼리 분석 완료"
                            )
                    else:
                        logger.info(
                            f"- {instance['instance_name']}: "
                            f"수집된 슬로우 쿼리 없음"
                        )

                except Exception as e:
                    logger.error(f"인스턴스 {instance['instance_name']} 처리 중 오류 발생: {e}")
                    continue

            if not instances_data:
                logger.warning("수집된 슬로우 쿼리가 없습니다")
                return {}

            await self._save_metrics(
                instances_data=instances_data,
                target_date=collect_date
            )

            return instances_data

        except Exception as e:
            logger.error(f"슬로우 쿼리 수집 중 오류 발생: {e}")
            raise

    @staticmethod
    async def _get_slow_query_logs(
            instance_name: str,
            region: str,
            start_date: datetime,
            end_date: datetime
    ) -> List[Dict]:
        """
        CloudWatch Logs에서 슬로우 쿼리 로그 조회

        Args:
            instance_name (str): RDS 인스턴스 식별자
            region (str): AWS 리전
            start_date (datetime.datetime): 조회 시작 시간
            end_date (datetime.datetime): 조회 종료 시간

        Returns:
            List[Dict]: 수집된 로그 이벤트 목록
        """
        try:
            logs_client = aws_session.get_client('logs', region)
            log_group_name = f"/aws/rds/instance/{instance_name}/slowquery"

            try:
                # 로그 스트림 조회
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

            log_events = []
            next_token = None

            # 모든 로그 이벤트 수집
            for stream in streams_response.get('logStreams', []):
                while True:
                    try:
                        params = {
                            'logGroupName': log_group_name,
                            'logStreamName': stream['logStreamName'],
                            'startTime': int(start_date.timestamp() * 1000),
                            'endTime': int(end_date.timestamp() * 1000),
                            'limit': 10000
                        }

                        if next_token:
                            params['nextToken'] = next_token

                        events_response = await asyncio.to_thread(
                            logs_client.get_log_events,
                            **params
                        )

                        if events_response.get('events'):
                            log_events.extend(events_response['events'])

                        next_token = events_response.get('nextForwardToken')

                        if not events_response.get('events') or next_token == params.get('nextToken'):
                            break

                    except Exception as e:
                        logger.warning(f"로그 이벤트 조회 실패 ({stream['logStreamName']}): {e}")
                        break

            logger.info(f"총 {len(log_events)}개의 로그 이벤트 수집됨")
            return log_events

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


async def collect_slow_queries(collect_date: Optional[datetime] = None) -> None:
    """
    RDS 슬로우 쿼리 수집 실행

    Args:
        collect_date: 수집할 날짜 (기본값: 어제)
    """
    try:
        if collect_date is None:
            collect_date = datetime.now(kst) - timedelta(days=1)

        # MongoDB 연결
        await MongoDBConnector.initialize()

        try:
            # 슬로우 쿼리 수집기 생성 및 초기화
            collector = RDSCloudWatchSlowQueryCollector()
            await collector.initialize()

            instances_data = await collector.collect_metrics(collect_date)

            # 결과 요약
            collected_instances = sorted(instances_data.keys())

            logger.info(
                f"{collect_date.strftime('%Y-%m-%d')} 슬로우 쿼리 수집 완료: "
                f"{len(instances_data)}개 인스턴스"
            )

            if collected_instances:
                logger.info(f"수집된 인스턴스: {', '.join(collected_instances)}")
            else:
                logger.warning("수집된 인스턴스가 없습니다")

        except Exception as e:
            logger.error(f"슬로우 쿼리 수집 중 오류 발생: {e}")
            raise

        finally:
            try:
                await MongoDBConnector.close()
            except Exception as e:
                logger.error(f"MongoDB 연결 종료 중 오류 발생: {e}")

    except Exception as e:
        logger.error(f"프로그램 실행 중 오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    import asyncio
    import argparse
    from datetime import datetime, timedelta

    # 커맨드 라인 인자 파서 설정
    parser = argparse.ArgumentParser(description='RDS 슬로우 쿼리 수집기')
    parser.add_argument(
        '--date',
        type=str,
        help='수집할 날짜 (YYYY-MM-DD 형식). 미입력시 어제 날짜 사용'
    )
    parser.add_argument(
        '--env',
        type=str,
        default='prd',
        choices=['dev', 'prd'],
        help='실행 환경 (기본값: prd)'
    )

    args = parser.parse_args()

    # 날짜 처리
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            logger.error(f"잘못된 날짜 형식: {args.date} (YYYY-MM-DD 형식으로 입력)")
            sys.exit(1)
    else:
        # 환경 변수에서 날짜 정보 가져오기 (옵션)
        target_date_str = os.getenv('TARGET_DATE')
        if target_date_str:
            try:
                target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
            except ValueError:
                logger.error(f"잘못된 날짜 형식: {target_date_str} (YYYY-MM-DD 형식으로 입력)")
                sys.exit(1)
        else:
            # 기본값: 어제
            target_date = datetime.now(kst) - timedelta(days=1)

    # 환경 설정
    os.environ['ENV'] = args.env

    # 로깅
    logger.info(f"일간 수집 시작: 날짜={target_date.strftime('%Y-%m-%d')}, 환경={args.env}")

    # Windows에서 실행 시 필요한 설정
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # 비동기 이벤트 루프 실행
    asyncio.run(collect_slow_queries(target_date))