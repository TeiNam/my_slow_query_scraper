"""
RDS MySQL Slow Query Explain Collector
슬로우 쿼리의 실행 계획을 수집하여 MongoDB에 저장
"""

import logging
import re
import json
import asyncio
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from modules.mongodb_connector import MongoDBConnector
from modules.mysql_connector import MySQLConnector
from modules.load_instance import InstanceLoader
from configs.mongo_conf import mongo_settings

logger = logging.getLogger(__name__)


class ExplainCollector:
    """RDS MySQL Slow Query Explain Collector"""

    def __init__(self):
        """초기화"""
        self._instance_loader = InstanceLoader()

    @staticmethod
    def remove_sql_comments(sql_text: str) -> str:
        """SQL 쿼리에서 주석 제거"""
        return re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)

    @staticmethod
    def validate_sql_query(sql_text: str) -> str:
        """SQL 쿼리 유효성 검증"""
        query_without_comments = ExplainCollector.remove_sql_comments(sql_text).strip()
        if not query_without_comments.lower().startswith("select"):
            raise ValueError("SELECT 쿼리만 가능합니다.")
        return query_without_comments

    async def get_instance_info(self, instance_name: str) -> Optional[Dict[str, Any]]:
        """인스턴스 정보 조회"""
        instances = await self._instance_loader.load_all_instances()
        return next((inst for inst in instances if inst['instance_name'] == instance_name), None)

    async def collect_explain_by_pid(self, pid: int) -> None:
        """
        PID 기반 슬로우 쿼리 실행 계획 수집

        Args:
            pid (int): 프로세스 ID
        """
        try:
            mongodb = await MongoDBConnector.get_database()
            slow_query_collection = mongodb[mongo_settings.MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION]
            plan_collection = mongodb[mongo_settings.MONGO_SLOW_LOG_PLAN_COLLECTION]

            # PID로 슬로우 쿼리 조회
            document = await slow_query_collection.find_one({"pid": pid})
            if not document:
                logger.error(f"PID {pid}에 해당하는 슬로우 쿼리를 찾을 수 없습니다")
                return

            # 인스턴스 정보 조회
            instance_info = await self.get_instance_info(document['instance'])
            if not instance_info:
                logger.error(f"인스턴스 정보를 찾을 수 없습니다: {document['instance']}")
                return

            # MySQL 연결 설정
            mysql_conn = MySQLConnector(instance_info['instance_name'])
            json_execution_plan = None
            tree_execution_plan = None

            # SQL 검증 및 실행 계획 수집
            sql_text = document['sql_text']
            try:
                validated_sql = self.validate_sql_query(sql_text)

                # JSON 형식 실행 계획 수집
                json_explain_query = f"EXPLAIN FORMAT=JSON {validated_sql}"
                json_execution_plan = await mysql_conn.execute_query_with_new_connection(
                    {
                        "host": instance_info['host'],
                        "port": instance_info['port'],
                        "user": instance_info['mgmt_user'],
                        "password": instance_info['mgmt_password'],
                        "db": document['db'],
                        "charset": 'utf8mb4'
                    },
                    json_explain_query
                )

                # TREE 형식 실행 계획 수집
                tree_explain_query = f"EXPLAIN FORMAT=TREE {validated_sql}"
                tree_execution_plan = await mysql_conn.execute_query_with_new_connection(
                    {
                        "host": instance_info['host'],
                        "port": instance_info['port'],
                        "user": instance_info['mgmt_user'],
                        "password": instance_info['mgmt_password'],
                        "db": document['db'],
                        "charset": 'utf8mb4'
                    },
                    tree_explain_query
                )

                if not json_execution_plan or 'EXPLAIN' not in json_execution_plan[0]:
                    logger.error(f"JSON EXPLAIN 결과가 예상된 형식이 아님: {pid}")
                    return

                if not tree_execution_plan or 'EXPLAIN' not in tree_execution_plan[0]:
                    logger.error(f"TREE EXPLAIN 결과가 예상된 형식이 아님: {pid}")
                    return

                # 실행 계획 저장
                plan_document = {
                    "pid": pid,
                    "instance": document['instance'],
                    "db": document['db'],
                    "user": document['user'],
                    "host": document['host'],
                    "time": document['time'],
                    "start": document['start'],
                    "end": document['end'],
                    "sql_text": self.remove_sql_comments(sql_text),
                    "explain_result": {
                        "json": json.loads(json_execution_plan[0]['EXPLAIN']),
                        "tree": tree_execution_plan[0]['EXPLAIN']
                    },
                    "created_at": datetime.now()
                }

                await plan_collection.update_one(
                    {"pid": pid},
                    {"$set": plan_document},
                    upsert=True
                )

                logger.info(f"PID {pid}의 실행 계획 수집 완료 (JSON & TREE)")

            except Exception as e:
                logger.error(f"쿼리 실행 계획 수집 중 오류 발생: {str(e)}")
                raise

        except Exception as e:
            logger.error(f"실행 계획 수집 중 오류 발생: {str(e)}")
            raise


async def collect_explain(pid: int) -> None:
    """
    슬로우 쿼리 실행 계획 수집 실행

    Args:
        pid (int): 프로세스 ID
    """
    try:
        # MongoDB 연결
        await MongoDBConnector.initialize()

        try:
            collector = ExplainCollector()
            await collector.collect_explain_by_pid(pid)

        except Exception as e:
            logger.error(f"실행 계획 수집 중 오류 발생: {e}")
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
    import argparse

    parser = argparse.ArgumentParser(description='MySQL 슬로우 쿼리 실행 계획 수집기')
    parser.add_argument(
        '--pid',
        type=int,
        required=True,
        help='수집할 슬로우 쿼리의 PID'
    )

    args = parser.parse_args()

    # Windows에서 실행 시 필요한 설정
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # 비동기 이벤트 루프 실행
    asyncio.run(collect_explain(args.pid))