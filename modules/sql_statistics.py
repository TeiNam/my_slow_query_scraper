"""
SQL Statistics Calculator
CloudWatch 슬로우 쿼리 통계 계산 및 저장
"""

import logging
from typing import Dict, Any, List
from datetime import datetime
from modules.mongodb_connector import mongodb
from configs.mongo_conf import mongo_settings

logger = logging.getLogger(__name__)

class SQLStatisticsCalculator:
    """SQL 통계 계산기"""

    @staticmethod
    def _clean_query(query: str) -> str:
        """SQL 쿼리에서 힌트와 주석 제거"""
        query = query.strip()

        # /* */ 형식의 힌트/주석 제거
        while '/*' in query and '*/' in query:
            start = query.find('/*')
            end = query.find('*/') + 2
            query = query[:start] + query[end:]
            query = query.strip()

        # -- 형식의 주석 제거
        query = '\n'.join(
            line.split('--')[0].strip()
            for line in query.splitlines()
        ).strip()

        return query

    @staticmethod
    def _is_read_query(query: str) -> bool:
        """읽기 쿼리 여부 확인"""
        cleaned_query = SQLStatisticsCalculator._clean_query(query)
        if not cleaned_query:
            return False
        return cleaned_query.upper().startswith(('SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN'))

    @staticmethod
    def _is_write_query(query: str) -> bool:
        """쓰기 쿼리 여부 확인"""
        cleaned_query = SQLStatisticsCalculator._clean_query(query)
        if not cleaned_query:
            return False
        return cleaned_query.upper().startswith(('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'UPSERT'))

    @staticmethod
    def _is_ddl_query(query: str) -> bool:
        """DDL 쿼리 여부 확인"""
        cleaned_query = SQLStatisticsCalculator._clean_query(query)
        if not cleaned_query:
            return False
        return cleaned_query.upper().startswith(('CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'RENAME'))

    async def calculate_monthly_statistics(self, year_month: str) -> List[Dict[str, Any]]:
        """
        월간 인스턴스별 SQL 통계 계산 및 저장

        Args:
            year_month: YYYY-MM 형식의 년월
        """
        try:
            db = await mongodb.get_database()
            source_collection = db[mongo_settings.MONGO_CW_MYSQL_SLOW_SQL_COLLECTION]

            # 기본 집계 파이프라인
            pipeline = [
                {
                    "$match": {
                        "date": {"$regex": f"^{year_month}"}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "instance_id": "$instance_id",
                            "month": {"$substr": ["$date", 0, 7]}
                        },
                        "unique_digests": {"$addToSet": "$digest_query"},
                        "total_exec_count": {"$sum": "$execution_count"},
                        "total_exec_time": {"$sum": "$total_time"},
                        "total_rows_examined": {"$sum": {"$multiply": ["$avg_rows_examined", "$execution_count"]}},
                        "queries": {"$push": "$digest_query"}
                    }
                }
            ]

            results = []
            async for group in source_collection.aggregate(pipeline):
                instance_id = group["_id"]["instance_id"]
                month = group["_id"]["month"]

                # 쿼리 타입 분류 (힌트/주석 제거 후)
                queries = group["queries"]
                read_queries = sum(1 for q in queries if self._is_read_query(q))
                write_queries = sum(1 for q in queries if self._is_write_query(q))
                ddl_queries = sum(1 for q in queries if self._is_ddl_query(q))
                commit_queries = sum(1 for q in queries if self._is_commit_query(q))  # commit 쿼리 카운트 추가

                stat = {
                    "instance_id": instance_id,
                    "month": month,
                    "unique_digest_count": len(group["unique_digests"]),
                    "total_slow_query_count": len(queries),
                    "total_execution_count": group["total_exec_count"],
                    "total_execution_time": round(group["total_exec_time"], 2),
                    "avg_execution_time": round(group["total_exec_time"] / group["total_exec_count"], 2) if group[
                                                                                                                "total_exec_count"] > 0 else 0,
                    "total_rows_examined": int(group["total_rows_examined"]),
                    "read_query_count": read_queries,
                    "write_query_count": write_queries,
                    "ddl_query_count": ddl_queries,
                    "commit_query_count": commit_queries,  # commit 쿼리 수 필드 추가
                    "created_at": datetime.utcnow()
                }
                results.append(stat)

            # 통계 저장
            if results:
                stats_collection = db[mongo_settings.MONGO_CW_SQL_STATISTICS]
                await stats_collection.delete_many({"month": year_month})
                await stats_collection.insert_many(results)
                logger.info(f"{year_month} 통계 계산 및 저장 완료: {len(results)}개 인스턴스")
            else:
                logger.warning(f"{year_month} 통계 계산 결과 없음")

            return results

        except Exception as e:
            logger.error(f"SQL 통계 계산 중 오류 발생: {e}")
            raise

    @staticmethod
    def _is_commit_query(query: str) -> bool:
        """커밋 관련 쿼리 여부 확인"""
        cleaned_query = SQLStatisticsCalculator._clean_query(query)
        if not cleaned_query:
            return False
        return cleaned_query.upper().startswith(('COMMIT', 'ROLLBACK', 'BEGIN', 'START TRANSACTION'))

    async def calculate_user_statistics(self, year_month: str) -> List[Dict[str, Any]]:
        """
        월간 인스턴스별 사용자 SQL 통계 계산 및 저장
        """
        try:
            db = await mongodb.get_database()
            source_collection = db[mongo_settings.MONGO_CW_MYSQL_SLOW_SQL_COLLECTION]

            # 기본 집계 파이프라인
            pipeline = [
                {
                    "$match": {
                        "date": {"$regex": f"^{year_month}"}
                    }
                },
                {
                    "$unwind": "$users"
                },
                {
                    "$group": {
                        "_id": {
                            "instance_id": "$instance_id",
                            "month": {"$substr": ["$date", 0, 7]},
                            "user": "$users"
                        },
                        "total_queries": {"$sum": 1},
                        "total_exec_count": {"$sum": "$execution_count"},
                        "total_exec_time": {"$sum": "$total_time"},
                        "queries": {"$push": "$digest_query"}
                    }
                },
                {
                    "$sort": {
                        "_id.instance_id": 1,
                        "total_queries": -1
                    }
                }
            ]

            results = []
            async for group in source_collection.aggregate(pipeline):
                # 쿼리 타입 분류 (힌트/주석 제거 후)
                queries = group["queries"]
                read_queries = sum(1 for q in queries if self._is_read_query(q))
                write_queries = sum(1 for q in queries if self._is_write_query(q))
                ddl_queries = sum(1 for q in queries if self._is_ddl_query(q))
                commit_queries = sum(1 for q in queries if self._is_commit_query(q))

                stat = {
                    "instance_id": group["_id"]["instance_id"],
                    "month": group["_id"]["month"],
                    "user": group["_id"]["user"],
                    "total_queries": group["total_queries"],
                    "total_exec_count": group["total_exec_count"],
                    "total_exec_time": round(group["total_exec_time"], 2),
                    "avg_execution_time": round(group["total_exec_time"] / group["total_exec_count"], 2) if group[
                                                                                                                "total_exec_count"] > 0 else 0,
                    "read_query_count": read_queries,
                    "write_query_count": write_queries,
                    "ddl_query_count": ddl_queries,
                    "commit_query_count": commit_queries,  # 커밋 관련 쿼리 수 추가
                    "created_at": datetime.utcnow()
                }
                results.append(stat)

            # 통계 저장
            if results:
                stats_collection = db[mongo_settings.MONGO_CW_SQL_USER_STATISTICS]
                await stats_collection.delete_many({"month": year_month})
                await stats_collection.insert_many(results)
                logger.info(f"{year_month} 사용자별 통계 계산 및 저장 완료: {len(results)}개 항목")
            else:
                logger.warning(f"{year_month} 사용자별 통계 계산 결과 없음")

            return results

        except Exception as e:
            logger.error(f"SQL 사용자별 통계 계산 중 오류 발생: {e}")
            raise