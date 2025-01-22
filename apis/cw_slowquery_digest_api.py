"""
CloudWatch Slow Query Digest API
CloudWatch에서 수집된 MySQL 슬로우 쿼리 통계 정보를 조회하는 API
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, status
from starlette.responses import JSONResponse

from modules.mongodb_connector import mongodb
from configs.mongo_conf import mongo_settings
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="CloudWatch Slow Query Digest")


async def get_previous_month_range() -> tuple[datetime, datetime]:
    """이전 달의 시작일과 종료일을 반환합니다."""
    today = datetime.now()
    first_day = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    last_day = today.replace(day=1) - timedelta(days=1)

    return first_day, last_day


@app.get("/cw-slowquery/digest/summary", response_model=Dict[str, Any])
async def get_slow_query_summary() -> JSONResponse:
    """
    인스턴스별 슬로우 쿼리 요약 통계를 조회합니다.

    Returns:
        Dict[str, Any]: 슬로우 쿼리 요약 통계
        {
            "month": str,
            "instance_summary": List[{
                "metric": str,
                "orderservice": float | int,
                "read_instance": float | int,
                "total": float | int
            }]
        }
    """
    try:
        await mongodb.initialize()
        db = await mongodb.get_database()
        collection = db[mongo_settings.MONGO_CW_MYSQL_SLOW_SQL_COLLECTION]

        start_date, end_date = await get_previous_month_range()

        pipeline = [
            {
                "$match": {
                    "date": {
                        "$gte": start_date.strftime("%Y-%m-%d"),
                        "$lte": end_date.strftime("%Y-%m-%d")
                    }
                }
            },
            {
                "$group": {
                    "_id": "$instance_id",
                    "unique_digest_count": {"$addToSet": "$digest_query"},
                    "total_queries": {"$sum": "$execution_count"},
                    "total_time": {"$sum": {"$multiply": ["$avg_time", "$execution_count"]}},
                    "total_examined_rows": {"$sum": {"$multiply": ["$avg_rows_examined", "$execution_count"]}},
                    "total_write_queries": {
                        "$sum": {
                            "$cond": [
                                {"$regexMatch": {
                                    "input": {"$arrayElemAt": ["$example_queries", 0]},
                                    "regex": "INSERT|UPDATE|DELETE",
                                    "options": "i"
                                }},
                                "$execution_count",
                                0
                            ]
                        }
                    },
                    "total_read_queries": {
                        "$sum": {
                            "$cond": [
                                {"$regexMatch": {
                                    "input": {"$arrayElemAt": ["$example_queries", 0]},
                                    "regex": "SELECT",
                                    "options": "i"
                                }},
                                "$execution_count",
                                0
                            ]
                        }
                    },
                    "total_ddl_queries": {
                        "$sum": {
                            "$cond": [
                                {"$regexMatch": {
                                    "input": {"$arrayElemAt": ["$example_queries", 0]},
                                    "regex": "CREATE|ALTER|DROP",
                                    "options": "i"
                                }},
                                "$execution_count",
                                0
                            ]
                        }
                    }
                }
            }
        ]

        results = await collection.aggregate(pipeline).to_list(length=None)

        if not results:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"message": "해당 기간의 요약 통계 정보를 찾을 수 없습니다."}
            )

        # 결과 초기화
        summary = {
            "orderservice": {
                "unique_digest_count": set(),  # set으로 변경하여 중복 제거
                "total_queries": 0,
                "total_time": 0,
                "total_examined_rows": 0,
                "total_read_queries": 0,
                "total_write_queries": 0,
                "total_ddl_queries": 0
            },
            "read_instance": {
                "unique_digest_count": set(),  # set으로 변경하여 중복 제거
                "total_queries": 0,
                "total_time": 0,
                "total_examined_rows": 0,
                "total_read_queries": 0,
                "total_write_queries": 0,
                "total_ddl_queries": 0
            }
        }

        # 데이터 집계
        for result in results:
            instance_type = "orderservice" if "orderservice" in result["_id"].lower() and "read-instance" not in result[
                "_id"].lower() else "read_instance"

            # unique_digest_count는 set으로 관리하여 중복 제거
            summary[instance_type]["unique_digest_count"].update(result["unique_digest_count"])

            # 나머지 숫자 데이터는 합산
            summary[instance_type]["total_queries"] += result["total_queries"]
            summary[instance_type]["total_time"] += result["total_time"]
            summary[instance_type]["total_examined_rows"] += int(result["total_examined_rows"])
            summary[instance_type]["total_read_queries"] += result["total_read_queries"]
            summary[instance_type]["total_write_queries"] += result["total_write_queries"]
            summary[instance_type]["total_ddl_queries"] += result["total_ddl_queries"]

        # 인스턴스 요약 데이터 생성
        instance_summary = [
            {
                "metric": "고유 다이제스트 수",
                "orderservice": len(summary["orderservice"]["unique_digest_count"]),
                "read_instance": len(summary["read_instance"]["unique_digest_count"]),
                "total": len(summary["orderservice"]["unique_digest_count"]) + len(
                    summary["read_instance"]["unique_digest_count"])
            },
            {
                "metric": "전체 슬로우 쿼리 수",
                "orderservice": summary["orderservice"]["total_queries"],
                "read_instance": summary["read_instance"]["total_queries"],
                "total": summary["orderservice"]["total_queries"] + summary["read_instance"]["total_queries"]
            },
            {
                "metric": "전체 실행 횟수",
                "orderservice": summary["orderservice"]["total_queries"],
                "read_instance": summary["read_instance"]["total_queries"],
                "total": summary["orderservice"]["total_queries"] + summary["read_instance"]["total_queries"]
            },
            {
                "metric": "전체 실행 시간(초)",
                "orderservice": summary["orderservice"]["total_time"],
                "read_instance": summary["read_instance"]["total_time"],
                "total": round(summary["orderservice"]["total_time"] + summary["read_instance"]["total_time"], 2)
            },
            {
                "metric": "평균 실행 시간(초)",
                "orderservice": round(
                    summary["orderservice"]["total_time"] / summary["orderservice"]["total_queries"] if
                    summary["orderservice"]["total_queries"] > 0 else 0, 3),
                "read_instance": round(
                    summary["read_instance"]["total_time"] / summary["read_instance"]["total_queries"] if
                    summary["read_instance"]["total_queries"] > 0 else 0, 3),
                "total": round((summary["orderservice"]["total_time"] + summary["read_instance"]["total_time"]) /
                               (summary["orderservice"]["total_queries"] + summary["read_instance"]["total_queries"]
                                if (summary["orderservice"]["total_queries"] + summary["read_instance"][
                                   "total_queries"]) > 0 else 1), 3)
            },
            {
                "metric": "전체 조회 행 수",
                "orderservice": summary["orderservice"]["total_examined_rows"],
                "read_instance": summary["read_instance"]["total_examined_rows"],
                "total": summary["orderservice"]["total_examined_rows"] + summary["read_instance"][
                    "total_examined_rows"]
            },
            {
                "metric": "읽기 쿼리",
                "orderservice": summary["orderservice"]["total_read_queries"],
                "read_instance": summary["read_instance"]["total_read_queries"],
                "total": summary["orderservice"]["total_read_queries"] + summary["read_instance"]["total_read_queries"]
            },
            {
                "metric": "쓰기 쿼리",
                "orderservice": summary["orderservice"]["total_write_queries"],
                "read_instance": summary["read_instance"]["total_write_queries"],
                "total": summary["orderservice"]["total_write_queries"] + summary["read_instance"][
                    "total_write_queries"]
            },
            {
                "metric": "DDL 쿼리",
                "orderservice": summary["orderservice"]["total_ddl_queries"],
                "read_instance": summary["read_instance"]["total_ddl_queries"],
                "total": summary["orderservice"]["total_ddl_queries"] + summary["read_instance"]["total_ddl_queries"]
            }
        ]

        response_data = {
            "month": start_date.strftime("%Y-%m"),
            "instance_summary": instance_summary
        }

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response_data
        )

    except Exception as e:
        logger.error(f"슬로우 쿼리 요약 통계 조회 중 오류 발생: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"슬로우 쿼리 요약 통계 조회 실패: {str(e)}"
        )
    finally:
        await mongodb.close()


@app.get("/cw-slowquery/digest/stats", response_model=Dict[str, Any])
async def get_slow_query_stats() -> JSONResponse:
    """
    인스턴스별, 쿼리 다이제스트별 슬로우 쿼리 통계를 조회합니다.

    Returns:
        Dict[str, Any]: 슬로우 쿼리 통계 정보
        {
            "month": str,
            "stats": List[{
                "instance_id": str,
                "digest_query": str,
                "avg_stats": {
                    "avg_lock_time": float,
                    "avg_rows_examined": float,
                    "avg_rows_sent": float,
                    "avg_time": float
                },
                "sum_stats": {
                    "execution_count": int,
                    "total_time": float
                }
            }]
        }
    """
    try:
        await mongodb.initialize()
        db = await mongodb.get_database()
        collection = db[mongo_settings.MONGO_CW_MYSQL_SLOW_SQL_COLLECTION]

        start_date, end_date = await get_previous_month_range()

        # MongoDB Aggregation Pipeline
        pipeline = [
            {
                "$match": {
                    "date": {
                        "$gte": start_date.strftime("%Y-%m-%d"),
                        "$lte": end_date.strftime("%Y-%m-%d")
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "instance_id": "$instance_id",
                        "digest_query": "$digest_query"
                    },
                    # 평균값을 위한 필드들
                    "avg_lock_time": {"$avg": "$avg_lock_time"},
                    "avg_rows_examined": {"$avg": "$avg_rows_examined"},
                    "avg_rows_sent": {"$avg": "$avg_rows_sent"},
                    "avg_time": {"$avg": "$avg_time"},
                    # 합산값을 위한 필드들
                    "total_execution_count": {"$sum": "$execution_count"},
                    "total_time": {"$sum": "$total_time"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "instance_id": "$_id.instance_id",
                    "digest_query": "$_id.digest_query",
                    "avg_stats": {
                        "avg_lock_time": "$avg_lock_time",
                        "avg_rows_examined": "$avg_rows_examined",
                        "avg_rows_sent": "$avg_rows_sent",
                        "avg_time": "$avg_time"
                    },
                    "sum_stats": {
                        "execution_count": "$total_execution_count",
                        "total_time": "$total_time"
                    }
                }
            },
            {
                # 정렬 추가: execution_count 기준 내림차순
                "$sort": {
                    "sum_stats.execution_count": -1
                }
            }
        ]

        results = await collection.aggregate(pipeline).to_list(length=None)

        if not results:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"message": "해당 기간의 슬로우 쿼리 통계 정보를 찾을 수 없습니다."}
            )

        response_data = {
            "month": start_date.strftime("%Y-%m"),
            "stats": results
        }

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response_data
        )

    except Exception as e:
        logger.error(f"슬로우 쿼리 통계 조회 중 오류 발생: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"슬로우 쿼리 통계 조회 실패: {str(e)}"
        )
    finally:
        await mongodb.close()