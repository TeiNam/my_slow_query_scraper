"""
MySQL/Aurora MySQL RDS Instance Collector
Collects information about MySQL and Aurora MySQL instances from AWS RDS
"""

import logging
from datetime import datetime, UTC
from typing import List, Dict, Any
from botocore.exceptions import ClientError
from modules.aws_session_manager import AWSSessionManager
from modules.time_utils import format_kst, to_kst
from modules.mongodb_connector import mongodb
from configs.mongo_conf import mongo_settings
from configs.aws_session_conf import aws_session_config
from pymongo import UpdateOne

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MySQLInstanceCollector:
    """MySQL/Aurora MySQL RDS 인스턴스 정보 수집기"""

    TARGET_ENGINES = {'mysql', 'aurora-mysql'}
    TARGET_ENV = 'prd'  # 수집 대상 환경

    def __init__(self):
        self.session_manager = AWSSessionManager()
        self.region = aws_session_config.get_aws_region()

    async def get_mysql_instances(self) -> List[Dict[str, Any]]:
        """현재 리전의 MySQL/Aurora MySQL 인스턴스 정보 수집"""
        instances = []
        current_time = datetime.now(UTC)
        collection_start_time = format_kst(to_kst(current_time))

        try:
            logger.info(f"[{collection_start_time}] Starting MySQL instance collection from AWS RDS...")
            rds = self.session_manager.get_client('rds', region=self.region)
            paginator = rds.get_paginator('describe_db_instances')

            for page in paginator.paginate():
                for db in page['DBInstances']:
                    # MySQL 또는 Aurora MySQL 엔진만 수집
                    if db.get('Engine', '').lower() not in self.TARGET_ENGINES:
                        continue

                    # env=prd 태그가 있는 인스턴스만 수집
                    tags = {tag['Key']: tag['Value'] for tag in db.get('TagList', [])}
                    if tags.get('env') != self.TARGET_ENV:
                        logger.debug(f"Skipping non-production instance: {db.get('DBInstanceIdentifier')}")
                        continue

                    create_time = db.get('InstanceCreateTime')
                    instance_data = {
                        'Region': self.region,
                        'DBInstanceIdentifier': db.get('DBInstanceIdentifier'),
                        'Engine': db.get('Engine'),
                        'EngineVersion': db.get('EngineVersion'),
                        'Endpoint': {
                            'Address': db.get('Endpoint', {}).get('Address'),
                            'Port': db.get('Endpoint', {}).get('Port')
                        } if db.get('Endpoint') else None,
                        'InstanceCreateTime': format_kst(to_kst(create_time)) if create_time else None,
                        'Tags': tags,
                        'updateTime': format_kst(to_kst(current_time))
                    }
                    instances.append(instance_data)
                    logger.debug(f"Collected instance: {instance_data['DBInstanceIdentifier']}")

            logger.info(f"[{collection_start_time}] Successfully collected {len(instances)} MySQL instances from region {self.region}")
            return instances

        except ClientError as e:
            logger.error(f"[{collection_start_time}] Error fetching RDS instances in region {self.region}: {e}")
            raise

    async def save_to_mongodb(self, instances: List[Dict[str, Any]]) -> None:
        """각 인스턴스를 DBInstanceIdentifier를 키로 하여 upsert로 MongoDB에 저장"""
        save_start_time = format_kst(to_kst(datetime.now(UTC)))

        try:
            logger.info(f"[{save_start_time}] Starting to save {len(instances)} instances to MongoDB...")
            db = await mongodb.get_database()
            collection = db[mongo_settings.MONGO_RDS_INSTANCE_COLLECTION]

            # 현재 수집된 인스턴스 ID 목록
            current_instance_ids = set(instance['DBInstanceIdentifier'] for instance in instances)

            # 더 이상 존재하지 않는 인스턴스 삭제
            delete_result = await collection.delete_many({
                'DBInstanceIdentifier': {'$nin': list(current_instance_ids)}
            })

            # bulk write 작업 준비
            operations = [
                UpdateOne(
                    {'DBInstanceIdentifier': instance['DBInstanceIdentifier']},
                    {'$set': instance},
                    upsert=True
                )
                for instance in instances
            ]

            if operations:
                result = await collection.bulk_write(operations)
                save_end_time = format_kst(to_kst(datetime.now(UTC)))
                logger.info(
                    f"[{save_end_time}] MongoDB write completed - "
                    f"Upserted: {result.upserted_count}, "
                    f"Modified: {result.modified_count}, "
                    f"Deleted: {delete_result.deleted_count}"
                )
            else:
                logger.warning(f"[{save_start_time}] No instances to save")

        except Exception as e:
            logger.error(f"[{save_start_time}] Error saving to MongoDB: {e}")
            raise

    async def run(self) -> None:
        """MySQL 인스턴스 수집 실행"""
        start_time = format_kst(to_kst(datetime.now(UTC)))
        logger.info(f"[{start_time}] Starting RDS MySQL instance collector...")

        try:
            await mongodb.initialize()
            instances = await self.get_mysql_instances()

            if instances:
                await self.save_to_mongodb(instances)
                end_time = format_kst(to_kst(datetime.now(UTC)))
                logger.info(f"[{end_time}] RDS MySQL instance collection completed successfully")
            else:
                logger.warning(f"[{start_time}] No MySQL instances found in region {self.region}")

        except Exception as e:
            logger.error(f"[{start_time}] Failed to run MySQL instance collection: {e}")
            raise
        finally:
            await mongodb.close()


async def main():
    try:
        collector = MySQLInstanceCollector()
        await collector.run()
    except KeyboardInterrupt:
        logger.info("Collection process interrupted by user")
    except Exception as e:
        logger.exception("Unexpected error occurred")
        raise


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())