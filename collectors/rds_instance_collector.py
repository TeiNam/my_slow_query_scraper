"""
MySQL/Aurora MySQL RDS Instance Collector
Collects information about MySQL and Aurora MySQL instances from AWS RDS
"""

import logging
from typing import List, Dict, Any
from botocore.exceptions import ClientError
from modules.aws_session_manager import AWSSessionManager
from modules.time_utils import format_kst, to_kst
from modules.mongodb_connector import mongodb
from configs.mongo_conf import mongo_settings
from configs.aws_session_conf import aws_session_config

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

        try:
            rds = self.session_manager.get_client('rds', region=self.region)
            paginator = rds.get_paginator('describe_db_instances')

            async for page in paginator.paginate():
                for db in page['DBInstances']:
                    # MySQL 또는 Aurora MySQL 엔진만 수집
                    if db.get('Engine', '').lower() not in self.TARGET_ENGINES:
                        continue

                    # env=prd 태그가 있는 인스턴스만 수집
                    tags = {tag['Key']: tag['Value'] for tag in db.get('TagList', [])}
                    if tags.get('env') != 'prd':
                        logger.debug(f"Skipping non-production instance: {db.get('DBInstanceIdentifier')}")
                        continue

                    instance_data = {
                        'AccountId': aws_session_config.get_aws_account_id(),
                        'Region': self.region,
                        'DBInstanceIdentifier': db.get('DBInstanceIdentifier'),
                        'Engine': db.get('Engine'),
                        'EngineVersion': db.get('EngineVersion'),
                        'Endpoint': {
                            'Address': db.get('Endpoint', {}).get('Address'),
                            'Port': db.get('Endpoint', {}).get('Port')
                        } if db.get('Endpoint') else None,
                        'InstanceCreateTime': format_kst(
                            to_kst(db.get('InstanceCreateTime'))
                        ) if db.get('InstanceCreateTime') else None,
                        'Tags': {tag['Key']: tag['Value'] for tag in db.get('TagList', [])}
                    }
                    instances.append(instance_data)

            logger.info(f"Found {len(instances)} MySQL instances in region {self.region}")
            return instances

        except ClientError as e:
            logger.error(f"Error fetching RDS instances in region {self.region}: {e}")
            raise

    async def save_to_mongodb(self, instances: List[Dict[str, Any]]) -> None:
        """MongoDB에 인스턴스 정보 저장"""
        try:
            db = await mongodb.get_database()
            collection = db[mongo_settings.MONGO_RDS_INSTANCE_COLLECTION]

            data = {
                'timestamp': format_kst(to_kst()),
                'account_id': aws_session_config.get_aws_account_id(),
                'region': self.region,
                'total_instances': len(instances),
                'instances': instances
            }

            await collection.insert_one(data)
            logger.info(f"Saved {len(instances)} MySQL instances to MongoDB")

        except Exception as e:
            logger.error(f"Error saving to MongoDB: {e}")
            raise

    async def run(self) -> None:
        """MySQL 인스턴스 수집 실행"""
        try:
            await mongodb.initialize()

            logger.info(f"Starting MySQL instance collection for region {self.region}")
            instances = await self.get_mysql_instances()

            if instances:
                await self.save_to_mongodb(instances)
                logger.info("MySQL instance collection completed successfully")
            else:
                logger.warning(f"No MySQL instances found in region {self.region}")

        except Exception as e:
            logger.error(f"Failed to run MySQL instance collection: {e}")
            raise
        finally:
            await mongodb.close()


async def main():
    collector = MySQLInstanceCollector()
    await collector.run()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())