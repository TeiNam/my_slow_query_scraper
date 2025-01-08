"""
Instance Loader
Loads RDS instances from MongoDB with different filtering options
"""

from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class InstanceLoader:
    """RDS 인스턴스 정보 로더"""

    def __init__(self, account: Optional[str] = None):
        self.account = account
        self._realtime_instances_cache = None
        self._all_instances_cache = None
        self._cached_account = None

    async def _clear_cache(self) -> None:
        """캐시 초기화"""
        self._realtime_instances_cache = None
        self._all_instances_cache = None
        self._cached_account = None

    @staticmethod
    async def _load_instances_from_mongodb(query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """MongoDB에서 인스턴스 정보 로드"""
        try:
            mongodb = await MongoDBConnector.get_database()
            collection = mongodb[mongo_settings.MONGO_RDS_INSTANCE_COLLECTION]
            instances = await collection.find(query).to_list(length=None)
            return instances
        except Exception as e:
            logger.error(f"Failed to load instances from MongoDB: {e}")
            return []

    @staticmethod
    def _process_instance(instance: Dict[str, Any]) -> Dict[str, Any]:
        """인스턴스 정보 처리"""
        return {
            'instance_name': instance['DBInstanceIdentifier'],
            'host': instance.get('Endpoint', {}).get('Address', ''),
            'port': instance.get('Endpoint', {}).get('Port', 3306),
            'region': instance.get('Region', ''),  # region 정보 추가
            'tags': instance.get('Tags', {})
        }

    async def load_realtime_instances(self) -> List[Dict[str, Any]]:
        """실시간 슬로우 쿼리 모니터링 대상 인스턴스 로드"""
        if (self._realtime_instances_cache is None or
            self._cached_account != self.account):

            # 기본 쿼리 조건
            query = {}
            if self.account:
                query["account"] = self.account

            # real_time_slow_sql 태그가 true인 인스턴스만 필터링
            instances = await self._load_instances_from_mongodb(query)
            filtered_instances = []

            for instance in instances:
                processed = self._process_instance(instance)
                if processed['tags'].get('real_time_slow_sql', '').lower() == 'true':
                    filtered_instances.append(processed)

            self._realtime_instances_cache = filtered_instances
            self._cached_account = self.account
            logger.info(f"Loaded {len(filtered_instances)} real-time monitoring instances")

        return self._realtime_instances_cache

    async def load_all_instances(self) -> List[Dict[str, Any]]:
        """모든 RDS 인스턴스 로드"""
        if (self._all_instances_cache is None or
            self._cached_account != self.account):

            query = {}
            if self.account:
                query["account"] = self.account

            instances = await self._load_instances_from_mongodb(query)
            processed_instances = [
                self._process_instance(instance)
                for instance in instances
            ]

            self._all_instances_cache = processed_instances
            self._cached_account = self.account
            logger.info(f"Loaded {len(processed_instances)} total instances")

        return self._all_instances_cache

    async def reload(self) -> None:
        """캐시 강제 갱신"""
        await self._clear_cache()
        logger.info("Instance cache cleared")