from pydantic_settings import BaseSettings
from modules.mongodb_connector import MongoDBConnector
from configs.mongo_conf import mongo_settings
import logging

logger = logging.getLogger(__name__)


class EnvSettings(BaseSettings):
    ACCOUNT: str = ""

    class Config:
        env_file = ".env"
        extra = "ignore"


env_settings = EnvSettings()

cached_instances = None
cached_account = None


async def load_instances_from_mongodb():
    global cached_instances, cached_account
    current_account = env_settings.ACCOUNT

    if cached_instances is None or cached_account != current_account:
        try:
            mongodb = await MongoDBConnector.get_database()
            collection = mongodb[mongo_settings.MONGO_GET_SLOW_MYSQL_INSTANCE_COLLECTION]

            # 계정 정보로 필터링
            query = {} if not current_account else {"account": current_account}
            instances = await collection.find(query).to_list(length=None)

            cached_instances = []
            for instance in instances:
                processed_instance = {
                    'instance_name': instance['instance_name'],
                    'host': instance['host'],
                    'port': instance['port'],
                    'user': instance['user'],
                    'password': instance['password'],
                    'db': instance.get('db', ''),
                    'account': instance.get('account', '')
                }
                cached_instances.append(processed_instance)

            cached_account = current_account
            logger.info(f"Loaded {len(cached_instances)} MySQL instances from MongoDB for account: {current_account}")
        except Exception as e:
            logger.error(f"Failed to load instances from MongoDB: {e}")
            cached_instances = []

    return cached_instances