"""
MySQL Connector
Provides read-only access to MySQL for slow query analysis
"""

import asyncmy
import asyncmy.cursors
from asyncmy import create_pool
from typing import Dict, Any, List, Optional, Tuple
import logging
from configs.mysql_conf import MySQLSettings

logger = logging.getLogger(__name__)


class MySQLConnector:
    """MySQL 읽기 전용 커넥터"""

    def __init__(self, instance_name: str):
        """
        Args:
            instance_name: RDS 인스턴스의 DBInstanceIdentifier
        """
        self.instance_name = instance_name
        self.pool: Optional[Any] = None
        self.settings: Optional[MySQLSettings] = None

    async def create_pool(self, settings: MySQLSettings) -> None:
        """MySQL 연결 풀 생성"""
        try:
            self.pool = await create_pool(
                host=settings.host,
                port=settings.port,
                user=settings.user,
                password=settings.password,
                db=settings.database,
                maxsize=settings.pool_size,
                connect_timeout=settings.connect_timeout
            )
            self.settings = settings
            logger.info(f"Connected to MySQL: {settings.host}:{settings.port}/{settings.database}")

        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {str(e)}")
            raise

    async def fetch_all(self, query: str) -> List[Dict[str, Any]]:
        """SELECT 쿼리 실행 및 결과 반환"""
        if not self.pool:
            raise ValueError(f"No connection pool available for {self.instance_name}")

        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(asyncmy.cursors.DictCursor) as cursor:
                    await cursor.execute(query)
                    return await cursor.fetchall()

        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise

    async def explain_query(self, query: str) -> List[Dict[str, Any]]:
        """쿼리 실행 계획 조회"""
        explain_query = f"EXPLAIN FORMAT=JSON {query}"
        return await self.fetch_all(explain_query)

    async def close(self) -> None:
        """연결 풀 종료"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("MySQL connection pool closed")

    async def execute_query(self, query: str, params: Tuple = None) -> List[Dict[str, Any]]:
        """Execute a query on the MySQL instance."""
        if not self.pool:
            raise ValueError(f"No connection pool found for {self.instance_name}")

        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(asyncmy.cursors.DictCursor) as cursor:
                    if params:
                        await cursor.execute(query, params)
                    else:
                        await cursor.execute(query)
                    return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Error executing query for {self.instance_name} - {self.instance_name}: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise

    @staticmethod
    async def execute_query_with_new_connection(connection_params: Dict[str, Any], query: str) -> List[Dict[str, Any]]:
        """
        Create a new connection and execute a query

        Args:
            connection_params (Dict[str, Any]): MySQL connection parameters
                Required keys:
                - host: database host
                - port: database port
                - user: database user
                - password: database password
                - db: database name
                Optional keys:
                - charset: database charset (default: utf8mb4)
                - connect_timeout: connection timeout in seconds (default: 10)
            query (str): Query to execute

        Returns:
            List[Dict[str, Any]]: Query results
        """
        try:
            # Create a new connection pool with the provided parameters
            temp_pool = await create_pool(
                host=connection_params['host'],
                port=connection_params['port'],
                user=connection_params['user'],
                password=connection_params['password'],
                db=connection_params['db'],
                charset=connection_params.get('charset', 'utf8mb4'),
                maxsize=1,  # Single connection is sufficient for temporary use
                connect_timeout=connection_params.get('connect_timeout', 10)
            )

            try:
                async with temp_pool.acquire() as conn:
                    async with conn.cursor(asyncmy.cursors.DictCursor) as cursor:
                        await cursor.execute(query)
                        return await cursor.fetchall()
            finally:
                temp_pool.close()
                await temp_pool.wait_closed()

        except Exception as e:
            logger.error(f"Error executing query with new connection - {str(e)}")
            logger.error(f"Query: {query}")
            raise

    async def set_database(self, database: str) -> None:
        """Set the database for the instance."""
        if not self.pool:
            raise ValueError(f"No connection pool found for {self.instance_name}")

        try:
            async with self.pool.acquire() as conn:
                await conn.select_db(database)
            logger.info(f"Set database to {database} for {self.instance_name}")
        except Exception as e:
            logger.error(f"Error setting database for {self.instance_name}: {str(e)}")
            raise

    @staticmethod
    async def test_connection(settings: MySQLSettings) -> bool:
        """연결 테스트"""
        try:
            test_pool = await create_pool(
                host=settings.host,
                port=settings.port,
                user=settings.user,
                password=settings.password,
                db=settings.database,
                maxsize=1,
                connect_timeout=settings.connect_timeout
            )

            async with test_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")

            test_pool.close()
            await test_pool.wait_closed()
            return True

        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False