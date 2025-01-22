"""
웹소켓 연결 관리 및 메시지 브로드캐스팅을 위한 모듈
"""
import logging
import asyncio
import json
from asyncio import Task
from datetime import datetime
from json import JSONEncoder
from typing import Dict, Set
from bson import ObjectId
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(self):
        self._connections: Dict[str, Set[WebSocket]] = {}
        self._collection_status: Dict[str, Dict] = {}
        self._cleanup_tasks: Dict[str, Task] = {}
        self._inactive_timeout = 60  # 60초 후 자동 종료

    async def connect(self, websocket: WebSocket, collection_id: str):
        """새로운 웹소켓 연결 추가"""
        await websocket.accept()

        if collection_id not in self._connections:
            self._connections[collection_id] = set()
        self._connections[collection_id].add(websocket)

        # 현재 상태 전송
        if collection_id in self._collection_status:
            serialized_status = self._serialize_dict(self._collection_status[collection_id])
            await websocket.send_json(serialized_status)

    async def disconnect(self, websocket: WebSocket, collection_id: str):
        """웹소켓 연결 제거"""
        try:
            if collection_id in self._connections:
                self._connections[collection_id].discard(websocket)  # remove 대신 discard 사용

                # 연결이 없으면 해당 collection_id 관련 데이터 정리
                if not self._connections[collection_id]:
                    self._connections.pop(collection_id, None)
                    self._collection_status.pop(collection_id, None)

                    # cleanup task가 있다면 정리
                    if collection_id in self._cleanup_tasks:
                        cleanup_task = self._cleanup_tasks[collection_id]
                        cleanup_task.cancel()
                        try:
                            await cleanup_task
                        except asyncio.CancelledError:
                            pass
                        await self._cleanup_tasks.pop(collection_id, None)

        except Exception as e:
            logger.error(f"Disconnect error for collection {collection_id}: {str(e)}")

    async def close_connections(self, collection_id: str):
        """특정 collection_id에 대한 모든 웹소켓 연결을 닫습니다"""
        if collection_id in self._connections:
            connections = self._connections[collection_id].copy()
            for websocket in connections:
                try:
                    await websocket.close()
                except Exception as e:
                    logger.error(f"웹소켓 연결 종료 중 오류 발생: {str(e)}")
            self._connections.pop(collection_id, None)
            # 해당 collection의 상태 정보도 정리
            self._collection_status.pop(collection_id, None)

    async def broadcast_log(self, collection_id: str, message: str, level: str = "info"):
        """로그 메시지 브로드캐스팅"""
        if collection_id in self._connections:
            log_data = {
                "type": "log",
                "level": level,
                "message": message,
                "timestamp": datetime.now().isoformat()
            }
            await self._broadcast(collection_id, log_data)

    async def update_status(self, collection_id: str, status: str, details: Dict = None):
        """수집 상태 업데이트 및 브로드캐스팅"""
        details = details or {}

        # Convert datetime objects to ISO format strings
        status_data = {
            "type": "status",
            "status": status,
            "details": self._serialize_dict(details),
            "timestamp": datetime.now().isoformat()
        }

        self._collection_status[collection_id] = status_data
        if collection_id in self._connections:
            await self._broadcast(collection_id, status_data)

    def _serialize_dict(self, data: Dict) -> Dict:
        """데이터를 JSON 직렬화 가능한 형태로 변환"""
        try:
            # JSONEncoder를 사용하여 직렬화 테스트
            json.dumps(data, cls=JSONEncoder)
            return data
        except TypeError as e:
            logger.error(f"직렬화 중 오류 발생: {e}")
            # 문제가 있는 경우 데이터를 직렬화 가능한 형태로 변환
            serialized = {}
            for key, value in data.items():
                if isinstance(value, ObjectId):
                    serialized[key] = str(value)
                elif isinstance(value, datetime):
                    serialized[key] = value.isoformat()
                elif isinstance(value, dict):
                    serialized[key] = self._serialize_dict(value)
                elif isinstance(value, list):
                    serialized[key] = [
                        str(item) if isinstance(item, ObjectId)
                        else item.isoformat() if isinstance(item, datetime)
                        else self._serialize_dict(item) if isinstance(item, dict)
                        else item
                        for item in value
                    ]
                else:
                    serialized[key] = value
            return serialized

    async def _broadcast(self, collection_id: str, message: Dict):
        """특정 수집 ID의 모든 연결에 메시지 전송"""
        if collection_id in self._connections:
            dead_connections = set()
            serialized_message = self._serialize_dict(message)

            for connection in self._connections[collection_id]:
                try:
                    await connection.send_json(serialized_message)
                except Exception as e:
                    logger.error(f"Failed to send message: {e}")
                    dead_connections.add(connection)

            # 끊어진 연결 제거
            for dead in dead_connections:
                await self.disconnect(dead, collection_id)

    async def schedule_cleanup(self, collection_id: str):
        """비활성 상태인 웹소켓 연결 정리 예약"""
        if collection_id in self._cleanup_tasks:
            self._cleanup_tasks[collection_id].cancel()

        self._cleanup_tasks[collection_id] = asyncio.create_task(
            self._delayed_cleanup(collection_id)
        )

    async def _delayed_cleanup(self, collection_id: str):
        """지정된 시간 후에 웹소켓 연결 정리"""
        try:
            await asyncio.sleep(self._inactive_timeout)
            await self.close_connections(collection_id)
        except asyncio.CancelledError:
            pass

    def reset_cleanup_timer(self, collection_id: str):
        """cleanup 타이머 재설정"""
        asyncio.create_task(self.schedule_cleanup(collection_id))


# 싱글톤 인스턴스
websocket_manager = WebSocketManager()