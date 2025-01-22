"""
웹소켓 연결 관리 및 메시지 브로드캐스팅을 위한 모듈
"""
from typing import Dict, Set
from fastapi import WebSocket
import asyncio
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(self):
        self._connections: Dict[str, Set[WebSocket]] = {}
        self._collection_status: Dict[str, Dict] = {}

    async def connect(self, websocket: WebSocket, collection_id: str):
        """새로운 웹소켓 연결 추가"""
        await websocket.accept()

        if collection_id not in self._connections:
            self._connections[collection_id] = set()
        self._connections[collection_id].add(websocket)

        # 현재 상태 전송 - 직렬화 처리 추가
        if collection_id in self._collection_status:
            # 상태 데이터를 전송하기 전에 직렬화
            serialized_status = self._serialize_dict(self._collection_status[collection_id])
            await websocket.send_json(serialized_status)

    async def disconnect(self, websocket: WebSocket, collection_id: str):
        """웹소켓 연결 제거"""
        self._connections[collection_id].remove(websocket)
        if not self._connections[collection_id]:
            del self._connections[collection_id]

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
        """Convert datetime objects to ISO format strings in a dictionary"""
        serialized = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            elif isinstance(value, dict):
                serialized[key] = self._serialize_dict(value)
            else:
                serialized[key] = value
        return serialized

    async def _broadcast(self, collection_id: str, message: Dict):
        """특정 수집 ID의 모든 연결에 메시지 전송"""
        if collection_id in self._connections:
            dead_connections = set()
            for connection in self._connections[collection_id]:
                try:
                    # Ensure message is JSON serializable
                    serialized_message = self._serialize_dict(message)
                    await connection.send_json(serialized_message)
                except Exception as e:
                    logger.error(f"Failed to send message: {e}")
                    dead_connections.add(connection)

            # 끊어진 연결 제거
            for dead in dead_connections:
                await self.disconnect(dead, collection_id)


# 싱글톤 인스턴스
websocket_manager = WebSocketManager()