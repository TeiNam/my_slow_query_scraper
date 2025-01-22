import asyncio
import websockets
import json


async def websocket_test():
    uri = "ws://localhost:8000/ws/collection/test_collection"
    async with websockets.connect(uri) as websocket:
        # 연결 성공 메시지 확인
        print("Connected to WebSocket server")

        # 테스트 메시지 전송
        test_message = json.dumps({"type": "ping", "message": "Hello WebSocket"})
        await websocket.send(test_message)
        print(f"Sent: {test_message}")

        # 서버로부터 응답 수신
        response = await websocket.recv()
        print(f"Received: {response}")


# 테스트 실행
if __name__ == "__main__":
    asyncio.run(websocket_test())
