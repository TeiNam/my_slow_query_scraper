from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseCollector(ABC):
    """슬로우 쿼리 수집을 위한 기본 콜렉터 클래스"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    async def collect(self) -> None:
        """쿼리 수집 메서드"""
        pass

    @abstractmethod
    async def process(self) -> None:
        """수집된 쿼리 처리 메서드"""
        pass
