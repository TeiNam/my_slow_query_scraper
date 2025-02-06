"""
프로젝트 루트 디렉토리 경로 관리
"""

import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def get_project_root() -> Path:
    """
    프로젝트 루트 디렉토리 경로 반환
    현재 파일(base_path.py)이 configs/ 디렉토리에 있으므로 parent를 한 번만 호출
    """
    current_file = Path(__file__).resolve()  # 현재 파일의 절대 경로
    logger.debug(f"Current file path: {current_file}")

    root = current_file.parent.parent
    logger.debug(f"Project root directory: {root}")

    # .env 파일 존재 여부로 루트 디렉토리 확인
    if not (root / '.env').exists() and not (root / '.env.example').exists():
        logger.warning(f"No .env or .env.example file found in {root}")

    return root

# 전역 상수로 루트 디렉토리 경로 설정
ROOT_DIR = get_project_root()