"""
프로젝트 기본 경로 관리
"""

import os
from pathlib import Path

# 프로젝트 루트 디렉토리 경로
ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

def get_project_root() -> Path:
    """프로젝트 루트 디렉토리 경로 반환"""
    return ROOT_DIR