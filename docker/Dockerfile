# Python 3.13.1-slim 이미지 사용
FROM python:3.13.1-slim

# 환경변수 설정
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=Asia/Seoul \
    LOG_LEVEL=INFO

# 시스템 패키지 설치 및 정리 (멀티 스테이지 빌드 적용)
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app

# 의존성 설치를 위한 파일만 먼저 복사
COPY ../requirements.txt .

# Python 패키지 설치 및 빌드 의존성 정리
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove gcc python3-dev

# 애플리케이션 기본 설정
ENV MYSQL_EXEC_TIME=2 \
    MYSQL_CONNECT_TIMEOUT=10 \
    MYSQL_POOL_SIZE=1 \
    APP_ENV=prd \
    DOCKER_ENV=true

# 소스 코드 복사 (docker 폴더 상위 경로에서 복사)
COPY .. .

# 포트 노출
EXPOSE 8000

# 헬스체크 설정
HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:8000/health || exit 1

# 실행 명령 설정 (gunicorn 사용)
CMD ["gunicorn", "app:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]