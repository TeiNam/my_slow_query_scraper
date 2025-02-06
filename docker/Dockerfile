# Python 3.13.1-slim 이미지 사용
FROM python:3.13.1-slim

# 필수 패키지 설치 및 정리 (타임존 설정 포함)
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 타임존 설정
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 작업 디렉토리 설정
WORKDIR /app

# requirements.txt 복사 및 패키지 설치
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 애플리케이션 소스 코드 복사
COPY .. .

# 환경 변수 설정
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV LOG_LEVEL=INFO
ENV MYSQL_EXEC_TIME=2
ENV MYSQL_CONNECT_TIMEOUT=10
ENV MYSQL_POOL_SIZE=1
ENV APP_ENV=prd

# 포트 노출
EXPOSE 8000

# 실행 명령 설정
ENTRYPOINT ["python", "app.py"]