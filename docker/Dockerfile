# docker/Dockerfile
FROM python:3.13.1-slim

# 타임존 설정을 위한 패키지 설치 및 설정
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 타임존 설정
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone

# 작업 디렉토리 설정
WORKDIR /app

# 루트 폴더의 requirements.txt 복사 및 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 소스 코드 복사
COPY . .

# 환경 변수 설정
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# 포트 노출
EXPOSE 8000

# 애플리케이션 실행
CMD ["python", "app.py"]