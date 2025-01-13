# MySQL Slow Query Collector

MySQL/Aurora MySQL 데이터베이스의 슬로우 쿼리를 실시간으로 모니터링하고 수집하는 도구입니다. 실시간 모니터링과 CloudWatch 로그 기반의 일간 수집을 모두 지원합니다.
데이터 독은 실행되는 모든 쿼리의 플랜을 프로시저 기반으로 저장하기 때문에 MySQL 모니터링에 추가적인 리소스 사용과 중복 플랜을 다수 저장하는 구조이기 때문에 실시간 수집기를 구현했습니다.

## 주요 기능

### 1. 실시간 슬로우 쿼리 모니터링
- MySQL performance_schema를 통한 실시간 슬로우 쿼리 감지
- 설정 가능한 실행 시간 임계값 기준 모니터링
- MongoDB를 통한 슬로우 쿼리 정보 저장
- 실시간 쿼리 실행 계획(EXPLAIN) 수집

### 2. CloudWatch 슬로우 쿼리 수집
- AWS CloudWatch Logs에서 슬로우 쿼리 로그 수집
- 일간 단위 수집 및 분석
- 쿼리 패턴 분석 및 통계 정보 제공

## 시스템 요구사항

- Python 3.7 이상
- MongoDB
- AWS 계정 및 적절한 IAM 권한
- MySQL/Aurora MySQL 인스턴스

## 프로젝트 구조

```
my_slow_queries/
├── apis/                   # FastAPI 엔드포인트
├── collectors/            # 데이터 수집기
├── configs/              # 설정 관리
├── modules/              # 공통 모듈
└── docker/              # 도커 관련 파일
```

## 설정

1. 환경 변수 설정 (.env 파일)
```env
# 애플리케이션 환경 설정
APP_ENV=dev                    # 필수: dev 또는 prd (base_config.py에서 사용)
APP_SECRET_NAME=slow-query-collector-secret  # 선택: prd 환경에서만 사용

# AWS 설정
AWS_DEFAULT_REGION=ap-northeast-2  # 필수: AWS 리전
AWS_SSO_START_URL=              # 필수: AWS SSO URL (로컬 개발 환경)
AWS_SSO_REGION=ap-northeast-2   # 필수: AWS SSO 리전
AWS_ROLE_NAME=AdministratorAccess  # 필수: AWS IAM 역할
AWS_ACCOUNT_ID=                 # 선택: AWS 계정 ID

# MongoDB 설정
MONGODB_URI=mongodb://localhost:27017  # 필수: MongoDB 연결 URI
MONGODB_DB_NAME=mgmt_mysql      # 필수: MongoDB 데이터베이스 이름
MONGODB_USER=admin              # 선택: MongoDB 사용자
MONGODB_PASSWORD=admin          # 선택: MongoDB 비밀번호
MONGO_TLS=false                # 선택: MongoDB TLS 사용 여부

# MySQL 설정
MYSQL_EXEC_TIME=2              # 필수: 슬로우 쿼리 기준 시간(초)
MYSQL_MONITORING_INTERVAL=1     # 필수: 모니터링 간격(초)
MYSQL_EXCLUDED_DBS=information_schema,mysql,performance_schema  # 선택: 모니터링 제외 DB
MYSQL_EXCLUDED_USERS=monitor,rdsadmin,system user,mysql_mgmt   # 선택: 모니터링 제외 사용자
MGMT_USER=mgmt_mysql           # 필수: MySQL 관리 사용자
MGMT_USER_PASS=                # 필수: MySQL 관리 사용자 비밀번호

# 컬렉션 이름 설정 (선택적, 기본값 있음)
MONGO_RDS_MYSQL_INSTANCE_COLLECTION=rds_mysql_instance
MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION=rds_mysql_realtime_slow_query
MONGO_RDS_MYSQL_SLOW_SQL_PLAN_COLLECTION=rds_mysql_slow_query_explain
MONGO_CW_MYSQL_SLOW_SQL_COLLECTION=rds_mysql_cw_slow_query
```

## API 엔드포인트

### 실시간 모니터링 API

- `POST /mysql/start`: 실시간 모니터링 시작
- `POST /mysql/stop`: 실시간 모니터링 중지
- `GET /mysql/status`: 모니터링 상태 확인
- `POST /mysql/explain/{pid}`: 특정 쿼리의 실행 계획 수집
- `GET /mysql/explain/{pid}`: 실행 계획 수집 상태 확인

### CloudWatch 수집 API

- `POST /cloudwatch/run`: CloudWatch 로그 수집 시작
- `GET /cloudwatch/status/{target_date}`: 특정 날짜의 수집 상태 확인

## 실행 방법

1. 실시간 모니터링 서버 실행:
```bash
python app.py
```

2. CloudWatch 슬로우 쿼리 수집:
```bash
python -m collectors.cloudwatch_slowquery_collector --date YYYY-MM-DD
```

## 데이터 저장

수집된 데이터는 MongoDB의 다음 컬렉션에 저장됩니다:

- `rds_mysql_instance`: RDS 인스턴스 정보
- `rds_mysql_realtime_slow_query`: 실시간 수집된 슬로우 쿼리
- `rds_mysql_slow_query_explain`: 쿼리 실행 계획
- `rds_mysql_cw_slow_query`: CloudWatch 로그에서 수집된 슬로우 쿼리

## 주의사항

1. 보안
   - 프로덕션 환경에서는 반드시 적절한 보안 설정 필요
   - AWS SSO 또는 IAM 역할 사용 권장
   - MongoDB 인증 필수

2. 성능
   - 실행 시간 임계값 적절히 설정
   - 모니터링 간격 조정으로 서버 부하 관리

## 라이선스

Copyright (c) 2025

## 기여

버그 리포트, 기능 제안, 풀 리퀘스트를 환영합니다.