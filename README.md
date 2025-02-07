# MySQL Slow Query Collector

AWS RDS MySQL/Aurora MySQL 데이터베이스의 슬로우 쿼리를 실시간으로 모니터링하고 수집하는 도구입니다. 실시간 모니터링과 CloudWatch 로그 기반의 수집을 모두 지원하며, WebSocket을 통한 실시간 모니터링 상태 확인이 가능합니다.

## 특징

- 실시간 슬로우 쿼리 모니터링
- CloudWatch 로그 기반 슬로우 쿼리 수집
- 쿼리 실행 계획(EXPLAIN) 자동 수집
- WebSocket을 통한 실시간 상태 모니터링
- Docker 컨테이너 지원
- AWS SSO 및 IAM Role 기반 인증 지원

## 시스템 요구사항

- Python 3.7 이상
- MongoDB 4.0 이상
- AWS 계정 및 적절한 IAM 권한
- RDS MySQL/Aurora MySQL 인스턴스

## 설치 방법

### Docker를 사용한 설치

1. 프로젝트 클론
```bash
git clone [repository-url]
cd my_slow_queries
```

2. 환경 변수 설정
```bash
cp .env.example .env
# .env 파일을 편집하여 필요한 설정 입력
```

3. Docker 컨테이너 실행
```bash
# 프로젝트 루트 디렉토리에서
docker build -f docker/Dockerfile -t my-slow-queries .
docker-compose -f docker/docker-compose.yml up -d
```

### 로컬 환경 설치

1. 의존성 설치
```bash
pip install -r requirements.txt
```

2. 환경 변수 설정 후 실행
```bash
python app.py
```

## API 엔드포인트

### 실시간 모니터링 API
- `POST /mysql/start` - 실시간 모니터링 시작
- `POST /mysql/stop` - 실시간 모니터링 중지
- `GET /mysql/status` - 모니터링 상태 확인
- `GET /mysql/queries` - 수집된 슬로우 쿼리 조회
- `POST /mysql/explain/{pid}` - 특정 쿼리의 실행 계획 수집
- `GET /mysql/explain/{pid}/markdown` - 실행 계획 마크다운 형식으로 다운로드

### CloudWatch 수집 API
- `POST /cloudwatch/run` - CloudWatch 로그 수집 시작
- `GET /cloudwatch/queries` - CloudWatch 수집 쿼리 조회
- `GET /cw-slowquery/digest/stats` - CloudWatch 수집 통계 조회

### RDS 인스턴스 관리 API
- `POST /collectors/rds-instances` - RDS 인스턴스 정보 수집
- `GET /rds-instances` - 수집된 RDS 인스턴스 목록 조회

### WebSocket 엔드포인트
- `ws://[host]/ws/collection/{collection_id}` - 실시간 수집 상태 모니터링

## 설정 옵션

### 필수 환경 변수
```env
# 애플리케이션 설정
APP_ENV=dev                    # 환경 설정 (dev/prd)
APP_SECRET_NAME=               # AWS Secrets Manager 시크릿 이름 (선택)

# AWS 설정
AWS_DEFAULT_REGION=            # AWS 리전
AWS_SSO_START_URL=            # AWS SSO URL (개발 환경)
AWS_ROLE_NAME=                # AWS IAM 역할

# MongoDB 설정
MONGODB_URI=                  # MongoDB 연결 URI
MONGODB_DB_NAME=              # 데이터베이스 이름
MONGO_TLS=false              # TLS 사용 여부

# MySQL 설정
MYSQL_EXEC_TIME=2            # 슬로우 쿼리 기준 시간(초)
MGMT_USER=                   # MySQL 관리자 계정
MGMT_USER_PASS=              # MySQL 관리자 비밀번호
```

### 선택적 환경 변수
```env
# MySQL 모니터링 설정
MYSQL_MONITORING_INTERVAL=1   # 모니터링 간격(초)
MYSQL_EXCLUDED_DBS=          # 모니터링 제외 데이터베이스
MYSQL_EXCLUDED_USERS=        # 모니터링 제외 사용자

# MongoDB 컬렉션 설정
MONGO_RDS_INSTANCE_COLLECTION=       # RDS 인스턴스 정보 컬렉션
MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION= # 실시간 슬로우 쿼리 컬렉션
```

## 데이터 구조

### MongoDB 컬렉션

1. RDS 인스턴스 정보 (`rds_mysql_instance`)
```javascript
{
    "instance_name": String,      // RDS 인스턴스 식별자
    "host": String,              // 엔드포인트
    "port": Number,              // 포트
    "region": String,            // AWS 리전
    "tags": Object               // 인스턴스 태그
}
```

2. 실시간 슬로우 쿼리 (`rds_mysql_realtime_slow_query`)
```javascript
{
    "pid": Number,               // 프로세스 ID
    "instance": String,          // 인스턴스 이름
    "db": String,               // 데이터베이스
    "user": String,             // 사용자
    "time": Number,             // 실행 시간
    "sql_text": String,         // SQL 쿼리
    "start": Date,              // 시작 시간
    "end": Date                 // 종료 시간
}
```

3. 실행 계획 (`rds_mysql_slow_query_explain`)
```javascript
{
    "pid": Number,              // 프로세스 ID
    "instance": String,         // 인스턴스 이름
    "explain_result": {         // 실행 계획 결과
        "json": Object,         // JSON 형식 실행 계획
        "tree": String         // TREE 형식 실행 계획
    }
}
```

IAM 설정

```bash
# 1️⃣ IAM 역할 생성
IAM_ROLE_NAME=rds_slow_monitor

cat > ec2-role-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": { "Service": "ec2.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

aws iam create-role --role-name ${IAM_ROLE_NAME} --assume-role-policy-document file://ec2-role-trust-policy.json

# 2️⃣ IAM 정책 생성
IAM_POLICY_NAME=rds_slow_monitor

# 정책 JSON 다운로드
curl --fail --silent --write-out "Response code: %{response_code}\n" https://raw.githubusercontent.com/qonto/prometheus-rds-exporter/main/configs/aws/policy.json -o rds-slow-monitor.policy.json

# IAM 정책 생성
aws iam create-policy --policy-name ${IAM_POLICY_NAME} --policy-document file://rds-slow-monitor.policy.json

# 정책 ARN 생성 및 역할에 정책 연결
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
IAM_POLICY_ARN=arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${IAM_POLICY_NAME}

aws iam attach-role-policy --role-name ${IAM_ROLE_NAME} --policy-arn ${IAM_POLICY_ARN}

# 3️⃣ IAM 인스턴스 프로필 생성
EC2_INSTANCE_PROFILE_NAME="rds_slow_monitor"

# IAM 인스턴스 프로필 생성
aws iam create-instance-profile --instance-profile-name ${EC2_INSTANCE_PROFILE_NAME}

# IAM 역할을 인스턴스 프로필에 추가
aws iam add-role-to-instance-profile --instance-profile-name ${EC2_INSTANCE_PROFILE_NAME} --role-name ${IAM_ROLE_NAME}

# 4️⃣ IAM 인스턴스 프로필을 EC2에 연결
EC2_INSTANCE_ID="i-1234567890abcdef0" # 실제 AWS EC2 인스턴스 ID로 변경

aws ec2 associate-iam-instance-profile \
--instance-id ${EC2_INSTANCE_ID} \
--iam-instance-profile Name="${EC2_INSTANCE_PROFILE_NAME}"
```

## 모니터링 및 알림

- WebSocket을 통한 실시간 수집 상태 모니터링
- 수집 진행률 및 오류 실시간 확인
- 수집 완료 시 자동 알림

## 보안 고려사항

1. AWS 인증
   - 개발 환경: AWS SSO 사용
   - 운영 환경: IAM Role 사용

2. 데이터베이스 접근
   - 읽기 전용 관리자 계정 사용
   - TLS 연결 지원
   - 최소 권한 원칙 적용

3. MongoDB 보안
   - TLS 연결 지원
   - 인증 필수 적용
   - 접근 제어 설정

## 트러블슈팅

### 일반적인 문제

1. 연결 오류
```
문제: MongoDB 연결 실패
해결: MongoDB URI 및 인증 정보 확인
```

2. 권한 오류
```
문제: AWS API 호출 실패
해결: IAM 역할 및 정책 확인
```

### 로그 확인
```bash
# 애플리케이션 로그
docker logs my-slow-queries

# MongoDB 로그
docker logs mongodb
```

## 라이선스

이 프로젝트는 MIT 라이선스를 따릅니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

## 기여하기

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

