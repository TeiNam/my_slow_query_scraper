# MySQL SlowQuery Monitoring System

## 개요

### 제작 배경
- MySQL은 Oracle의 AWR Report처럼 뭔가 성능 분석을 위한 자동화된 도구가 별로 없다.
- 데이터독은 MySQL에 프로시저를 설치하며 실행되는 모든 SQL에 대해 트리거 방식으로 프로시저를 돌려 MySQL의 자체 부하를 발생을 발생시키고, 불필요한 SQL의 플랜에 저장하기도 한다.
- 필요한 슬로우 쿼리에 대해서만 성능 고도화 및 최적화 작업을 진행 할 수 있다.
- 월간 슬로우 쿼리에 대한 통계를 내고 싶다.

### 특징

- 실시간 슬로우 쿼리 모니터링
- CloudWatch 로그 기반 슬로우 쿼리 수집
- 쿼리 실행 계획(EXPLAIN) 자동 수집
- WebSocket을 통한 실시간 상태 모니터링
- Docker 컨테이너 지원
- AWS SSO 및 IAM Role 기반 인증 지원

### 시스템 요구사항

- Python 3.13 이상
- MongoDB 4.4 이상
- AWS 계정 및 적절한 IAM 권한
- 코드가 설치될 EC2 혹은 EKS

## 소개

### 쿼리 수집
- 실시간으로 슬로우쿼리를 캡쳐하여 DB에 저장하고, 플랜을 같이 저장하여 해당 쿼리가 어떻게 동작하는지 확인 할 수 있다.
- 쿼리 정규화 및 다이제스트 생성
  - 문자열 및 숫자 리터럴 파라미터화
  - 주석 및 힌트 제거
- 쿼리 유형 분류
  - READ 쿼리 (SELECT, SHOW, DESCRIBE, EXPLAIN)
  - WRITE 쿼리 (INSERT, UPDATE, DELETE, REPLACE, UPSERT)
  - DDL 쿼리 (CREATE, ALTER, DROP, TRUNCATE, RENAME)
  - 트랜잭션 쿼리 (COMMIT, ROLLBACK, BEGIN, START TRANSACTION)

### 쿼리 플랜 시각화
- JSON 형식 실행 계획 수집
- TREE 형식 실행 계획 수집
- 실행 계획 마크다운 변환
- 웹에서 직접 시각화하여 보기 쉽게 변환

### CloudWatch 로그 수집
- 웹을 통해서는 전월 데이터만 수집할 수 있다.
- 매월 써야하는 보고서 쓰는게 귀찮았다.
- 코드를 통해서는 특정 기간 수집하는것도 가능하다.
```bash
# 특정 기간의 CloudWatch 로그 수집
python collectors/cloudwatch_slowquery_collector.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD

# 특정 인스턴스의 로그만 수집
python collectors/cloudwatch_slowquery_collector.py --instance-id <instance_id>
```

### 병렬 처리
- CloudWatch 로그 수집 시 인스턴스별 병렬 처리
- 실행 계획 수집 시 동시 처리 제한
- MongoDB 벌크 작업 최적화


## 설치 (backend)

### 필수 환경 변수
```env
# 애플리케이션 설정
APP_ENV=dev                          # 환경 설정 (dev/prd)
APP_SECRET_NAME=                     # AWS Secrets Manager 시크릿 이름 (선택)

# AWS 설정
AWS_DEFAULT_REGION=                  # AWS 리전
AWS_SSO_START_URL=                   # AWS SSO URL (개발 환경)
AWS_ROLE_NAME=                       # AWS IAM 역할

# MongoDB 설정
MONGODB_URI=                         # MongoDB 연결 URI
MONGODB_DB_NAME=                     # 데이터베이스 이름
MONGO_TLS=false                      # TLS 사용 여부

# MySQL 설정
MYSQL_EXEC_TIME=2                    # 슬로우 쿼리 기준 시간(초)
MGMT_USER=                           # MySQL 관리자 계정
MGMT_USER_PASS=                      # MySQL 관리자 비밀번호
```

### 선택적 환경 변수
```env
# MySQL 모니터링 설정
MYSQL_MONITORING_INTERVAL=1          # 모니터링 간격(초)
MYSQL_EXCLUDED_DBS=                  # 모니터링 제외 데이터베이스
MYSQL_EXCLUDED_USERS=                # 모니터링 제외 사용자

# MongoDB 컬렉션 설정
MONGO_RDS_INSTANCE_COLLECTION=       # RDS 인스턴스 정보 컬렉션
MONGO_RDS_MYSQL_SLOW_SQL_COLLECTION= # 실시간 슬로우 쿼리 컬렉션
```

### MySQL 모니터링 유저 생성
- 모니터링 대상 DB에 생성하는 계정
- 환경 변수 MGMT_USER에 맵핑한다.
```sql
CREATE USER 'mgmt_mysql' identified by '<PASSWORD>';
GRANT SELECT, PROCESS, SHOW VIEW ON *.* TO `mgmt_mysql`@`%`;
GRANT SELECT ON `performance_schema`.* TO `mgmt_mysql`@`%`;
```
### AWS RDS 태그설정
```
env = prd                   # 운영 레벨 수집 설정
real_time_slow_sql = true   # 실시간 슬로우 쿼리 수집 여부
```

### IAM 설정

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

# 필요한 RDS 및 CloudWatch Logs 권한 추가
cat > rds-logs-permissions.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "rds:DescribeDBInstances",
                "rds:DescribeDBClusters",
                "rds:DescribeDBLogFiles",
                "rds:ListTagsForResource",
                "iam:ListAccountAliases",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:GetLogEvents",
                "logs:FilterLogEvents"
            ],
            "Resource": [
                "arn:aws:rds:*:*:db:*",
                "arn:aws:rds:*:*:cluster:*",
                "arn:aws:logs:*:*:log-group:/aws/rds/instance/*",
                "arn:aws:logs:*:*:log-group:/aws/rds/instance/*/log-stream:*"
            ]
        }
    ]
}
EOF

# 기존 정책과 새 권한을 병합
jq -s '.[0].Statement += .[1].Statement | .[0]' rds-slow-monitor.policy.json rds-logs-permissions.json > combined-policy.json

# IAM 정책 생성
aws iam create-policy --policy-name ${IAM_POLICY_NAME} --policy-document file://combined-policy.json

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


## 성능 최적화

### MongoDB 인덱스
```javascript
// 슬로우 쿼리 컬렉션 인덱스
db.rds_mysql_realtime_slow_query.createIndex({ "instance": 1, "start": -1 })
db.rds_mysql_realtime_slow_query.createIndex({ "pid": 1 }, { unique: true })

// CloudWatch 통계 컬렉션 인덱스
db.cw_mysql_slow_sql.createIndex({ "date": 1, "instance_id": 1 })
db.cw_mysql_slow_sql.createIndex({ "digest_query": 1 })
```

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

