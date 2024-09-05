# AWS 기반 실시간 데이터 파이프라인 구축 프로젝트

본 프로젝트는 AWS EC2 인스턴스(t3.large)를 활용하여 Kafka를 이용한 실시간 데이터 파이프라인을 구축하고, 
PostgreSQL과 S3에 데이터를 저장합니다.
Airflow를 이용하여 배치 단위로 데이터를 분석합니다.
Raw 데이터와 분석된 결과를 Grafana를 통해 시각화하는 시스템입니다.

## 주요 기능
1. **개발/배포 환경 구축**  
   - AWS EC2 인스턴스(t3.large)를 활용한 개발 및 배포 환경 구축.
   
2. **실시간 데이터 파이프라인 구축**  
   - Apache Kafka를 사용하여 실시간 데이터를 처리.
   - PostgreSQL 데이터베이스와 AWS S3에 배치 단위로 데이터를 저장 (parquet 형식).
   
3. **데이터 분석 및 통계 도출**  
   - Apache Airflow를 이용하여 S3에 저장된 배치 데이터를 분석하고, 통계 정보를 도출.
   - 도출된 통계 데이터는 PostgreSQL에 저장.
   
4. **데이터 시각화**  
   - Grafana를 통해 실시간 데이터와 배치 단위 데이터를 시각화.

## 사전 준비 사항

이 프로젝트를 시작하기 전에, 아래의 도구와 설정이 필요합니다.

### 필수 설치 도구

1. **AWS CLI**  <br>
   AWS CLI를 설치하고 AWS 계정에 로그인해야 합니다. 아래의 명령어로 AWS CLI를 설치할 수 있습니다:

   ```bash
   # AWS CLI 설치 (macOS/Linux)
   $ curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
   $ unzip awscliv2.zip
   $ sudo ./aws/install

   # AWS Access key로 로그인
   $ aws configure
   ```

2. **Docker**  
   `docker`와 `docker compose`를 설치해야 합니다.



3. **Python** 
   
   Producer와 Consumer 코드를 실행하기 위해 Python 환경을 구축해야 합니다. 라이브러리 호환성 문제로 **Python 3.10** 버전을 사용하는 것이 권장됩니다. 필자는 venv 가상환경을 사용했습니다.

   ```bash
   $ python -m venv .venv
   $ source .venv/bin/activate
   $ pip install -r requirements.txt
   ```

4. **.env 파일 생성**

   환경변수를 입력합니다.
   BUCKET_NAME은 배치 데이터 parquet 파일을 저장할 S3 버킷명이고, DB 붙은 변수들은 모두 Postgres 관련입니다.

   ```
   AWS_ACCESS_KEY_ID=
   AWS_SECRET_ACCESS_KEY=
   AWS_DEFAULT_REGION=
   AIRFLOW_UID=
   BUCKET_NAME=
   DB_NAME=
   DB_USER=
   DB_PASSWORD=
   DB_HOST=
   DB_PORT=
   ```

## 실행
1. docker 컨테이너 실행

   ```bash
   $ docker compose up -d
   ```

2. Producer, Consumer 코드 실행

   ```bash
   $ nohup python upbit_producer.py > producer.log 2>&1 &
   $ nohup python consumer_postgres.py > consumer_post.log  2>&1 &
   $ nohup python consumer_s3.py > consumer_s3.log  2>&1 &
   ```

## 웹 콘솔 확인
1. kafka Topic, Data 확인 : http://localhost:9000
2. Airflow DAG 확인 : http://localhost:8085
3. Grafana 확인 : http://localhost:3000