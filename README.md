# AWS 기반 실시간 데이터 파이프라인 구축 프로젝트

본 프로젝트는 AWS EC2 인스턴스(t3.large)를 활용하여 Kafka를 이용한 실시간 데이터 파이프라인을 구축하고, PostgreSQL과 S3에 데이터를 저장 및 분석하여 Grafana를 통해 시각화하는 시스템입니다.

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

1. **AWS CLI**  
   AWS CLI를 설치하고 AWS 계정에 로그인해야 합니다. 아래의 명령어로 AWS CLI를 설치할 수 있습니다:

   ```bash
   # AWS CLI 설치 (macOS/Linux)
   curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
   sudo installer -pkg AWSCLIV2.pkg -target /

   # AWS CLI 설치 (Windows)
   msiexec.exe /i https://awscli.amazonaws.com/AWSCLIV2.msi
