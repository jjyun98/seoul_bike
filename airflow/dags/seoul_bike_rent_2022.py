from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Airflow 플러그인 경로 추가
sys.path.append('/opt/airflow/plugins')
from rent_history_utils import collect_and_upload_monthly_data

# 환경 변수에서 API 키 및 S3 버킷 이름 로드
SEOUL_API_KEY = os.getenv('SEOUL_API_KEY')
S3_BUCKET = os.getenv('S3_BUCKET', 'yn-project-seoul-bike')
YEAR = 2022

# DAG 기본 설정
default_args = {
    'owner': 'yunho',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,  # 불안정한 API를 고려하여 재시도 1회 추가
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id='seoul_bike_rent_2022',
    default_args=default_args,
    description='2022년 서울시 공공자전거 대여 이력을 월별로 수집, 처리하여 S3에 저장합니다.',
    schedule=None,  # 수동 실행
    catchup=False,
    tags=['seoul-bike', 'history', 's3'],
) as dag:

    # 1월부터 12월까지 각 월에 대한 태스크를 동적으로 생성
    for month in range(1, 13):
        PythonOperator(
            task_id=f'collect_month_{month:02d}',
            python_callable=collect_and_upload_monthly_data,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': YEAR,
                'month': month,
            },
        )
