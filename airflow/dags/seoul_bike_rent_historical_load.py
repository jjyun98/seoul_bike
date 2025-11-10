from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Airflow 플러그인 경로 추가
sys.path.append('/opt/airflow/plugins')
from rent_history_utils import process_entire_year

# 환경 변수에서 API 키 및 S3 버킷 이름 로드
SEOUL_API_KEY = os.getenv('SEOUL_API_KEY')
S3_BUCKET = os.getenv('S3_BUCKET', 'yn-project-seoul-bike')

# DAG 기본 설정
default_args = {
    'owner': 'yunho',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 0, # 연간 태스크 실패 시 재시도는 수동으로 하는 것이 나으므로 0으로 설정
}

# DAG 정의
with DAG(
    dag_id='seoul_bike_rent_historical_load',
    default_args=default_args,
    description='2016년부터 2024년까지의 서울시 공공자전거 대여 이력을 순차적으로 S3에 저장합니다.',
    schedule=None,  # 수동 실행
    catchup=False,
    tags=['seoul-bike', 'history', 's3', 'historical-load'],
) as dag:

    # 2016년부터 2024년까지 각 연도에 대한 태스크를 생성
    years = range(2016, 2025)
    tasks = {}
    for year in years:
        tasks[year] = PythonOperator(
            task_id=f'process_year_{year}',
            python_callable=process_entire_year,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': year,
            },
        )

    # 태스크들을 순차적으로 연결 (2016 -> 2017 -> ...)
    for i in range(len(years) - 1):
        tasks[years[i]] >> tasks[years[i+1]]
