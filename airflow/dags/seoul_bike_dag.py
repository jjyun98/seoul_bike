from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.append('/opt/airflow/plugins')
from seoul_bike_utils import fetch_seoul_bike_stations, preprocess_station_data, upload_to_s3

SEOUL_API_KEY = os.getenv('SEOUL_API_KEY')
S3_BUCKET = os.getenv('S3_BUCKET', 'yn-project-seoul-bike')

default_args = {
    'owner': 'yunho',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def collect_and_upload():
    """정거장 정보 수집 및 업로드"""
    
    df_raw = fetch_seoul_bike_stations(SEOUL_API_KEY)
    df_clean = preprocess_station_data(df_raw)
    
    upload_to_s3(df_clean, S3_BUCKET, 'station/station.csv')

with DAG(
    'seoul_bike_station_manual',
    default_args=default_args,
    description='서울 따릉이 정거장 데이터 수집 (수동 실행)',
    schedule=None,  # 스케줄 없음 = 수동 실행만
    catchup=False,
) as dag:
    
    collect_task = PythonOperator(
        task_id='collect_and_upload_stations',
        python_callable=collect_and_upload,
    )