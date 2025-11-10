# /opt/airflow/dags/seoul_bike_rent_load.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

sys.path.append('/opt/airflow/plugins')
from rent_utils import collect_and_upload_monthly_data

# 환경 변수
SEOUL_API_KEY = os.getenv('SEOUL_API_KEY')
S3_BUCKET = os.getenv('S3_BUCKET', 'yn-project-seoul-bike')

# DAG 기본 설정
default_args = {
    'owner': 'yunho',
    'depends_on_past': False,  # False로 변경
    'start_date': datetime(2016, 1, 1),
    'email_on_failure': False,
    'retries': 1,
}

# 2016년 DAG
with DAG(
    dag_id='seoul_bike_rent_2016',
    default_args=default_args,
    description='2016년 따릉이 대여 이력 수집 (월별)',
    schedule=None,
    catchup=False,
    tags=['seoul-bike', '2016'],
) as dag_2016:
    
    month_tasks = []
    for month in range(1, 13):
        task = PythonOperator(
            task_id=f'month_{month:02d}',
            python_callable=collect_and_upload_monthly_data,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': 2016,
                'month': month,
            },
        )
        month_tasks.append(task)
    
    # 월별 순차 실행
    for i in range(len(month_tasks) - 1):
        month_tasks[i] >> month_tasks[i + 1]


# 2017년 DAG
with DAG(
    dag_id='seoul_bike_rent_2017',
    default_args=default_args,
    description='2017년 따릉이 대여 이력 수집 (월별)',
    schedule=None,
    catchup=False,
    tags=['seoul-bike', '2017'],
) as dag_2017:
    
    month_tasks = []
    for month in range(1, 13):
        task = PythonOperator(
            task_id=f'month_{month:02d}',
            python_callable=collect_and_upload_monthly_data,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': 2017,
                'month': month,
            },
        )
        month_tasks.append(task)
    
    for i in range(len(month_tasks) - 1):
        month_tasks[i] >> month_tasks[i + 1]


# 2018년 DAG
with DAG(
    dag_id='seoul_bike_rent_2018',
    default_args=default_args,
    description='2018년 따릉이 대여 이력 수집 (월별)',
    schedule=None,
    catchup=False,
    tags=['seoul-bike', '2018'],
) as dag_2018:
    
    month_tasks = []
    for month in range(1, 13):
        task = PythonOperator(
            task_id=f'month_{month:02d}',
            python_callable=collect_and_upload_monthly_data,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': 2018,
                'month': month,
            },
        )
        month_tasks.append(task)
    
    for i in range(len(month_tasks) - 1):
        month_tasks[i] >> month_tasks[i + 1]


# 2019년 DAG
with DAG(
    dag_id='seoul_bike_rent_2019',
    default_args=default_args,
    description='2019년 따릉이 대여 이력 수집 (월별)',
    schedule=None,
    catchup=False,
    tags=['seoul-bike', '2019'],
) as dag_2019:
    
    month_tasks = []
    for month in range(1, 13):
        task = PythonOperator(
            task_id=f'month_{month:02d}',
            python_callable=collect_and_upload_monthly_data,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': 2019,
                'month': month,
            },
        )
        month_tasks.append(task)
    
    for i in range(len(month_tasks) - 1):
        month_tasks[i] >> month_tasks[i + 1]


# 2020년 DAG
with DAG(
    dag_id='seoul_bike_rent_2020',
    default_args=default_args,
    description='2020년 따릉이 대여 이력 수집 (월별)',
    schedule=None,
    catchup=False,
    tags=['seoul-bike', '2020'],
) as dag_2020:
    
    month_tasks = []
    for month in range(1, 13):
        task = PythonOperator(
            task_id=f'month_{month:02d}',
            python_callable=collect_and_upload_monthly_data,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': 2020,
                'month': month,
            },
        )
        month_tasks.append(task)
    
    for i in range(len(month_tasks) - 1):
        month_tasks[i] >> month_tasks[i + 1]


# 2021년 DAG
with DAG(
    dag_id='seoul_bike_rent_2021',
    default_args=default_args,
    description='2021년 따릉이 대여 이력 수집 (월별)',
    schedule=None,
    catchup=False,
    tags=['seoul-bike', '2021'],
) as dag_2021:
    
    month_tasks = []
    for month in range(1, 13):
        task = PythonOperator(
            task_id=f'month_{month:02d}',
            python_callable=collect_and_upload_monthly_data,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': 2021,
                'month': month,
            },
        )
        month_tasks.append(task)
    
    for i in range(len(month_tasks) - 1):
        month_tasks[i] >> month_tasks[i + 1]


# 2022년 DAG
with DAG(
    dag_id='seoul_bike_rent_2022',
    default_args=default_args,
    description='2022년 따릉이 대여 이력 수집 (월별)',
    schedule=None,
    catchup=False,
    tags=['seoul-bike', '2022'],
) as dag_2022:
    
    month_tasks = []
    for month in range(1, 13):
        task = PythonOperator(
            task_id=f'month_{month:02d}',
            python_callable=collect_and_upload_monthly_data,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': 2022,
                'month': month,
            },
        )
        month_tasks.append(task)
    
    for i in range(len(month_tasks) - 1):
        month_tasks[i] >> month_tasks[i + 1]


# 2023년 DAG
with DAG(
    dag_id='seoul_bike_rent_2023',
    default_args=default_args,
    description='2023년 따릉이 대여 이력 수집 (월별)',
    schedule=None,
    catchup=False,
    tags=['seoul-bike', '2023'],
) as dag_2023:
    
    month_tasks = []
    for month in range(1, 13):
        task = PythonOperator(
            task_id=f'month_{month:02d}',
            python_callable=collect_and_upload_monthly_data,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': 2023,
                'month': month,
            },
        )
        month_tasks.append(task)
    
    for i in range(len(month_tasks) - 1):
        month_tasks[i] >> month_tasks[i + 1]


# 2024년 DAG
with DAG(
    dag_id='seoul_bike_rent_2024',
    default_args=default_args,
    description='2024년 따릉이 대여 이력 수집 (월별)',
    schedule=None,
    catchup=False,
    tags=['seoul-bike', '2024'],
) as dag_2024:
    
    month_tasks = []
    for month in range(1, 13):
        task = PythonOperator(
            task_id=f'month_{month:02d}',
            python_callable=collect_and_upload_monthly_data,
            op_kwargs={
                'api_key': SEOUL_API_KEY,
                'bucket': S3_BUCKET,
                'year': 2024,
                'month': month,
            },
        )
        month_tasks.append(task)
    
    for i in range(len(month_tasks) - 1):
        month_tasks[i] >> month_tasks[i + 1]