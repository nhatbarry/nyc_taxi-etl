from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta 
from utils import get_url

import os

DOWNLOAD_PATH = '/opt/airflow/downloads'
yellow_taxi_data_url = get_url.get_list_yellow_url()
green_taxi_data_url = get_url.get_list_green_url()
fhv_data_url = get_url.get_list_fhv_url()


dag_owner = 'nhatbarry'
default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }


with DAG(dag_id='download',
        default_args=default_args,
        description='download parquet files',
        start_date=datetime(2025, 4, 20),
        schedule_interval=None,
        catchup=False,
        tags=['']
):
    @task
    def download_yellow_taxi_data(url: str):
        file_name = url.split('/')[-1]
        dir = os.path.join(DOWNLOAD_PATH, 'yellow_taxi_data')
        local_path = os.path.join(dir, file_name)
        os.system(f'mkdir -p {dir}')
        os.system(f'curl -Lfo {local_path} {url}')


    @task
    def download_green_taxi_data(url: str):
        file_name = url.split('/')[-1]
        dir = os.path.join(DOWNLOAD_PATH, 'green_taxi_data')
        local_path = os.path.join(dir, file_name)
        os.system(f'mkdir -p {dir}')
        os.system(f'curl -Lfo {local_path} {url}')


    @task
    def download_fhv_data(url: str):
        file_name = url.split('/')[-1]
        dir = os.path.join(DOWNLOAD_PATH, 'fhv_taxi_data')
        local_path = os.path.join(dir, file_name)
        os.system(f'mkdir -p {dir}')
        os.system(f'curl -Lfo {local_path} {url}')

    
    yellow_taxi_task = []
    for url in yellow_taxi_data_url:
        task = download_yellow_taxi_data.override(task_id=f"download_{url.split('/')[-1]}")(url)
        yellow_taxi_task.append(task)

    green_taxi_task = []
    for url in green_taxi_data_url:
        task = download_green_taxi_data.override(task_id=f"download_{url.split('/')[-1]}")(url)
        green_taxi_task.append(task)

    fhv_task = []
    for url in fhv_data_url:
        task = download_fhv_data.override(task_id=f"download_{url.split('/')[-1]}")(url)
        fhv_task.append(task)
