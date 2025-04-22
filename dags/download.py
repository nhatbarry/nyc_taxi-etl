from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

import os

dag_owner = 'nhatbarry'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }
files = [
    'yellow_tripdata_2019-01.csv.gz'
    , 'yellow_tripdata_2019-02.csv.gz'
    , 'yellow_tripdata_2019-03.csv.gz'
    , 'yellow_tripdata_2019-04.csv.gz'
    , 'yellow_tripdata_2019-05.csv.gz'
    , 'yellow_tripdata_2019-06.csv.gz'
    , 'yellow_tripdata_2019-07.csv.gz'
    , 'yellow_tripdata_2019-08.csv.gz'
    , 'yellow_tripdata_2019-09.csv.gz'
    , 'yellow_tripdata_2019-10.csv.gz'
    , 'yellow_tripdata_2019-11.csv.gz'
    , 'yellow_tripdata_2019-12.csv.gz'
]

DOWNLOAD_PATH = '/opt/airflow/downloads'
BASE_URL = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'

with DAG(dag_id='download',
        default_args=default_args,
        description='download and decompress csv files',
        start_date=datetime(2025, 4, 20),
        schedule_interval=None,
        catchup=False,
        tags=['']
):

    @task
    def download_file(filename: str):
        url = f"{BASE_URL}/{filename}"
        local_path = os.path.join(DOWNLOAD_PATH, filename)
        os.system(f"mkdir -p {DOWNLOAD_PATH}")
        os.system(f"curl -L -f -o {local_path} {url}")
        return local_path

    @task
    def decompress_files():
        os.system(f"gunzip -f {DOWNLOAD_PATH}/*.csv.gz")
        
    download_tasks = []
    for file in files:
        task = download_file.override(task_id=f"download_{file.replace('.', '_')}")(file)
        download_tasks.append(task)

    decompress_task = decompress_files()

    download_tasks >> decompress_task