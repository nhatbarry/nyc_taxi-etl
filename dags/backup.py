from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from sqlalchemy.engine import create_engine
from time import time

import pyarrow.parquet as pq
import pandas as pd
import os


dag_owner = 'nhatbarry'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5)
                }

engine = create_engine('postgresql://root:root@postgres-data/nyc_taxi_data')
DATA_PATH = '/opt/airflow/downloads/yellow_taxi_data'



with DAG(dag_id='backup',
         default_args=default_args,
         description='',
         start_date=datetime(2025, 4, 20),
         schedule_interval=None,
         catchup=False,
         tags=['']
         ):


    @task
    def backup_to_postgres(file_name):
        print(f'Ingesting {file_name}')
        file_path = os.path.join(DATA_PATH, file_name)

        parquet_file = pq.ParquetFile(file_path)

        batch_size = 100_000

        for i, batch in enumerate(parquet_file.iter_batches(batch_size=batch_size)):
            start = time()

            df = batch.to_pandas()

            df.to_sql(name='yellow_taxi_trips', con=engine, if_exists='append', index=False)

            duration = time() - start
            print(f'Inserted batch {i} with {len(df)} rows from {file_name}, took {duration:.3f}s')


    
    list_file = os.listdir(DATA_PATH)
    previous_task = None

    for file in list_file:
        safe_task_id = f"backup_from_{file.replace('.', '_').replace(' ', '_')}"
        task = backup_to_postgres.override(task_id=safe_task_id)(file)
        
        if previous_task:
            previous_task >> task

        previous_task = task



