from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from sqlalchemy.engine import create_engine
from time import time

import pandas as pd
import os


dag_owner = 'nhatbarry'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5)
                }

engine = create_engine('postgresql://root:root@postgres-data/nyc_taxi_data')
DATA_PATH = './downloads'
LIST_CSV = os.listdir(DATA_PATH)

with DAG(dag_id='ingest_data',
         default_args=default_args,
         description='',
         start_date=datetime(2025, 4, 20),
         schedule_interval='@daily',
         catchup=False,
         tags=['']
         ):

    start = EmptyOperator(task_id='start')

    @task
    def create_schema():
        df = pd.read_csv(f'{DATA_PATH}/yellow_tripdata_2019-01.csv', nrows=1)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.head(n=0).to_sql(name='yellow_nyc_taxitrip',
                            con=engine, if_exists='replace')
        return ''

    @task
    def ingest_to_postgres():
        for file in LIST_CSV:
            print(f'ingesting {file}:')
            while True:
                try:
                        start_time = time()
                        
                        df_iter = pd.read_csv(os.path.join(DATA_PATH, file), iterator=True, chunksize=100000)
                        df = next(df_iter)

                        df.tpep_pickup_datetime = pd.to_datetime(
                        df.tpep_pickup_datetime)
                        df.tpep_dropoff_datetime = pd.to_datetime(
                        df.tpep_dropoff_datetime)

                        df.to_sql(name='yellow_nyc_taxitrip',
                                con=engine, if_exists='append')
                        
                        end_time = time()
                        
                        print(f'inserted 100000 rows, took %.3f sec' % (end_time - start_time))
                except:
                        break

    end = EmptyOperator(task_id='end')

    start >> create_schema() >> ingest_to_postgres() >> end
