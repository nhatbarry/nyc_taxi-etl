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


import psycopg2

def copy_from_csv(file_path, table_name, host, dbname, user, password):
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password
    )
    cur = conn.cursor()
    
    with open(file_path, 'r') as f:
        next(f) 
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", f)
    
    conn.commit()
    cur.close()
    conn.close()



with DAG(dag_id='ingest_data',
         default_args=default_args,
         description='',
         start_date=datetime(2025, 4, 20),
         schedule_interval=None,
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
            print(f'Ingesting {file}...')
            file_path = os.path.join(DATA_PATH, file)
            chunk_iter = pd.read_csv(file_path, iterator=True, chunksize=1000000)
            i = 0
            for chunk in chunk_iter:
                start_time = time()
                chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
                chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)

                chunk.to_sql(name='yellow_nyc_taxitrip', con=engine, if_exists='append')
                duration = time() - start_time
                print(f'\tInserted chunk {i} from {file}, took {duration:.2f} seconds')
                i += 1

                

    end = EmptyOperator(task_id='end')

    start >> create_schema() >> ingest_to_postgres() >> end
