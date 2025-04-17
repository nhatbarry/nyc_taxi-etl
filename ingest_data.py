import pandas as pd
import os
import argparse

from sqlalchemy import create_engine
from time import time

def main(params):
    
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db 
    table_name = params.table_name

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df = pd.read_csv('./datasets/yellow_tripdata_2016-01.csv', nrows=2)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    datasets_path = './datasets/'
    datasets_list = os.listdir(datasets_path)

    for dataset in datasets_list:
        path = f"{datasets_path}{dataset}"
        print(f'reading {path}:')
        df_iter = pd.read_csv(path, iterator=True, chunksize=10000)
        while True:
            try:
                time_start = time()
                df = next(df_iter)
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
                df.to_sql(name=table_name, con=engine, if_exists='append')
                time_end = time()
                print('\tinserted another 10.000 records...., took %.3f sec' %(time_end - time_start))
            except:
                print(f"{dataset} insgeted sucesfully\n")
                break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ingest data from csv files to postgres")
    parser.add_argument('--user')
    parser.add_argument('--password')
    parser.add_argument('--host')
    parser.add_argument('--port')
    parser.add_argument('--db')
    parser.add_argument('--table_name')

    args = parser.parse_args()
    main(args)