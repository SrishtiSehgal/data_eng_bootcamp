#!/usr/bin/env python
# coding: utf-8

import argparse
from time import time

import pandas as pd
from sqlalchemy import create_engine

# NOTE: to convert from jupyter notebook to python script run the following command:
# jupyter nbconvert --to=script <name_of_jupyter_notebook>

def manipulate_df(params, df_iter, engine, show_schema = False):
    t_start = time()
    df = next(df_iter)
    if show_schema:
        # print schema before
        print(pd.io.sql.get_schema(df, name=params.table_name, con=engine))
    # these two columns will be of type TIMESTAMP rather than TEXT
    # convert all to pd.datetime type
    if params.dt_cols:
        for dt_col in params.dt_cols:
            df[dt_col] = pd.to_datetime(df[dt_col])
    if show_schema:
        # print schema after
        print(pd.io.sql.get_schema(df, name=params.table_name, con=engine))
        # creates table
        df.head(n=0).to_sql(name=params.table_name, con=engine, if_exists='replace')
    # performs ingestion
    df.to_sql(name=params.table_name, con=engine, if_exists='append')
    t_end = time()
    print('inserted another chunk, took %.3f second' % (t_end - t_start))
    return df


def data_ingestion(params):
    engine = create_engine(
        f'postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}'
    )
    # specifying iterator=True is only required if you have variable chunksize!
    df_iter = pd.read_csv(params.csv_name, chunksize=100000)
    manipulate_df(params, df_iter, engine, show_schema = True)
    while True: 
        try:
            df = manipulate_df(params, df_iter, engine, show_schema = False)
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break
        except Exception as e:
            print(e)
            print("index_start:", df.index[0], "index_end:", df.index[-1])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--csv_name', required=True, help='relative path to the csv file')
    parser.add_argument('--dt_cols', required=False, help='list of dt cols', nargs="*", default=[])
    args = parser.parse_args()
    print("Args submitted:", args)
    data_ingestion(args)




# Question 3 SQL Query = 20530
# SELECT COUNT(*) FROM green_taxi_tripdata WHERE CAST(lpep_pickup_datetime AS DATE) = date '2019-01-15' AND CAST(lpep_dropoff_datetime AS DATE) = date '2019-01-15';

# Question 4 SQL Query = 2019-01-15 # using pickup time
# SELECT MAX(trip_distance), CAST(lpep_pickup_datetime AS DATE) FROM green_taxi_tripdata GROUP BY CAST(lpep_pickup_datetime AS DATE) ORDER BY MAX(trip_distance) DESC;

# Question 5 SQL Query = 2: 1282 ; 3: 254 # using pickup time
# SELECT COUNT(passenger_count) FROM green_taxi_tripdata WHERE CAST(lpep_pickup_datetime AS DATE) = date '2019-01-01' AND passenger_count=2;
# SELECT COUNT(passenger_count) FROM green_taxi_tripdata WHERE CAST(lpep_pickup_datetime AS DATE) = date '2019-01-01' AND passenger_count=3;

# Question 6 SQL Query Long Island City/Queens Plaza
# SELECT MAX(tip_amount), "Zone" 
# FROM green_taxi_tripdata g 
# LEFT JOIN taxi_lookup z 
# ON g."DOLocationID" = z."LocationID" 
# WHERE "PULocationID"=(
	# SELECT "LocationID" FROM taxi_lookup WHERE "Zone"='Astoria'
# ) 
# GROUP BY z."Zone"
# ORDER BY MAX(tip_amount) DESC;