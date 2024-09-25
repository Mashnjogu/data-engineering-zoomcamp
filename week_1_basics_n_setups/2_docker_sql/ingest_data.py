
import pandas as pd
from time import time
import argparse
import os
from sqlalchemy import create_engine
import requests


def download_file(url, output):
    """Download a file from a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status() # raise an error for bad requests
        with open(output, 'wb') as f:
            f.write(response.content)
            print(f"Downloaded: {output}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        raise
 

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    csv_name = 'output.csv.gz' if url.endswith('.csv.gz') else 'output.csv'

    #Download csv file
    print(f"Downloading data from {url}...")
    download_file(url, csv_name)


    # # Create connection to postgres. Here we specify type of database, then the user,
    #  the password, at local host, the port, and the database name
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    # Convert our date columns to a date-time data type
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    #user
    #password
    #host
    #port
    #database name
    #table name
    #url of the csv

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)








