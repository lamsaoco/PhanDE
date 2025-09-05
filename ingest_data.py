import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    print(f"🔽 Downloading data from {url}...")
    os.system(f"wget {url} -O {csv_name}")
    print(f"✅ Downloaded to {csv_name}")

    try:
        # Use SQLAlchemy Engine with `future=True` for better compatibility
        engine = create_engine(
            f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}',
            future=True
        )
        print(f"🔌 Connected to database `{db}`")

        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000)
        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        with engine.begin() as conn:  # Automatically manages commit/rollback
            df.head(0).to_sql(name=table_name, con=conn, if_exists='replace', index=False)
            print(f"🛠 Table `{table_name}` created")

            df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
            print(f"✅ Inserted first chunk of {len(df)} rows")

            while True:
                try:
                    t_start = time()
                    df = next(df_iter)

                    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                    df.to_sql(name=table_name, con=conn, if_exists='append', index=False)

                    t_end = time()
                    print(f"✅ Inserted chunk of {len(df)} rows in {t_end - t_start:.2f} sec")

                except StopIteration:
                    print("🎉 All chunks ingested successfully!")
                    break

    except Exception as e:
        print("❌ ETL failed due to:", str(e))
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', type=int, help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table where results will be written')
    parser.add_argument('--url', required=True, help='url of the .csv or .csv.gz file')

    args = parser.parse_args()
    main(args)
