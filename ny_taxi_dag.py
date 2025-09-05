from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="manual_ingest_taxi_data",
    schedule=None,
    default_args=default_args,
    catchup=False,
) as dag:

    ingest_data = BashOperator(
        task_id="ingest_data_to_postgres",
        bash_command="""
            python /workspaces/PhanDE/airflow-manual/Scripts/ingest_data.py \
                --user=root \
                --password=root \
                --host=localhost \
                --port=5432 \
                --db=ny_taxi \
                --table_name=yellow_taxi_trips \
                --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
        """
    )
