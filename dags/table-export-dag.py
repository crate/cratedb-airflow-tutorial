import datetime
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="cratedb_table_export",
    start_date=datetime.datetime(2021, 11, 11),
    schedule_interval="@daily",  # start at 00:00
    catchup=False,
) as dag:
    PostgresOperator(
        task_id="copy_to",
        postgres_conn_id="cratedb_connection",
        sql="""
                COPY telegraf.metrics WHERE DATE_TRUNC('day', timestamp) = {time}::TIMESTAMP - '1 day'::INTERVAL TO DIRECTORY 's3://{access}:{secret}@crate-astro-tutorial/data'; 
            """.format(time='{{ ds }}', access=os.environ.get('ACCESS_KEY'), secret=os.environ.get('SECRET_KEY')),
    )
