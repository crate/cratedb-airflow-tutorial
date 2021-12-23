"""
Regularly exports a table's rows to an S3 bucket as JSON files

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-automating-data-export-to-s3/901
"""
import datetime
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from include.table_exports import TABLES


with DAG(
    dag_id="cratedb_table_export",
    start_date=datetime.datetime(2021, 11, 11),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    with TaskGroup(group_id='table_exports') as tg1:
        for export_table in TABLES:
            PostgresOperator(
                task_id="copy_{table}".format(table=export_table['table']),
                postgres_conn_id="cratedb_connection",
                sql="""
                        COPY {table} WHERE DATE_TRUNC('day', {timestamp_column}) = '{day}'
                        TO DIRECTORY 's3://{access}:{secret}@{target_bucket}-{day}';
                    """.format(
                    table=export_table['table'],
                    timestamp_column=export_table['timestamp_column'],
                    target_bucket=export_table['target_bucket'],
                    day='{{ macros.ds_add(ds, -1) }}',
                    access=os.environ.get("ACCESS_KEY_ID"),
                    secret=os.environ.get("SECRET_ACCESS_KEY")
                )
            )
