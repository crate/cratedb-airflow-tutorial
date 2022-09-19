"""
Regularly exports a table's rows to an S3 bucket as JSON files

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-automating-data-export-to-s3/901
"""
import os
import pendulum
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from include.table_exports import TABLES


@dag(
    start_date=pendulum.datetime(2021, 11, 11, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def table_export():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    with TaskGroup(group_id="table_exports") as tg1:
        for export_table in TABLES:
            PostgresOperator(
                task_id=f"copy_{export_table['table']}",
                postgres_conn_id="cratedb_connection",
                sql="""
                        COPY {{params.table}} WHERE DATE_TRUNC('day', {{params.timestamp_column}}) = '{{macros.ds_add(ds, -1)}}'
                        TO DIRECTORY 's3://{{params.access}}:{{params.secret}}@{{params.target_bucket}}-{{macros.ds_add(ds, -1)}}';
                    """,
                params={
                    "table": export_table["table"],
                    "timestamp_column": export_table["timestamp_column"],
                    "target_bucket": export_table["target_bucket"],
                    "access": os.environ.get("ACCESS_KEY_ID"),
                    "secret": os.environ.get("SECRET_ACCESS_KEY"),
                },
            )
    chain(start, tg1, end)


table_export()
