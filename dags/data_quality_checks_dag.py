"""
Imports local files to S3, then to CrateDB and checks several data quality properties

Prerequisites
-------------
In CrateDB, set up tables for temporarily and permanently storing incoming data.
See the file setup/smart_home_data.sql in this repository.
"""
import os
import logging
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.utils.dates import datetime
from airflow.decorators import task

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator

from include.data_checks import COL_CHECKS, TABLE_CHECKS


S3_BUCKET = os.environ.get("S3_BUCKET")
ACCESS_KEY_ID = os.environ.get("SECRET_ACCESS_KEY")
SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")
FILE_DIR = os.environ.get("FILE_DIR")
TEMP_TABLE = os.environ.get("TEMP_TABLE")
TABLE = os.environ.get("TABLE")


def slack_failure_notification(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    exec_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    slack_msg = f"""
            :red_circle: Task Failed. 
            *Task*: {task_id}  
            *Dag*: {dag_id} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_webhook',
        message=slack_msg)
    return failed_alert.execute(context=context)


@task
def get_files_from_s3(bucket, prefix_value):
    s3_hook = S3Hook()
    paths = s3_hook.list_keys(bucket_name=bucket, prefix=prefix_value)
    return paths


@task
def get_import_statements(bucket, prefix_value):
    s3_hook = S3Hook()
    file_paths = s3_hook.list_keys(bucket_name=bucket, prefix=prefix_value)
    statements = []
    for path in file_paths:
        sql = f"""
                COPY {TEMP_TABLE} FROM 's3://{ACCESS_KEY_ID}:{SECRET_ACCESS_KEY}@{S3_BUCKET}/{path}' WITH (format='csv');
              """
        logging.info(sql)
        statements.append(sql)
    return statements


with DAG(
    "data_quality_checks",
    description="DAG for checking quality of home metering data.",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    on_failure_callback=slack_failure_notification
) as dag:
    with TaskGroup(group_id="upload_local_files") as upload:
        for file in os.listdir(FILE_DIR):
            LocalFilesystemToS3Operator(
                task_id=f"upload_{file}",
                filename=f"{FILE_DIR}/{file}",
                dest_key=f"incoming-data/{file}",
                dest_bucket=S3_BUCKET,
                aws_conn_id="aws_default",
                replace=True,
            )

    import_stmt = get_import_statements(S3_BUCKET, "incoming")
    upload >> import_stmt

    import_data = PostgresOperator.partial(
        task_id="import_data_to_cratedb",
        postgres_conn_id="cratedb_connection"
    ).expand(sql=import_stmt)

    refresh = PostgresOperator(
        task_id="refresh_table",
        postgres_conn_id="cratedb_connection",
        sql="""
                REFRESH TABLE {{params.temp_table}};   
            """,
        params={
                "table": TEMP_TABLE,
        }
    )
    import_stmt >> import_data >> refresh

    with TaskGroup(group_id="home_data_checks") as checks:
        column_checks = SQLColumnCheckOperator.partial(
            task_id="home_data_column_check",
            conn_id="cratedb_connection",
            table=TEMP_TABLE,
        ).expand(column_mapping=COL_CHECKS)

        # Table Check Operator requires Airflow 2.3.4 release
        table_checks = SQLTableCheckOperator.partial(
            task_id="home_data_table_check",
            conn_id="cratedb_connection",
            table=TEMP_TABLE,
        ).expand(checks=TABLE_CHECKS)

    move_data = PostgresOperator(
        task_id="move_to_table",
        postgres_conn_id="cratedb_connection",
        sql="""
                INSERT INTO {{params.table}} SELECT * FROM {{params.temp_table}};   
            """,
        params={
                "table": TABLE,
                "temp_table": TEMP_TABLE
        }
    )

    delete_data = PostgresOperator(
        task_id="delete_from_temp_table",
        postgres_conn_id="cratedb_connection",
        sql="""
                DELETE FROM {{params.temp_table}};   
            """,
        params={
                "temp_table": TEMP_TABLE
        },
        trigger_rule='all_done'
    )

    refresh >>  checks >> move_data >> delete_data

    with TaskGroup(group_id="move_incoming_files") as processed:
        for file in os.listdir(FILE_DIR):
            S3CopyObjectOperator(
                task_id=f"move_{file}",
                aws_conn_id='aws_default',
                source_bucket_name=S3_BUCKET,
                source_bucket_key=f"incoming-data/{file}",
                dest_bucket_name=S3_BUCKET,
                dest_bucket_key=f"processed-data/{file}",
            )

    s3_files = get_files_from_s3(S3_BUCKET, "incoming")

    delete_files = S3DeleteObjectsOperator.partial(
        task_id='delete_incoming_files',
        aws_conn_id='aws_default',
        bucket=S3_BUCKET
    ).expand(keys=s3_files)

delete_data >> processed >> s3_files
