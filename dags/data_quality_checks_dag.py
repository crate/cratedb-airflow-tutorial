"""
Imports local files to S3, then to CrateDB and checks several data quality properties

Prerequisites
-------------
In CrateDB, set up tables for temporarily and permanently storing incoming data.
See the file setup/smart_home_data.sql in this repository.

To run this DAG you need to set environment variables:

AIRFLOW_CONN_CRATEDB_CONNECTION=postgresql://<username>:<pass>@<host>:<port>/doc?sslmode=required
S3_BUCKET=<bucket_name>
FILE_DIR=<path_to_your_data>
AIRFLOW_CONN_AWS_DEFAULT=aws://<your_aws_access_key>:<your_aws_secret_key>@
TEMP_TABLE=<name_temporary_data>
TABLE=<name_permanent_data>
ACCESS_KEY_ID=<your_aws_access_key>
SECRET_ACCESS_KEY=<your_aws_secret_key>
"""

import os
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.utils.dates import datetime
from airflow.decorators import task
from airflow.models.baseoperator import chain

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
)
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3DeleteObjectsOperator,
)

S3_BUCKET = os.environ.get("S3_BUCKET")
ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")
FILE_DIR = os.environ.get("FILE_DIR")
TEMP_TABLE = os.environ.get("TEMP_TABLE")
TABLE = os.environ.get("TABLE")


def slack_failure_notification(context):
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    exec_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :red_circle: Task Failed. 
            *Task*: {task_id}  
            *Dag*: {dag_id} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """
    failed_alert = SlackWebhookOperator(
        task_id="slack_notification", http_conn_id="slack_webhook", message=slack_msg
    )
    return failed_alert.execute(context=context)


@task
def get_files_from_s3(bucket, prefix_value):
    s3_hook = S3Hook()
    paths = s3_hook.list_keys(bucket_name=bucket, prefix=prefix_value)
    return paths


@task
def get_import_statements(files):
    statements = []
    for path in files:
        sql = f"""
                COPY {TEMP_TABLE} FROM 's3://{ACCESS_KEY_ID}:{SECRET_ACCESS_KEY}@{S3_BUCKET}/{path}' WITH (format='csv');
              """
        statements.append(sql)
    return statements


with DAG(
    "data_quality_checks",
    default_args={"on_failure_callback": slack_failure_notification},
    description="DAG for checking quality of home metering data.",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
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
    s3_files = get_files_from_s3(S3_BUCKET, "incoming")
    import_stmt = get_import_statements(s3_files)

    import_data = PostgresOperator.partial(
        task_id="import_data_to_cratedb", postgres_conn_id="cratedb_connection"
    ).expand(sql=import_stmt)

    refresh = PostgresOperator(
        task_id="refresh_table",
        postgres_conn_id="cratedb_connection",
        sql="""
                REFRESH TABLE {{params.temp_table}};   
            """,
        params={
            "temp_table": TEMP_TABLE,
        },
    )

    with TaskGroup(group_id="home_data_checks") as checks:
        column_checks = SQLColumnCheckOperator(
            task_id="home_data_column_check",
            conn_id="cratedb_connection",
            table=TEMP_TABLE,
            column_mapping={
                "time": {
                    "null_check": {"equal_to": 0},
                    "unique_check": {"equal_to": 0},
                },
                "use_kw": {"null_check": {"equal_to": 0}},
                "gen_kw": {"null_check": {"equal_to": 0}},
                "temperature": {"min": {"geq_to": -20}, "max": {"less_than": 99}},
                "humidity": {"min": {"geq_to": 0}, "max": {"less_than": 1}},
            },
        )

        table_checks = SQLTableCheckOperator(
            task_id="home_data_table_check",
            conn_id="cratedb_connection",
            table=TEMP_TABLE,
            checks={
                "row_count_check": {"check_statement": "COUNT(*) > 100000"},
                "total_usage_check": {
                    "check_statement": "dishwasher + home_office + "
                    + "fridge + wine_cellar + kitchen + "
                    + "garage_door + microwave + barn + "
                    + " well + living_room  <= house_overall"
                },
            },
        )

    move_data = PostgresOperator(
        task_id="move_to_table",
        postgres_conn_id="cratedb_connection",
        sql="""
                INSERT INTO {{params.table}} SELECT * FROM {{params.temp_table}};   
            """,
        params={"table": TABLE, "temp_table": TEMP_TABLE},
    )

    delete_data = PostgresOperator(
        task_id="delete_from_temp_table",
        postgres_conn_id="cratedb_connection",
        sql="""
                DELETE FROM {{params.temp_table}};   
            """,
        params={"temp_table": TEMP_TABLE},
        trigger_rule="all_done",
    )

    with TaskGroup(group_id="move_incoming_files") as processed:
        for file in os.listdir(FILE_DIR):
            S3CopyObjectOperator(
                task_id=f"move_{file}",
                aws_conn_id="aws_default",
                source_bucket_name=S3_BUCKET,
                source_bucket_key=f"incoming-data/{file}",
                dest_bucket_name=S3_BUCKET,
                dest_bucket_key=f"processed-data/{file}",
            )

    delete_files = S3DeleteObjectsOperator.partial(
        task_id="delete_incoming_files", aws_conn_id="aws_default", bucket=S3_BUCKET
    ).expand(keys=s3_files)

    chain(
        upload,
        s3_files,
        import_stmt,
        import_data,
        refresh,
        checks,
        move_data,
        delete_data,
        processed,
        delete_files,
    )
