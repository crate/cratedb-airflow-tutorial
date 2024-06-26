"""
Imports local files to S3, then to CrateDB and checks several data quality properties

Prerequisites
-------------
In CrateDB, set up tables for temporarily and permanently storing incoming data.
See the file setup/smart_home_data.sql in this repository.

To run this DAG with sample data, use the following files:
- https://srv.demo.crate.io/datasets/home_data_aa.csv
- https://srv.demo.crate.io/datasets/home_data_ab.csv

Finally, you need to set environment variables:
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
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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
INCOMING_DATA_PREFIX = "incoming-data"
PROCESSED_DATA_PREFIX = "processed-data"


def slack_failure_notification(context):
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    exec_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :red_circle: Task Failed. 
            *Task*: {task_id}
            *DAG*: {dag_id}
            *Execution Time*: {exec_date}
            *Log URL*: {log_url}
            """
    failed_alert = SlackWebhookOperator(
        task_id="slack_notification",
        slack_webhook_conn_id="slack_webhook",
        message=slack_msg,
    )
    return failed_alert.execute(context=context)


# Always execute this task, even if previous uploads have failed. There can be already existing
# files in S3 that haven't been processed yet.
@task(trigger_rule="all_done")
def get_files_from_s3(bucket, prefix_value):
    s3_hook = S3Hook()
    paths = s3_hook.list_keys(bucket_name=bucket, prefix=prefix_value)
    # list_keys also returns directories, we are only interested in files for further processing
    return list(filter(lambda element: element.endswith(".csv"), paths))


@task
def list_local_files(directory):
    return list(filter(lambda entry: entry.endswith(".csv"), os.listdir(directory)))


def copy_file_kwargs(file):
    return {
        "temp_table": TEMP_TABLE,
        "access_key_id": ACCESS_KEY_ID,
        "secret_access_key": SECRET_ACCESS_KEY,
        "s3_bucket": S3_BUCKET,
        "path": file,
    }


def upload_kwargs(file):
    return {
        "filename": f"{FILE_DIR}/{file}",
        "dest_key": f"{INCOMING_DATA_PREFIX}/{file}",
    }


@task_group
def upload_local_files():
    files = list_local_files(FILE_DIR)
    # pylint: disable=E1101
    file_upload_kwargs = files.map(upload_kwargs)

    LocalFilesystemToS3Operator.partial(
        task_id="upload_csv",
        dest_bucket=S3_BUCKET,
        aws_conn_id="aws_default",
        replace=True,
    ).expand_kwargs(file_upload_kwargs)


@task_group
def home_data_checks():
    SQLColumnCheckOperator(
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

    SQLTableCheckOperator(
        task_id="home_data_table_check",
        conn_id="cratedb_connection",
        table=TEMP_TABLE,
        checks={
            "row_count_check": {"check_statement": "COUNT(*) > 100000"},
            "total_usage_check": {
                "check_statement": "dishwasher + home_office + "
                + "fridge + wine_cellar + kitchen + "
                + "garage_door + microwave + barn + "
                + "well + living_room <= house_overall"
            },
        },
    )


def move_incoming_kwargs(file):
    # file includes the whole directory structure
    # Split into parts, replace the first one, and join again
    parts = file.split("/")
    parts[0] = PROCESSED_DATA_PREFIX

    return {
        "source_bucket_key": file,
        "dest_bucket_key": "/".join(parts),
    }


@task_group
def move_incoming_files(s3_files):
    S3CopyObjectOperator.partial(
        task_id="move_incoming",
        aws_conn_id="aws_default",
        source_bucket_name=S3_BUCKET,
        dest_bucket_name=S3_BUCKET,
    ).expand_kwargs(s3_files.map(move_incoming_kwargs))


@dag(
    default_args={"on_failure_callback": slack_failure_notification},
    description="DAG for checking quality of home metering data.",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
)
def data_quality_checks():
    upload = upload_local_files()
    s3_files = get_files_from_s3(S3_BUCKET, INCOMING_DATA_PREFIX)

    # pylint: disable=E1101
    import_data = SQLExecuteQueryOperator.partial(
        task_id="import_data_to_cratedb",
        conn_id="cratedb_connection",
        sql="""
            COPY {{params.temp_table}}
            FROM 's3://{{params.acces_key_id}}:{{params.secret_access_key}}@{{params.s3_bucket}}/{{params.path}}'
            WITH (format = 'csv');
            """,
    ).expand(params=s3_files.map(upload_kwargs))

    refresh = SQLExecuteQueryOperator(
        task_id="refresh_table",
        conn_id="cratedb_connection",
        sql="REFRESH TABLE {{params.temp_table}};",
        params={"temp_table": TEMP_TABLE},
    )

    checks = home_data_checks()

    move_data = SQLExecuteQueryOperator(
        task_id="move_to_table",
        conn_id="cratedb_connection",
        sql="INSERT INTO {{params.table}} SELECT * FROM {{params.temp_table}};",
        params={
            "table": TABLE,
            "temp_table": TEMP_TABLE,
        },
    )

    delete_data = SQLExecuteQueryOperator(
        task_id="delete_from_temp_table",
        conn_id="cratedb_connection",
        sql="DELETE FROM {{params.temp_table}};",
        params={"temp_table": TEMP_TABLE},
        trigger_rule="all_done",
    )

    processed = move_incoming_files(s3_files)

    delete_files = S3DeleteObjectsOperator.partial(
        task_id="delete_incoming_files",
        aws_conn_id="aws_default",
        bucket=S3_BUCKET,
    ).expand(keys=s3_files)

    chain(
        upload,
        s3_files,
        import_data,
        refresh,
        checks,
        move_data,
        delete_data,
        processed,
        delete_files,
    )

    # Require that data must have moved to the target table before marking files as processes.
    # delete_from_temp_table always gets executed, even if previous tasks failed. We only want
    # to mark files as processed if moving data to the target table has not failed.
    move_data >> processed


data_quality_checks()
