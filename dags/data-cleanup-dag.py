import datetime
import os
import json
import logging
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable


# define a function that connects to cratedb and fetches policies
def get_records(sql):
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    records = pg_hook.get_records(sql=sql)
    json_output = json.dumps(records)
    return json_output


def analyze_output(ti):
    json_output = ti.xcom_pull(task_ids="push_results")
    logging.info(json_output)


with DAG(
    dag_id="data-cleanup-dag",
    start_date=datetime.datetime(2021, 11, 17),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    t1 = (
        PythonOperator(
            task_id="push_results",
            python_callable=get_records,
            op_kwargs={
                "sql": " SELECT p.table_schema, p.table_name, p.values FROM information_schema.table_partitions p JOIN doc.retention_policies r ON p.table_schema = r.table_schema AND p.table_name = r.table_name AND p.values[r.partition_column] < CURDATE() - r.retention_period;"
            },
            dag=dag,
        ),
    )
    # t2 = PythonOperator(
    #     task_id="pull_results",
    #     python_callable=analyze_output,
    #     provide_context=True,
    #     op_kwargs={},
    #     dag=dag,
    # )

# t1 >> t2
