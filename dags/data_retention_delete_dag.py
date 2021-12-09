"""
Implements a retention policy by dropping expired partitions

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913

Prerequisites
-------------
In CrateDB, tables for storing retention policies need to be created once manually.
See the file setup/data_retention_schema.sql in this repository.
"""
import datetime
import json
import logging
from pathlib import Path
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def get_policies(logical_date):
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    sql = Path('include/data_retention_retrieve_delete_policies.sql') \
              .read_text().format(date=logical_date)
    records = pg_hook.get_records(sql=sql)

    return json.dumps(records)


def map_policy(policy):
    return {
        "table_fqn": policy[0],
        "column_name": policy[1],
        "partition_value": policy[2],
    }


def delete_partitions(ti):
    retention_policies = ti.xcom_pull(task_ids="retrieve_retention_policies")
    policies_obj = json.loads(retention_policies)

    for policy in policies_obj:
        partition = map_policy(policy)

        logging.info("Deleting partition %s = %s for table %s",
                     partition["column_name"],
                     partition["partition_value"],
                     partition["table_fqn"],
        )

        PostgresOperator(
            task_id="delete_from_{table}_{partition}_{value}".format(
                table=partition["table_fqn"],
                partition=partition["column_name"],
                value=partition["partition_value"],
            ),
            postgres_conn_id="cratedb_connection",
            sql=Path('include/data_cleanup_delete.sql').read_text().format(
                table=partition["table_fqn"],
                column=partition["column_name"],
                value=partition["partition_value"],
            ),
        ).execute(dict())


with DAG(
    dag_id="data-retention-delete-dag",
    start_date=datetime.datetime(2021, 11, 19),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    get_policies = PythonOperator(
        task_id="retrieve_retention_policies",
        python_callable=get_policies,
        op_kwargs={
            "logical_date": "{{ ds }}",
        },
    )

    apply_policies = PythonOperator(
        task_id="apply_data_retention_policies",
        python_callable=delete_partitions,
        provide_context=True,
        op_kwargs={},
    )

    get_policies >> apply_policies
