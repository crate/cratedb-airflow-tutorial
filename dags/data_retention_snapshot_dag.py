"""
Implements a retention policy by snapshotting expired partitions to a repository

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
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def get_policies(logical_date):
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    sql = Path('include/data_retention_retrieve_snapshot_policies.sql') \
              .read_text(encoding="utf-8").format(date=logical_date)
    records = pg_hook.get_records(sql=sql)

    return json.dumps(records)


def map_policy(policy):
    return {
        "table_schema": policy[0],
        "table_name": policy[1],
        "table_fqn": policy[2],
        "column_name": policy[3],
        "partition_value": policy[4],
        "target_repository_name": policy[5],
    }


def snapshot_partitions(ti):
    retention_policies = ti.xcom_pull(task_ids="retrieve_retention_policies")
    policies_obj = json.loads(retention_policies)

    for policy in policies_obj:
        partition = map_policy(policy)

        logging.info("Snapshoting partition %s = %s for table %s",
                     partition["column_name"],
                     partition["partition_value"],
                     partition["table_fqn"],
        )

        PostgresOperator(
            task_id=f"snapshot_{partition['table_fqn']}" \
                    f"_{partition['column_name']}_{partition['partition_value']}",
            postgres_conn_id="cratedb_connection",
            sql=Path('include/data_retention_snapshot.sql').read_text(encoding="utf-8").format(
                repository_name=partition["target_repository_name"],
                table_schema=partition["table_schema"],
                table_name=partition["table_name"],
                table_fqn=partition["table_fqn"],
                partition_column=partition["column_name"],
                partition_value=partition["partition_value"],
            ),
        ).execute({})

        PostgresOperator(
            task_id=f"delete_{partition['table_fqn']}" \
                    f"_{partition['column_name']}_{partition['partition_value']}",
            postgres_conn_id="cratedb_connection",
            sql=Path('include/data_retention_delete.sql').read_text(encoding="utf-8").format(
                table=partition["table_fqn"],
                column=partition["column_name"],
                value=partition["partition_value"],
            ),
        ).execute({})


with DAG(
    dag_id="data-retention-snapshot-dag",
    start_date=datetime.datetime(2022, 1, 31),
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
        python_callable=snapshot_partitions,
        op_kwargs={},
    )

    get_policies >> apply_policies
