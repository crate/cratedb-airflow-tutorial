"""
Implements a retention policy by reallocating cold partitions

Prerequisites
-------------
In CrateDB, tables for storing retention policies need to be created once manually.
See the file setup/data_retention_schema.sql in this repository.
"""
import json
import logging
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def get_policies(logical_date):
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    sql = Path('include/data_retention_retrieve_reallocate_policies.sql') \
              .read_text(encoding="utf-8").format(date=logical_date)
    records = pg_hook.get_records(sql=sql)

    return json.dumps(records)


def reallocate_partition(partition):
    logging.info("Reallocating partition %s = %s for table %s to %s = %s",
                 partition["column_name"],
                 partition["partition_value"],
                 partition["table_fqn"],
                 partition["reallocation_attribute_name"],
                 partition["reallocation_attribute_value"],
    )

    # Reallocate the partition
    PostgresOperator(
        task_id=f"reallocate_{partition['table_fqn']}" \
                f"_{partition['column_name']}_{partition['partition_value']}",
        postgres_conn_id="cratedb_connection",
        sql=Path('include/data_retention_reallocate.sql').read_text(encoding="utf-8").format(
            table=partition["table_fqn"],
            column=partition["column_name"],
            value=partition["partition_value"],
            attribute_name=partition["reallocation_attribute_name"],
            attribute_value=partition["reallocation_attribute_value"],
        ),
    ).execute({})

    # Update tracking information. As we process partitions in ascending order
    # by the partition value, it is safe to always save the current value.
    PostgresOperator(
        task_id=f"reallocate_track_{partition['table_fqn']}" \
                f"_{partition['column_name']}_{partition['partition_value']}",
        postgres_conn_id="cratedb_connection",
        sql=Path('include/data_retention_reallocate_tracking.sql') \
                .read_text(encoding="utf-8") \
                .format(table=partition["table_name"],
                        schema=partition["schema_name"],
                        partition_value=partition["partition_value"],
                        ),
    ).execute({})


def map_policy(policy):
    return {
        "schema_name": policy[0],
        "table_name": policy[1],
        "table_fqn": policy[2],
        "column_name": policy[3],
        "partition_value": policy[4],
        "reallocation_attribute_name": policy[5],
        "reallocation_attribute_value": policy[6],
    }


def reallocate_partitions(ti):
    retention_policies = ti.xcom_pull(task_ids="retrieve_retention_policies")
    policies_obj = json.loads(retention_policies)

    for policy in policies_obj:
        reallocate_partition(map_policy(policy))


with DAG(
    dag_id="data-retention-reallocate-dag",
    start_date=pendulum.datetime(2021, 11, 19, tz="UTC"),
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
        python_callable=reallocate_partitions,
        op_kwargs={},
    )

    get_policies >> apply_policies
