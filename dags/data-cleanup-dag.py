import datetime
import json
import logging
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def get_policies(sql):
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    records = pg_hook.get_records(sql=sql)
    retention_policies = json.dumps(records)
    return retention_policies


def delete_partitions(ti):
    retention_policies = ti.xcom_pull(task_ids="retrieve_retention_policies")
    policies_obj = json.loads(retention_policies)
    for p in policies_obj:
        key = list(p[1].keys())[0]
        value = p[1][key]
        PostgresOperator(
            task_id="delete_from_{table}".format(table=str(p[1])),
            postgres_conn_id="cratedb_connection",
            sql="DELETE FROM %(table)s WHERE %(column)s=%(value)s",
            parameters={
                "table": str(p[0]),
                "column": str(key),
                "value": value,
            },
        ).execute(dict())


with DAG(
    dag_id="data-cleanup-dag",
    start_date=datetime.datetime(2021, 11, 17),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    t1 = (
        PythonOperator(
            task_id="retrieve_retention_policies",
            python_callable=get_policies,
            op_kwargs={
                "sql": """ SELECT QUOTE_IDENT(p.table_schema) || '.' || QUOTE_IDENT(p.table_name) as fqn, p.values 
                           FROM information_schema.table_partitions p JOIN doc.retention_policies r ON p.table_schema = r.table_schema 
                           AND p.table_name = r.table_name AND p.values[r.partition_column] < CURDATE() - r.retention_period;"""
            },
        ),
    )
    t2 = PythonOperator(
        task_id="apply_data_retention_policies",
        python_callable=delete_partitions,
        provide_context=True,
        op_kwargs={},
    )

t1 >> t2
