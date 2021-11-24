import datetime
import json
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
    for policy in policies_obj:
        table_name = policy[0]
        column_name = policy[1]
        partition_value = policy[2]
        PostgresOperator(
            task_id="delete_from_{table}".format(table=str(table_name)),
            postgres_conn_id="cratedb_connection",
            sql="DELETE FROM %(table)s WHERE %(column)s=%(value)s",
            parameters={
                "table": str(table_name),
                "column": str(column_name),
                "value": partition_value,
            },
        ).execute(dict())


with DAG(
    dag_id="data-cleanup-dag",
    start_date=datetime.datetime(2021, 11, 19),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    get_policies = PythonOperator(
        task_id="retrieve_retention_policies",
        python_callable=get_policies,
        op_kwargs={
            "sql": """ SELECT QUOTE_IDENT(p.table_schema) || '.' || QUOTE_IDENT(p.table_name) as fqn, 
                        r.partition_column, p.values[r.partition_column] 
                       FROM information_schema.table_partitions p JOIN doc.retention_policies r ON p.table_schema = r.table_schema 
                       AND p.table_name = r.table_name 
                       AND p.values[r.partition_column] < {date}::TIMESTAMP - r.retention_period;"""
            .format(date="{{ ds }}")
        },
    )
    apply_policies = PythonOperator(
        task_id="apply_data_retention_policies",
        python_callable=delete_partitions,
        provide_context=True,
        op_kwargs={},
    )

    get_policies >> apply_policies
