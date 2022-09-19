"""
Implements a retention policy by dropping expired partitions

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913

Prerequisites
-------------
In CrateDB, tables for storing retention policies need to be created once manually.
See the file setup/data_retention_schema.sql in this repository.
"""
from pathlib import Path
import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task


@task
def generate_sql(policy):
    """Generate DROP statment for a given partition"""
    return (
        Path("include/data_retention_delete.sql")
        .read_text(encoding="utf-8")
        .format(
            table_fqn=policy[0],
            column=policy[1],
            value=policy[2],
        )
    )


@task
def get_policies(ds=None):
    """Retrieve all partitions effected by a policy"""
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    sql = Path("include/data_retention_retrieve_delete_policies.sql")
    return pg_hook.get_records(
        sql=sql.read_text(encoding="utf-8"), parameters={"day": ds}
    )


@dag(
    start_date=pendulum.datetime(2021, 11, 19, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def data_retention_delete():
    sql_statements = generate_sql.expand(policy=get_policies())

    PostgresOperator.partial(
        task_id="delete_partition",
        postgres_conn_id="cratedb_connection",
    ).expand(sql=sql_statements)


data_retention_delete_dag = data_retention_delete()
