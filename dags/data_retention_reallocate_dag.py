"""
Implements a retention policy by reallocating cold partitions

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934

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
def get_policies(ds=None):
    """Retrieve all partitions effected by a policy"""
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    sql = Path("include/data_retention_retrieve_reallocate_policies.sql")
    return pg_hook.get_records(
        sql=sql.read_text(encoding="utf-8"), parameters={"day": ds}
    )


@task
def map_policy(policy):
    """Map index-based policy to readable dict structure"""
    return {
        "schema": policy[0],
        "table": policy[1],
        "table_fqn": policy[2],
        "column": policy[3],
        "value": policy[4],
        "attribute_name": policy[5],
        "attribute_value": policy[6],
    }


@task
def generate_sql_reallocate(policy):
    """Generate SQL for reallocation"""
    return (
        Path("include/data_retention_reallocate.sql")
        .read_text(encoding="utf-8")
        .format(**policy)
    )


@dag(
    start_date=pendulum.datetime(2021, 11, 19, tz="UTC"),
    schedule="@daily",
    catchup=False,
    template_searchpath=["include"],
)
def data_retention_reallocate():
    policies = map_policy.expand(policy=get_policies())

    reallocate = PostgresOperator.partial(
        task_id="reallocate_partitions",
        postgres_conn_id="cratedb_connection",
    ).expand(sql=generate_sql_reallocate.expand(policy=policies))

    track = PostgresOperator.partial(
        task_id="add_tracking_information",
        postgres_conn_id="cratedb_connection",
        sql="data_retention_reallocate_tracking.sql",
    ).expand(parameters=policies)

    reallocate >> track


data_retention_reallocate_dag = data_retention_reallocate()
