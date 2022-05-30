"""
Implements a retention policy by snapshotting expired partitions to a repository

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-building-a-data-retention-policy-using-external-snapshot-repositories/1001

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

# Retrieve all partitions effected by a policy
@task
def get_policies(ds=None):
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    sql = Path('include/data_retention_retrieve_snapshot_policies.sql') \
            .read_text(encoding="utf-8").format(date=ds)
    return pg_hook.get_records(sql=sql)

# Generate DROP statment for a given partition
@task
def generate_sql_delete(policy):
    return Path('include/data_retention_delete.sql') \
        .read_text(encoding="utf-8").format(table=policy[2],
                                            column=policy[3],
                                            value=policy[4],
                                           )

# Generate DROP statment for a given partition
@task
def generate_sql_snapshot(policy):
    return Path('include/data_retention_snapshot.sql').read_text(encoding="utf-8") \
            .format(repository_name=policy[5],
                    table_schema=policy[0],
                    table_name=policy[1],
                    table_fqn=policy[2],
                    partition_column=policy[3],
                    partition_value=policy[4],
                   )

@dag(
    start_date=pendulum.datetime(2021, 11, 19, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
)
def data_retention_snapshot():
    policies = get_policies()
    sql_statements_snapshot = generate_sql_snapshot.expand(policy=policies)
    sql_statements_delete = generate_sql_delete.expand(policy=policies)

    reallocate = PostgresOperator.partial(
        task_id="snapshot_partitions",
        postgres_conn_id="cratedb_connection",
    ).expand(sql=sql_statements_snapshot)

    delete = PostgresOperator.partial(
        task_id="delete_partitions",
        postgres_conn_id="cratedb_connection",
    ).expand(sql=sql_statements_delete)

    reallocate >> delete

data_retention_snapshot_dag = data_retention_snapshot()
