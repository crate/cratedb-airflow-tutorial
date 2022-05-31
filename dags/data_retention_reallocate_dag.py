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

# Retrieve all partitions effected by a policy
@task
def get_policies(ds=None):
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    sql = Path('include/data_retention_retrieve_reallocate_policies.sql')
    return pg_hook.get_records(sql=sql.read_text(encoding="utf-8"), parameters={"day": ds})

# Generate SQL for reallocation
@task
def generate_sql_reallocate(policy):
    return Path('include/data_retention_reallocate.sql').read_text(encoding="utf-8").format(
        table=policy[2],
        column=policy[3],
        value=policy[4],
        attribute_name=policy[5],
        attribute_value=policy[6],
    )

# Generate SQL for tracking entry
@task
def generate_sql_tracking(policy):
    return Path('include/data_retention_reallocate_tracking.sql').read_text(encoding="utf-8") \
            .format(schema=policy[0],
                    table=policy[1],
                    partition_value=policy[4],
                   )

@dag(
    start_date=pendulum.datetime(2021, 11, 19, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
)
def data_retention_reallocate():
    policies = get_policies()
    sql_statements_reallocate = generate_sql_reallocate.expand(policy=policies)
    sql_statements_tracking = generate_sql_tracking.expand(policy=policies)

    reallocate = PostgresOperator.partial(
        task_id="reallocate_partitions",
        postgres_conn_id="cratedb_connection",
    ).expand(sql=sql_statements_reallocate)

    track = PostgresOperator.partial(
        task_id="add_tracking_information",
        postgres_conn_id="cratedb_connection",
    ).expand(sql=sql_statements_tracking)

    reallocate >> track

data_retention_reallocate_dag = data_retention_reallocate()
