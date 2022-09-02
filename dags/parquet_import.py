import numpy as np
from airflow.decorators import task, dag
from datetime import datetime
import logging
from pathlib import Path
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.models import Variable

from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(dag_id="nyc-taxi1",schedule_interval='@monthly',start_date=datetime(2022, 1, 1), catchup=False)
def taskflow():
    task_list_s3_files = S3ListOperator(
        task_id="task_s3_list_files",
        bucket='nyc-tlc',
        prefix='trip data/yellow_tripdata',
        delimiter='/',
        aws_conn_id='s3_conn'
    )  
    @task
    def get_processed_files():
        pg_hook = PostgresHook(postgres_conn_id="cratedb_demo_connection")
        records = pg_hook.get_records(sql='SELECT file_name FROM nyc_taxi.load_files_processed')
        # flatten nested list as there is only one column
        return list(map(lambda record: record[0], records))

    @task(task_id='task_list_unprocessed_files')
    def list_unprocessed_files(data_urls_processed,ti=None):
        data_urls_available = ti.xcom_pull(task_ids=['task_s3_list_files'])[0]
        filename_retriever = lambda s: s.split('/')[-1]
        transform_filename = np.vectorize(filename_retriever)
        data_urls_available_transformed = transform_filename(data_urls_available)
        return list(set(data_urls_available_transformed) - set(data_urls_processed))

    task_list_unprocessed_files = list_unprocessed_files(get_processed_files())
    (task_list_unprocessed_files << task_list_s3_files)

    @task(task_id='task_download_files_s3')
    def download_from_s3_csv(ti=None):
        files = ti.xcom_pull(task_ids=['task_list_unprocessed_files'])[0]
        filename_retriever = lambda s: s.split('/')[-1]
        transform_filename = np.vectorize(filename_retriever)
        files_names = transform_filename(files)
        local_path = Variable.get("local_path")
        s3_path = Variable.get("s3_path")
        for file_name in files_names:
            file_name_csv = file_name.replace('parquet','csv')
            task_name = file_name.replace('parquet','')
            print(f'parquet-tools csv "{s3_path}{file_name}" > "{local_path}{file_name_csv}"')
            BashOperator(
            task_id=f'download_from_s3_csv_{task_name}',
            bash_command=f'parquet-tools csv "{s3_path}{file_name}" > "{local_path}{file_name_csv}"',
            ).execute({})

    @task(task_id='task_process_new_files')
    def process_new_files(ti=None):
        missing_files = ti.xcom_pull(task_ids="task_list_unprocessed_files")
        s3_path = Variable.get("s3_path")
        local_path = Variable.get("local_path")
        for missing_file in missing_files:
            logging.info(missing_file)

            file_name = missing_file.replace('parquet','csv')

            PostgresOperator(
                task_id=f"copy_{file_name}",
                postgres_conn_id="cratedb_demo_connection",
                sql=f"""
                        COPY nyc_taxi.load_trips_staging_parquet
                        FROM '{local_path}{file_name}'
                        WITH (format = 'csv', empty_string_as_null = true)
                        RETURN SUMMARY;
                    """
            ).execute({})

            PostgresOperator(
                task_id=f"log_{file_name}",
                postgres_conn_id="cratedb_demo_connection",
                sql=Path('include/taxi-insert.sql').read_text(encoding="utf-8"),
            ).execute({})

            PostgresOperator(
                task_id=f"mark_processed_{file_name}",
                postgres_conn_id="cratedb_demo_connection",
                sql=f"INSERT INTO nyc_taxi.load_files_processed_parquet VALUES ('{s3_path}{missing_file}');",
            ).execute({})

            PostgresOperator(
                task_id=f"purge_staging_{file_name}",
                postgres_conn_id="cratedb_demo_connection",
                sql="DELETE FROM nyc_taxi.load_trips_staging_parquet;"
            ).execute({})
    task_list_unprocessed_files >> download_from_s3_csv() >> process_new_files()
dag = taskflow()
