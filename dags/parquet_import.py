import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def get_latest_month_year():
    currentMonth = datetime.now().month - 1
    currentYear = datetime.now().year
    if currentMonth == 12:
        currentYear = currentYear - 1
    if currentMonth < 10:
        currentMonth = f'0{currentMonth}'
    return f'_{currentYear}-{currentMonth}'


def download_from_s3_csv(ti,prefix_file:str,bucket_name: str) -> str:
    latest_month_year = ti.xcom_pull(task_ids=['get_latest_month_year'])[0]
    file_name_path = f'{local_path}{prefix_file}{latest_month_year}.csv'
    s3_path = f's3://{bucket_name}/{prefix_file}{latest_month_year}.parquet'
    os.system(f'parquet-tools csv "{s3_path}" > "{file_name_path}"')
    return file_name_path

def process_new_file(ti):
    file_path = ti.xcom_pull(task_ids=['download_from_s3_csv'])[0]
    PostgresOperator(
        task_id="copy_csv",
        postgres_conn_id="cratedb_demo_connection",
        sql=f"""
            COPY nyc_taxi.load_trips_staging
            FROM '{file_path}'
            WITH (format = 'csv', empty_string_as_null = true)
            RETURN SUMMARY;
            """
    )


with DAG(
    dag_id='parquet_to_csv_nyc_tlc',
    schedule_interval='@monthly',
    start_date=datetime(2022,3,1),
    catchup=False
) as dag:
    task_get_latest_month_year = PythonOperator(
        task_id='get_latest_month_year',
        python_callable=get_latest_month_year)

    task_download_from_s3_csv = PythonOperator(
        task_id='download_from_s3_csv',
        python_callable=download_from_s3_csv,
        op_kwargs={
        'bucket_name':'nyc-tlc',
        'prefix_file':'trip data/yellow_tripdata'
        })

    task_process_new_file = PythonOperator(
        task_id='process_new_file',
        python_callable=process_new_file)

    task_get_latest_month_year >> task_download_from_s3_csv
    task_download_from_s3_csv >> task_process_new_file
