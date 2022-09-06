from airflow.decorators import task, dag
from datetime import datetime
from pathlib import Path
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable


@dag(dag_id="nyc-taxi-parquet",schedule_interval='@monthly',start_date=datetime(2009, 1, 1), catchup=True)
def taskflow():
    @task(task_id='get_month_year')
    def get_month_year(ds=None):
        currentMonth = int(ds.split('-')[1])
        currentYear = int(ds.split('-')[0])
        if currentMonth == 12:
            currentYear = currentYear - 1
        if currentMonth < 10:
            currentMonth = f'0{currentMonth}'

        return f'_{currentYear}-{currentMonth}'


    @task(task_id='download_files_s3')
    def download_from_s3_csv(file_date):
        local_path = Variable.get("local_path")
        s3_path = Variable.get("s3_path")
        file_name = f'yellow_tripdata{file_date}'
        file_path = f'{local_path}{file_name}.csv'
        BashOperator(
            task_id='download_from_s3_csv',
            bash_command=f'parquet-tools csv "{s3_path}{file_name}.parquet" > "{file_path}"',
            ).execute({})
        return file_path
   
    @task(task_id='process_new_files')
    def process_new_files(new_csv_file):
        local_path = Variable.get("local_path")

        PostgresOperator(
                task_id="copy_new_csv_file",
                postgres_conn_id="cratedb_demo_connection",
                sql=f"""
                        COPY nyc_taxi.load_trips_staging
                        FROM '{local_path}{new_csv_file}'
                        WITH (format = 'csv', empty_string_as_null = true)
                        RETURN SUMMARY;
                    """
            ).execute({})

        PostgresOperator(
            task_id="log_new_csv_file",
            postgres_conn_id="cratedb_demo_connection",
            sql=Path('include/taxi-insert.sql').read_text(encoding="utf-8"),
        ).execute({})

        PostgresOperator(
            task_id="purge_staging_new_csv_file",
            postgres_conn_id="cratedb_demo_connection",
            sql="DELETE FROM nyc_taxi.load_trips_staging;"
        ).execute({})

    process_new_files(download_from_s3_csv(get_month_year()))

dag = taskflow()
