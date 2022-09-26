"""
This DAG is intended to demonstrate how to import Parquet files into a CrateDB instance.
This is performed using the NYC taxi datasetwhich is publicly available in their website
here https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page the data is also available
in their public S3 Bucket s3://nyc-tlc/trip data/. However due to its latest update,
the bucket is no longer accessible.


Prerequisites
-------------
The variables S3_BUCKET_PATH and DESTINATION_PATH were configured
in Airflow interface on Admin > Variables as s3_path and destination_path, respectively.
In the CrateDB schema "nyc_taxi", the tables "load_files_processed",
"load_trips_staging" and "trips" need to be present before running the DAG.
You can retrieve the CREATE TABLE statements from the file setup/taxi-schema.sql
in this repository.

"""

from pathlib import Path
from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pendulum

S3_BUCKET_PATH = Variable.get("s3_path")
DESTINATION_PATH = Variable.get("destination_path")

#The configuration of the DAG was done based on the info shared by NYC TLC here: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
#The documentation mentioned that the Parquet files are released @monthly since January 2009
@dag(dag_id="nyc-taxi-parquet",
schedule='@monthly',
start_date=pendulum.datetime(2009, 3, 1, tz="UTC"),
catchup=True)
def taskflow():
    @task(task_id='format_file_name')
    def format_file_name(ds=None):
        #The files are released with 2 months of delay therefore the -2
        currentMonth = int(ds.split('-')[1]) - 2
        currentYear = int(ds.split('-')[0])
        if currentMonth == 12:
            currentYear = currentYear - 1
        if currentMonth < 10:
            currentMonth = f'0{currentMonth}'
        return f'yellow_tripdata_{currentYear}-{currentMonth}'

    download_from_s3_csv = BashOperator(
            task_id='download_from_s3_csv',
            bash_command='''
                    parquet-tools csv "{{params.S3_BUCKET_PATH}}{{ti.xcom_pull(task_ids="format_file_name")}}.parquet"
                    > "{{params.DESTINATION_PATH}}{{ti.xcom_pull(task_ids="format_file_name")}}.csv"
            ''',
            params={'S3_BUCKET_PATH': S3_BUCKET_PATH, 'DESTINATION_PATH': DESTINATION_PATH},
        )

    copy_new_csv_file = PostgresOperator(
        task_id="copy_new_csv_file",
        postgres_conn_id="cratedb_demo_connection",
        sql="""
                COPY nyc_taxi.load_trips_staging
                FROM '{{params.DESTINATION_PATH}}{{ti.xcom_pull(task_ids="format_file_name")}}.csv'
                WITH (format = 'csv', empty_string_as_null = true)
                RETURN SUMMARY;
            """,
        params={'S3_BUCKET_PATH': S3_BUCKET_PATH, 'DESTINATION_PATH': DESTINATION_PATH}
    )

    log_new_csv_file = PostgresOperator(
        task_id="log_new_csv_file",
        postgres_conn_id="cratedb_demo_connection",
        sql=Path('include/taxi-insert.sql').read_text(encoding="utf-8"),
    )

    purge_staging_new_csv_file = PostgresOperator(
        task_id="purge_staging_new_csv_file",
        postgres_conn_id="cratedb_demo_connection",
        sql="DELETE FROM nyc_taxi.load_trips_staging;"
    )
    download_step = format_file_name() >> download_from_s3_csv
    download_step >> copy_new_csv_file
    copy_new_csv_file >> log_new_csv_file
    log_new_csv_file >> purge_staging_new_csv_file
taskflow()
