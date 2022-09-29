"""
This DAG is intended to demonstrate how to import Parquet files into CrateDB instance using Airflow.
This is performed using the NYC taxi dataset which is publicly available in their website
here https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page the data is also available
in their public S3 Bucket s3://nyc-tlc/trip data/ or through the CDN described in the website above.

A detailed tutorial is available at TBD

Prerequisites
-------------
The variables ORIGIN_PATH and DESTINATION_PATH were configured
in Airflow interface on Admin > Variables as ORIGIN_PATH and DESTINATION_PATH, respectively.
The credentials and bucket info, were also configured using the approach described above.
In the CrateDB schema "nyc_taxi", the tables "load_trips_staging" and "trips" need to be
present before running the DAG. You can retrieve the CREATE TABLE statements
from the file setup/taxi-schema.sql in this repository.

"""

from pathlib import Path
import pendulum
from airflow.models import Variable
from airflow.decorators import task, dag
from airflow.models.baseoperator import chain

from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

ORIGIN_PATH = Variable.get("ORIGIN_PATH", "test-path")
DESTINATION_PATH = Variable.get("DESTINATION_PATH", "test-path")
S3_KEY = Variable.get("S3_KEY", "test-key")
S3_BUCKET = Variable.get("S3_BUCKET", "test-bucket")
ACCESS_KEY_ID = Variable.get("ACCESS_KEY_ID", "access-key")
SECRET_ACCESS_KEY = Variable.get("SECRET_ACCESS_KEY", "secret-key")

# The configuration of the DAG was done based on the info shared by NYC TLC here: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# The documentation mentioned that the Parquet files are released @monthly since January 2009
@dag(
    dag_id="nyc-taxi-parquet",
    schedule="@monthly",
    start_date=pendulum.datetime(2009, 3, 1, tz="UTC"),
    catchup=False,
)
def taskflow():
    @task(task_id="format_file_name")
    def formated_file_name(ds=None):
        # The files are released with 2 months of delay, therefore the .subtract(months=2)
        timestamp = pendulum.parse(ds)
        timestamp = timestamp.subtract(months=3)
        date_formated = timestamp.format("_YYYY-MM")
        return f"yellow_tripdata{date_formated}"

    formated_file_date = formated_file_name()

    process_parquet = BashOperator(
        task_id="process_parquet",
        bash_command="""
            curl -o "${{params.DESTINATION_PATH}}{{ task_instance.xcom_pull(task_ids='format_file_name')}}.parquet" {{params.ORIGIN_PATH}}{{ task_instance.xcom_pull(task_ids='format_file_name')}}.parquet;
            parquet-tools csv "${{params.DESTINATION_PATH}}{{ task_instance.xcom_pull(task_ids='format_file_name')}}.parquet" > "${{params.DESTINATION_PATH}}{{ task_instance.xcom_pull(task_ids='format_file_name')}}.csv"
        """,
        params={
            "ORIGIN_PATH": ORIGIN_PATH,
            "DESTINATION_PATH": DESTINATION_PATH,
        },
    )
    copy_csv_to_s3 = LocalFilesystemToS3Operator(
        task_id="copy_csv_to_s3",
        filename=f"{DESTINATION_PATH}{formated_file_date}.csv",
        dest_key=f"{S3_KEY}{formated_file_date}.csv",
        aws_conn_id="s3_conn",
        replace=True,
    )

    copy_csv_staging = PostgresOperator(
        task_id="copy_csv_staging",
        postgres_conn_id="cratedb_demo_connection",
        sql=f"""
                COPY nyc_taxi.load_trips_staging
                FROM 's3://{ACCESS_KEY_ID}:{SECRET_ACCESS_KEY}@{S3_BUCKET}{formated_file_date}.csv' 
                WITH (format = 'csv', empty_string_as_null = true)
                RETURN SUMMARY;
            """,
    )

    copy_staging_to_trips = PostgresOperator(
        task_id="copy_staging_to_trips",
        postgres_conn_id="cratedb_demo_connection",
        sql=Path("include/taxi-insert.sql").read_text(encoding="utf-8"),
    )

    delete_staging = PostgresOperator(
        task_id="delete_staging",
        postgres_conn_id="cratedb_demo_connection",
        sql="DELETE FROM nyc_taxi.load_trips_staging;",
    )

    delete_local_parquet_csv = BashOperator(
        task_id="delete_local_parquet_csv",
        bash_command="""
        rm "${{params.DESTINATION_PATH}}{{ task_instance.xcom_pull(task_ids='format_file_name')}}.parquet" "${{params.DESTINATION_PATH}}{{ task_instance.xcom_pull(task_ids='format_file_name')}}.csv"
        """,
        params={"DESTINATION_PATH": DESTINATION_PATH},
    )

    chain(
        formated_file_date,
        process_parquet,
        copy_csv_to_s3,
        copy_csv_staging,
        copy_staging_to_trips,
        delete_staging,
        delete_local_parquet_csv,
    )


taskflow()
