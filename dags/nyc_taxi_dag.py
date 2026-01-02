"""
This DAG is intended to demonstrate how to import Parquet files into CrateDB instance using Airflow.
This is performed using the NYC taxi dataset which is publicly available in their website
here https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page the data is also available
in their public S3 Bucket s3://nyc-tlc/trip data/ or through the CDN described in the website above.

A detailed tutorial is available at https://community.crate.io/t/tutorial-how-to-automate-the-import-of-parquet-files-using-airflow/1247

Prerequisites
-------------
The variables ORIGIN_PATH and DESTINATION_PATH were configured
in Airflow interface on Admin > Variables as ORIGIN_PATH and DESTINATION_PATH, respectively.
The credentials and bucket info, were also configured using the approach described above.
In the CrateDB schema "nyc_taxi", the tables "load_trips_staging" and "trips" need to be
present before running the DAG. You can retrieve the CREATE TABLE statements
from the file setup/taxi-schema.sql in this repository.
"""

import pendulum
from airflow.models import Variable
from airflow.sdk import task, dag
from airflow.sdk.bases.operator import chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

# The URL of the directory containing the Parquet files
ORIGIN_PATH = Variable.get("ORIGIN_PATH", "test-path")
# Any local directory to which the Parquet files are temporarily downloaded to, such as /tmp
DESTINATION_PATH = Variable.get("DESTINATION_PATH", "test-path")
# The name of an S3 bucket to which CSV files are temporarily uploaded to
S3_BUCKET = Variable.get("S3_BUCKET", "test-bucket")
# AWS Access Key ID
ACCESS_KEY_ID = Variable.get("ACCESS_KEY_ID", "access-key")
# AWS Secret Access Key
SECRET_ACCESS_KEY = Variable.get("SECRET_ACCESS_KEY", "secret-key")

# Append trailing slash if missing
DESTINATION_PATH = (
    DESTINATION_PATH + "/" if not DESTINATION_PATH.endswith("/") else DESTINATION_PATH
)


# The configuration of the DAG was done based on the info shared by NYC TLC here: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# The documentation mentioned that the Parquet files are released monthly since January 2009
@dag(
    dag_id="nyc-taxi-parquet",
    schedule="@monthly",
    start_date=pendulum.datetime(2009, 3, 1, tz="UTC"),
    catchup=True,
    template_searchpath=["include"],
)
def taskflow():
    @task
    def format_file_name(ds=None):
        # The files are released with 2 months of delay, therefore the .subtract(months=2)
        timestamp = pendulum.parse(ds)
        timestamp = timestamp.subtract(months=2)
        date_formatted = timestamp.format("_YYYY-MM")
        return f"yellow_tripdata{date_formatted}"

    formatted_file_date = format_file_name()

    process_parquet = BashOperator(
        task_id="process_parquet",
        bash_command="""
                        curl -o "${destination_path}${formatted_file_date}.parquet" "${origin_path}${formatted_file_date}.parquet" &&
                        parquet-tools csv "${destination_path}${formatted_file_date}.parquet" > "${destination_path}${formatted_file_date}.csv"
                    """,
        env={
            "origin_path": ORIGIN_PATH,
            "destination_path": DESTINATION_PATH,
            "formatted_file_date": formatted_file_date,
        },
    )

    copy_csv_to_s3 = LocalFilesystemToS3Operator(
        task_id="copy_csv_to_s3",
        filename=f"{DESTINATION_PATH}{formatted_file_date}.csv",
        dest_bucket=S3_BUCKET,
        dest_key=f"{formatted_file_date}.csv",
        aws_conn_id="s3_conn",
        replace=True,
    )

    copy_csv_staging = SQLExecuteQueryOperator(
        task_id="copy_csv_staging",
        conn_id="cratedb_connection",
        # pylint: disable=C0301
        sql=f"""
                COPY nyc_taxi.load_trips_staging
                FROM 's3://{ACCESS_KEY_ID}:{SECRET_ACCESS_KEY}@{S3_BUCKET}/{formatted_file_date}.csv'
                WITH (format = 'csv', empty_string_as_null = true)
                RETURN SUMMARY;
            """,
    )

    copy_staging_to_trips = SQLExecuteQueryOperator(
        task_id="copy_staging_to_trips",
        conn_id="cratedb_connection",
        sql="taxi-insert.sql",
    )

    delete_staging = SQLExecuteQueryOperator(
        task_id="delete_staging",
        conn_id="cratedb_connection",
        sql="DELETE FROM nyc_taxi.load_trips_staging;",
    )

    delete_local_parquet_csv = BashOperator(
        task_id="delete_local_parquet_csv",
        bash_command="""
                        rm "${destination_path}${formatted_file_date}.parquet";
                        rm "${destination_path}${formatted_file_date}.csv"
                     """,
        env={
            "destination_path": DESTINATION_PATH,
            "formatted_file_date": formatted_file_date,
        },
    )

    chain(
        formatted_file_date,
        process_parquet,
        copy_csv_to_s3,
        copy_csv_staging,
        copy_staging_to_trips,
        delete_staging,
        delete_local_parquet_csv,
    )


taskflow()
