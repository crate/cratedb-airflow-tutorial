# crate-airflow-tutorial
Orchestration Project - Astronomer/Airflow tutorials


This repository contains examples of Apache Airflow DAGs for automating recurrent queries. All DAGs run on Astronomer infrastructure installed on Ubuntu 20.04.3 LTS.


## Installation

Before running examples make sure to set up the right environment:

* [Python3](https://www.python.org/downloads/)
* [Docker](https://www.docker.com/) version 18.09 and higher
* [Astronomer](https://www.astronomer.io/)

### Astronomer
Astronomer is the managed provider that allows users to easily run and monitor Apache Airflow environments. The best way to initialize and run projects on Astronomer is to use [Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart). To install its latest version on Ubuntu run:

```shell
curl -sSL https://install.astronomer.io | sudo bash
```

To make sure that Astronomer CLI is installed run:

```shell
astro version
```

For installation of Astronomer CLI on another operating system, please refer to the [official documentation](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart).

## Project files

The project directory has the following file structure:

```
  ├── dags # directory containing all DAGs
  ├── include # additional files which are used in DAGs
  ├── .astro # project settings
  ├── Dockerfile # runtime overrides for Astronomer Docker image
  ├── packages.txt # specification of OS-level packages
  ├── plugins # custom or community Airflow plugins
  ├── setup # additional setup-related scripts/database schemas
  └── requirements.txt # specification of Python packages
```

In the [dags](dags) directory you can find the specification of all DAGs for our examples.
Each DAG is accompanied by a tutorial:

* [table_export_dag.py](dags/table_export_dag.py) ([tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-automating-data-export-to-s3/901)): performs a daily export of table data to a remote filesystem (in our case S3)
* [data_retention_delete_dag.py](dags/data_retention_delete_dag.py) ([tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913)): implements a retention policy algorithm that drops expired partitions
* [data_retention_reallocate_dag.py](dags/data_retention_reallocate_dag.py) ([tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934)): implements a retention policy algorithm that reallocates expired partitions from hot nodes to cold nodes
* [data_retention_snapshot_dag.py](dags/data_retention_snapshot_dag.py): implements a retention policy algorithm that snapshots expired partitions to a repository
* [nyc_taxi_dag.py](dags/nyc_taxi_dag.py) ([tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-building-a-data-ingestion-pipeline/926)): imports [NYC Taxi data](https://github.com/toddwschneider/nyc-taxi-data) from AWS S3 into CrateDB
* [financial_data_dag.py](dags/financial_data_dag.py) ([tutorial](https://community.crate.io/t/cratedb-and-apache-airflow-automating-stock-data-collection-and-storage/990)): downloads financial data from S&P 500 companies and stores them into CrateDB
* [data_quality_checks_dag.py](dags/data_quality_checks_dag.py): loads incoming data to S3 then to CrateDB and checks several data quality properties. In case of failure, it sends Slack message.

## Start the project

To start the project on your local machine run:

```shell
astro dev start
```

To access the Apache Airflow UI go to `http://localhost:8081`.

From Airflow UI you can further manage running DAGs, check their status, the time of the next and last run and some metadata.

### Docker BuildKit issue

If your Docker environment has the [BuildKit feature](https://docs.docker.com/develop/develop-images/build_enhancements/) enabled, you may run into an error when starting the Astronomer project:

```shell
$ astro dev start
Env file ".env" found. Loading...
buildkit not supported by daemon
Error: command 'docker build -t astronomer-project_dccf4f/airflow:latest failed: failed to execute cmd: exit status 1
```

To overcome this issue, start Astronomer without the BuildKit feature: `DOCKER_BUILDKIT=0 astro dev start` (see the [Astronomer Forum](https://forum.astronomer.io/t/buildkit-not-supported-by-daemon-error-command-docker-build-t-airflow-astro-bcb837-airflow-latest-failed-failed-to-execute-cmd-exit-status-1/857)).

## Code linting

Before opening a pull request, please run [pylint](https://www.pylint.org) and [black](https://github.com/psf/black). To install all dependencies, run:

```shell
python -m pip install --upgrade -e ".[develop]"
python -m pip install --upgrade -r requirements.txt
```

Then run `pylint` and `black` using:

```shell
python -m pylint dags
python -m black .
```

## Testing

[Pytest](https://pytest.org/) is used for automated testing of DAGs. To set up test infrastructure locally, run:

```shell
python -m pip install --upgrade -e ".[testing]"
```

Tests can be run via:

```shell
python -m pytest -vvv
```
