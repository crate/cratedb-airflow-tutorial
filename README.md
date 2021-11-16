# crate-astro-tutorial
Orchestration Project - Astronomer/Airflow tutorials

This repository contains examples of Apache Airflow DAGs for automating recurrent queires. All DAGs run on Astronomer infrastructure installed on Ubuntu 20.04.3 LTS.

## Installation

Before running examples make sure to set up the right environment:

* [Python3](https://www.python.org/downloads/)
* [Docker](https://www.docker.com/) version 18.09 and higher
* [Astronomer](https://www.astronomer.io/)

### Astronomer
Astronomer is the managed provider that allows users to easily run and monitor Apache Airflow environments. The best way to initialize and run projects on Astronomer is to use [Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart). To install its latest version on Ubuntu run:

`curl -sSL https://install.astronomer.io | sudo bash`

To make sure that Astronomer CLI is installed run:

`astro version`

For installation of Astronomer CLI on another operating system, please refer to the [official documentation](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart).

## Project files

Project directory has the following file structure:
```
  ├── dags # directory containing all DAGs
      ├── example-dag.py
      ├── table-export-dag.py
  ├── .astro # project settings
  ├── Dockerfile # runtime overrides for Astronomer Docker image
  ├── include # other project files
  ├── packages.txt # specification of OS-level packages
  ├── plugins # custom or community Airflow plugins
  └── requirements.txt # specification of Python packages
```
In the `dag` directory you can find specification of all DAGs for our examples:
* `example-dag.py` is generated during project initialization
* `table-export-dag.py` performs daily export of table data to a remote filesystem (in our case S3)

## Start the project

To start the project on your local machine run:

`astro dev start`

To access the Apache Airflow UI go to `http://localhost:8081`.
From Airflow UI you can further manage running DAGs, check their status, the time of the next and last run and some metadata. 
