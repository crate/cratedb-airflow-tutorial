from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    extras_require={
        "testing": [
            "apache-airflow==2.2.2",
            "pytest==6.2.5",
            "apache-airflow-providers-postgres==2.4.0",
        ],
    },
)
