from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "apache-airflow==2.3.4",
        "apache-airflow-providers-postgres==5.2.0",
    ],
    extras_require={
        "develop": [
            "pylint==2.14.5",
        ],
        "testing": [
            "pytest==7.1.2",
        ],
    },
)
