from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "apache-airflow==2.3.1",
        "apache-airflow-providers-postgres==4.1.0",
    ],
    extras_require={
        "develop": [
            "pylint==2.13.9",
        ],
        "testing": [
            "pytest==7.1.2",
        ],
    },
)
