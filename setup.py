from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "apache-airflow==2.2.5",
        "apache-airflow-providers-postgres==4.1.0",
    ],
    extras_require={
        "develop": [
            "pylint==2.13.7",
        ],
        "testing": [
            "pytest==7.1.2",
        ],
    },
)
