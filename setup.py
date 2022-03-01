from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "apache-airflow==2.2.3",
        "apache-airflow-providers-postgres==3.0.0",
    ],
    extras_require={
        "develop": [
            "pylint==2.12.2",
        ],
        "testing": [
            "pytest==6.2.5",
        ],
    },
)
