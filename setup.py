from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=["apache-airflow==2.6.3"],
    extras_require={
        "develop": [
            "pylint==2.17.4",
            "black==23.3.0",
        ],
        "testing": [
            # https://www.reddit.com/r/apache_airflow/comments/14oji77/airflow_262_and_pydantic_warningserrors/
            "pydantic<2",
            "pytest==7.4.0",
        ],
    },
)
