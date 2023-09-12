from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=["apache-airflow==2.7.1"],
    extras_require={
        "develop": [
            "pylint==2.17.5",
            "black==23.7.0",
        ],
        "testing": [
            "pytest==7.4.0",
        ],
    },
)
