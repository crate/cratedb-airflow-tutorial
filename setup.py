from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=["apache-airflow==2.7.1"],
    extras_require={
        "develop": [
            "pylint==2.17.7",
            "black==23.9.1",
        ],
        "testing": [
            "pytest==7.4.2",
        ],
    },
)
