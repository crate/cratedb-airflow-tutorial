from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=["apache-airflow==2.6.3"],
    extras_require={
        "develop": [
            "pylint==2.17.4",
            "black==23.7.0",
        ],
        "testing": [
            "pytest==7.4.0",
        ],
    },
)
