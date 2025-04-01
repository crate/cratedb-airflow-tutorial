from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.12",
    install_requires=["apache-airflow==2.10.5"],
    extras_require={
        "develop": [
            "pylint==3.3.6",
            "black==25.1.0",
        ],
        "testing": [
            "pytest==8.3.5",
        ],
    },
)
