from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=["apache-airflow==2.10.2"],
    extras_require={
        "develop": [
            "pylint==3.3.1",
            "black==24.10.0",
        ],
        "testing": [
            "pytest==8.3.3",
        ],
    },
)
