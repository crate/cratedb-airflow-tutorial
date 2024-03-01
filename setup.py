from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=["apache-airflow==2.8.1"],
    extras_require={
        "develop": [
            "pylint==3.1.0",
            "black==24.2.0",
        ],
        "testing": [
            "pytest==8.0.2",
        ],
    },
)
