from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=["apache-airflow==2.7.3"],
    extras_require={
        "develop": [
            "pylint==3.0.2",
            "black==23.11.0",
        ],
        "testing": [
            "pytest==7.4.3",
        ],
    },
)
