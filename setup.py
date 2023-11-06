from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=["apache-airflow==2.7.2"],
    extras_require={
        "develop": [
            "pylint==3.0.2",
            "black==23.10.1",
        ],
        "testing": [
            "pytest==7.4.2",
        ],
    },
)
