from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=["apache-airflow==2.9.3"],
    extras_require={
        "develop": [
            "pylint==3.2.6",
            "black==24.4.2",
        ],
        "testing": [
            "pytest==8.3.2",
        ],
    },
)
