from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=["apache-airflow==2.8.2"],
    extras_require={
        "develop": [
            "pylint==3.0.3",
            "black==24.1.1",
        ],
        "testing": [
            "pytest==8.0.0",
        ],
    },
)
