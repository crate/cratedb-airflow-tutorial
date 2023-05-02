from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=["apache-airflow==2.6.0"],
    extras_require={
        "develop": [
            "pylint==2.17.1",
            "black==23.3.0",
        ],
        "testing": ["pytest==7.3.1"],
    },
)
