from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=["apache-airflow==2.5.1"],
    extras_require={
        "develop": ["pylint==2.15.10", "black==23.1.0"],
        "testing": ["pytest==7.2.1"],
    },
)
