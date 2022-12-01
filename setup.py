from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=["apache-airflow==2.4.2"],
    extras_require={
        "develop": ["pylint==2.15.7", "black==22.10.0"],
        "testing": ["pytest==7.2.0"],
    },
)
