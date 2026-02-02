from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.12",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
    ],
    install_requires=["apache-airflow==3.1.6"],
    extras_require={
        "develop": [
            "ruff==0.14.14",
        ],
        "testing": [
            "pytest==9.0.2",
        ],
    },
)
