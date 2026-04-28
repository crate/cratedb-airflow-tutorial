from setuptools import setup, find_packages

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.13",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.13",
    ],
    install_requires=["apache-airflow==3.2.0"],
    extras_require={
        "develop": [
            "ruff==0.15.12",
        ],
        "testing": [
            "pytest==9.0.3",
        ],
    },
)
