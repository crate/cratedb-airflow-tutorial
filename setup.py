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
    install_requires=["apache-airflow==2.10.5"],
    extras_require={
        "develop": [
            "pylint==3.3.7",
            "black==25.1.0",
        ],
        "testing": [
            "pytest==8.3.5",
        ],
    },
)
