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
    install_requires=["apache-airflow==3.1.0"],
    extras_require={
        "develop": [
            "pylint==3.3.8",
            "black==25.9.0",
        ],
        "testing": [
            "pytest==8.4.2",
        ],
    },
)
