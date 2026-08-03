from setuptools import find_packages, setup

setup(
    name="crate-airflow-tutorial",
    packages=find_packages(),
    python_requires=">=3.14",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.14",
    ],
    install_requires=["apache-airflow==3.3.0"],
    extras_require={
        "develop": [
            "ruff==0.16.1",
        ],
        "testing": [
            "pytest==9.1.1",
        ],
    },
)
