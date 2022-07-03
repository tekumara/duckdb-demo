from setuptools import find_packages, setup

setup(
    name="duckdb-demo",
    version="0.0.0",
    description="DuckDB Demo",
    python_requires=">=3.7",
    packages=find_packages(exclude=["tests"]),
    package_data={
        "": ["py.typed"],
    },
    install_requires=["duckdb~=0.4", "pyarrow~=8.0", "pandas~=1.4"],
    extras_require={
        "dev": [
            "autoflake~=1.4",
            "black~=22.3",
            "build~=0.7",
            "isort~=5.9",
            "flake8~=4.0",
            "flake8-annotations~=2.7",
            "flake8-colors~=0.1",
            "pre-commit~=2.15",
            "pytest~=7.0",
        ]
    },
)
