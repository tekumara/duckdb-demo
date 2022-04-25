# to install: pip install duckdb
# to download the parquet file:
# wget https://github.com/cwida/duckdb-data/releases/download/v1.0/taxi_2019_04.parquet
import duckdb

print(
    duckdb.query(
        """
SELECT COUNT(*)
FROM 'taxi_2019_04.parquet'
WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20'
"""
    ).fetchall()
)
