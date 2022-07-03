# import os

import duckdb

con = duckdb.connect()

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
# con.execute("SET s3_region='us-east-1';")
# con.execute(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID')}';")
# con.execute(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY')}';")
# con.execute(f"SET s3_session_token='{os.getenv('AWS_SESSION_TOKEN')}';")

# will only fetch headers, not the whole parquet file
con.execute(
    "SELECT * FROM parquet_schema('s3://gbif-open-data-ap-southeast-2/occurrence/2022-07-01/occurrence.parquet/000000');"
)
for r in con.fetchall():
    print(",".join([str(e) for e in r]))
