# import os

import duckdb
import pyarrow as pa

con = duckdb.connect()

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
# con.execute("SET s3_region='us-east-1';")
# con.execute(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID')}';")
# con.execute(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY')}';")
# con.execute(f"SET s3_session_token='{os.getenv('AWS_SESSION_TOKEN')}';")

# will only fetch headers, unlike pyarrow.parquet.read_table which fetchs whole file
tbl: pa.Table = con.execute(
    "SELECT * FROM parquet_schema('s3://gbif-open-data-ap-southeast-2/occurrence/2022-07-01/occurrence.parquet/000000');"
).fetch_arrow_table()

print(",".join(tbl.column_names))

for r in tbl.to_pylist():
    print(",".join([str(e) for e in r.values()]))
