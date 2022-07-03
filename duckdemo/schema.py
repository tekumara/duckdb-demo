import duckdb

con = duckdb.connect()

con.execute("SELECT * FROM parquet_schema('data/userdata1.parquet');")
for r in con.fetchall():
    print(r)
