# see https://github.com/duckdb/duckdb/blob/v0.3.3/tools/pythonpkg/tests/fast/arrow/test_arrow_recordbatchreader.py

import duckdb
import os
import pyarrow
import pyarrow.parquet
import pyarrow.dataset

duckdb_conn = duckdb.connect()
duckdb_conn.execute("PRAGMA threads=4")

parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

userdata_parquet_dataset= pyarrow.dataset.dataset([
    parquet_filename,
    parquet_filename,
    parquet_filename,
]
, format="parquet")

batches= [r for r in userdata_parquet_dataset.to_batches()]
reader=pyarrow.dataset.Scanner.from_batches(batches,userdata_parquet_dataset.schema).to_reader()

rel = duckdb_conn.from_arrow(reader)

assert rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 12
# The reader is already consumed so this should be 0
assert rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 0
