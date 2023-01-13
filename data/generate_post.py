#!/usr/bin/env python3

import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date

# init for ported postgres modify feature: ported_postgres
# ported_postgres/loc1.parquet
schema = pa.schema([
    pa.field('f1', pa.int32()),
    pa.field('f2', pa.string())])

df = pd.DataFrame({'f1': [], 'f2': []})
table = pa.Table.from_pandas(df, schema)
with pq.ParquetWriter('ported_postgres/loc1.parquet', table.schema) as writer:
    writer.write_table(table)

# ported_postgres/gloc1.parquet
schema = pa.schema([
    pa.field('a', pa.int32()),
    pa.field('b', pa.int32())])

df = pd.DataFrame({'a': [], 'b': []})
table = pa.Table.from_pandas(df, schema)
with pq.ParquetWriter('ported_postgres/gloc1.parquet', table.schema) as writer:
    writer.write_table(table)

# ported_postgres/loct.parquet
schema = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('aa', pa.string()),
    pa.field('bb', pa.string())])

df = pd.DataFrame({'id':[], 'aa': [], 'bb': []})
table = pa.Table.from_pandas(df, schema)
with pq.ParquetWriter('ported_postgres/loct.parquet', table.schema) as writer:
    writer.write_table(table)

# ported_postgres/loct1.parquet
schema = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('f1', pa.int32()),
    pa.field('f2', pa.int32()),
    pa.field('f3', pa.int32())])

df = pd.DataFrame({'id':[], 'f1': [], 'f2': [], 'f3': []})
table = pa.Table.from_pandas(df, schema)
with pq.ParquetWriter('ported_postgres/loct1.parquet', table.schema) as writer:
    writer.write_table(table)

# ported_postgres/loct2.parquet
schema = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('f1', pa.int32()),
    pa.field('f2', pa.int32()),
    pa.field('f3', pa.int32())])

df = pd.DataFrame({'id':[], 'f1': [], 'f2': [], 'f3': []})
table = pa.Table.from_pandas(df, schema)
with pq.ParquetWriter('ported_postgres/loct2.parquet', table.schema) as writer:
    writer.write_table(table)

# ported_postgres/loct4.parquet
schema = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('f1', pa.int32()),
    pa.field('f2', pa.int32()),
    pa.field('f3', pa.int32())])

df = pd.DataFrame({'id':[], 'f1': [], 'f2': [], 'f3': []})
table = pa.Table.from_pandas(df, schema)
with pq.ParquetWriter('ported_postgres/loct4.parquet', table.schema) as writer:
    writer.write_table(table)

# ported_postgres/loct3_1.parquet
schema = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('a', pa.int32()),
    pa.field('b', pa.string())])

df = pd.DataFrame({'id':[], 'a': [], 'b': []})
table = pa.Table.from_pandas(df, schema)
with pq.ParquetWriter('ported_postgres/loct3_1.parquet', table.schema) as writer:
    writer.write_table(table)

# ported_postgres/loct4_1.parquet
schema = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('a', pa.int32()),
    pa.field('b', pa.string())])

df = pd.DataFrame({'id':[], 'a': [], 'b': []})
table = pa.Table.from_pandas(df, schema)
with pq.ParquetWriter('ported_postgres/loct4_1.parquet', table.schema) as writer:
    writer.write_table(table)
