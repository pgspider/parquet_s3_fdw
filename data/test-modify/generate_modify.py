#!/usr/bin/env python3

import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date

# for parquet_modify_2 directory
df1 = pd.DataFrame({'id': [0, 1],
                    'a': ['zero', 'one']})
table1 = pa.Table.from_pandas(df1)

df2 = pd.DataFrame({'id': [2, 3],
                    'a': ['zerozero', 'oneone']})
table2 = pa.Table.from_pandas(df2)

df3 = pd.DataFrame({'id': [4, 5],
                    'a': ['zerozerozero', 'oneoneone']})
table3 = pa.Table.from_pandas(df3)

with pq.ParquetWriter('./parquet_modify_2/t1_table.parquet', table1.schema) as writer:
    writer.write_table(table1)

with pq.ParquetWriter('./parquet_modify_2/t2_table.parquet', table2.schema) as writer:
    writer.write_table(table2)

with pq.ParquetWriter('./parquet_modify_2/t3_table.parquet', table3.schema) as writer:
    writer.write_table(table3)


df4 = pd.DataFrame({'id': [6, 7],
                    'b': ['zero', 'one']})
table4 = pa.Table.from_pandas(df4)
with pq.ParquetWriter('./parquet_modify_2/t4_table.parquet', table4.schema) as writer:
    writer.write_table(table4)


# for parquet_modify_3 directory
df1 = pd.DataFrame({'id': [0, 1],
                    'a': ['zero', 'one']})
table1 = pa.Table.from_pandas(df1)

df2 = pd.DataFrame({'id': [2, 3],
                    'a': ['zerozero', 'oneone']})
table2 = pa.Table.from_pandas(df2)

df3 = pd.DataFrame({'id': [4, 5],
                    'a': ['zerozerozero', 'oneoneone']})
table3 = pa.Table.from_pandas(df3)

with pq.ParquetWriter('./parquet_modify_3/t1_table.parquet', table1.schema) as writer:
    writer.write_table(table1)

with pq.ParquetWriter('./parquet_modify_3/t2_table.parquet', table2.schema) as writer:
    writer.write_table(table2)

with pq.ParquetWriter('./parquet_modify_3/t3_table.parquet', table3.schema) as writer:
    writer.write_table(table3)


df4 = pd.DataFrame({'id': [6, 7],
                    'b': ['zero', 'one']})
table4 = pa.Table.from_pandas(df4)
with pq.ParquetWriter('./parquet_modify_3/t4_table.parquet', table4.schema) as writer:
    writer.write_table(table4)

# for parquet_modify directory
df_tmp = pd.DataFrame({'id':[10, 20],
                    'a':['a', 'b']})
table_tmp = pa.Table.from_pandas(df_tmp)
with pq.ParquetWriter('./parquet_modify/tmp_table.parquet', table_tmp.schema) as writer:
    writer.write_table(table_tmp)

df_tmp1 = pd.DataFrame({'id':[1.1, 2.2],
                    'a':['a', 'b']})
table_tmp1 = pa.Table.from_pandas(df_tmp1)
with pq.ParquetWriter('./parquet_modify/tmp1_table.parquet', table_tmp1.schema) as writer:
    writer.write_table(table_tmp1)

mdt1 = pa.map_(pa.string(), pa.string())
df_tmp2 = pd.DataFrame({
        'id': pd.Series([
            [('1', 'foo')], [('2', 'bar')]]),
        'a': pd.Series(['a', 'b'])
    }
)
schema = pa.schema([
    pa.field('id', mdt1),
    pa.field('a', pa.string())])

table_tmp2 = pa.Table.from_pandas(df_tmp2, schema)
with pq.ParquetWriter('./parquet_modify/tmp2_table.parquet', table_tmp2.schema) as writer:
    writer.write_table(table_tmp2)


df_tmp3 = pd.DataFrame({'id':[[1, 2], [3, 4]],
                    'a':['a', 'b']})
table_tmp3 = pa.Table.from_pandas(df_tmp3)
with pq.ParquetWriter('./parquet_modify/tmp3_table.parquet', table_tmp3.schema) as writer:
    writer.write_table(table_tmp3)

df_tmp4 = pd.DataFrame({'id':[True, False, True, False],
                    'a':['a', 'b', 'c', 'd']})
table_tmp4 = pa.Table.from_pandas(df_tmp4)

with pq.ParquetWriter('./parquet_modify/tmp4_table.parquet', table_tmp4.schema) as writer:
    writer.write_table(table_tmp4)

df_tmp5 = pd.DataFrame({'id':[['a', 'b'], ['c', 'd']],
                    'a':['a', 'b']})
table_tmp5 = pa.Table.from_pandas(df_tmp5)
with pq.ParquetWriter('./parquet_modify/tmp5_table.parquet', table_tmp5.schema) as writer:
    writer.write_table(table_tmp5)

schema1 = pa.schema([
    pa.field('c1', pa.int16()),
    pa.field('c2', pa.string()),
	pa.field('c3', pa.bool_())])

df_t1 = pd.DataFrame({'c1': [], 'c2': [], 'c3': []})
table_t1 = pa.Table.from_pandas(df_t1, schema1)
with pq.ParquetWriter('./parquet_modify/ft1_int2.parquet', table_t1.schema) as writer:
    writer.write_table(table_t1)

schema2 = pa.schema([
    pa.field('c1', pa.int32()),
    pa.field('c2', pa.string()),
	pa.field('c3', pa.bool_())])

df_t2 = pd.DataFrame({'c1': [], 'c2': [], 'c3': []})
table_t2 = pa.Table.from_pandas(df_t2, schema2)
with pq.ParquetWriter('./parquet_modify/ft1_int4.parquet', table_t2.schema) as writer:
    writer.write_table(table_t2)

schema3 = pa.schema([
    pa.field('c1', pa.int64()),
    pa.field('c2', pa.string()),
	pa.field('c3', pa.bool_())])

df_t3 = pd.DataFrame({'c1': [], 'c2': [], 'c3': []})
table_t3 = pa.Table.from_pandas(df_t3, schema3)
with pq.ParquetWriter('./parquet_modify/ft1_int8.parquet', table_t3.schema) as writer:
    writer.write_table(table_t3)

schema = pa.schema([
    pa.field('c1', pa.float32()),
    pa.field('c2', pa.string()),
	pa.field('c3', pa.bool_())])

df = pd.DataFrame({'c1': [], 'c2': [], 'c3': []})
table = pa.Table.from_pandas(df, schema)
with pq.ParquetWriter('./parquet_modify/ft1_float4.parquet', table.schema) as writer:
    writer.write_table(table)

schema = pa.schema([
    pa.field('c1', pa.float64()),
    pa.field('c2', pa.string()),
	pa.field('c3', pa.bool_())])

df = pd.DataFrame({'c1': [], 'c2': [], 'c3': []})
table = pa.Table.from_pandas(df, schema)
with pq.ParquetWriter('./parquet_modify/ft1_float8.parquet', table.schema) as writer:
    writer.write_table(table)

schema4 = pa.schema([
    pa.field('c1', pa.date32()),
    pa.field('c2', pa.string()),
	pa.field('c3', pa.float64())])

df_t4 = pd.DataFrame({'c1': [date(2018, 1, 1)], 'c2': ['un'], 'c3': [0.0]})
table_t4 = pa.Table.from_pandas(df_t4, schema4)
with pq.ParquetWriter('./parquet_modify/ft1_date.parquet', table_t4.schema) as writer:
    writer.write_table(table_t4)

schema5 = pa.schema([
    pa.field('c1', pa.string()),
    pa.field('c2', pa.string()),
	pa.field('c3', pa.float64())])

df_t5 = pd.DataFrame({'c1': [], 'c2': [], 'c3': []})
table_t5 = pa.Table.from_pandas(df_t5, schema5)
with pq.ParquetWriter('./parquet_modify/ft1_text.parquet', table_t5.schema) as writer:
    writer.write_table(table_t5)

schema6 = pa.schema([
    pa.field('c1', pa.timestamp('s')),
    pa.field('c2', pa.string()),
	pa.field('c3', pa.float64())])

df_t6 = pd.DataFrame({'c1': [datetime(2018, 1, 1)], 'c2': ['une'], 'c3': [0.0]})
table_t6 = pa.Table.from_pandas(df_t6, schema6)
with pq.ParquetWriter('./parquet_modify/ft1_timestamp.parquet', table_t6.schema) as writer:
    writer.write_table(table_t6)

schema_update = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('a', pa.int32()),
    pa.field('b', pa.int32()),
	pa.field('c', pa.string())])

df_update = pd.DataFrame({'id': [],'a': [], 'b': [], 'c': []})
table_update = pa.Table.from_pandas(df_update, schema_update)
with pq.ParquetWriter('./parquet_modify/update_test.parquet', table_update.schema) as writer:
    writer.write_table(table_update)

schema_delete = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('a', pa.int32()),
	pa.field('b', pa.string())])

df_delete = pd.DataFrame({'id': [], 'a': [], 'b': []})
table_delete = pa.Table.from_pandas(df_delete, schema_delete)
with pq.ParquetWriter('./parquet_modify/delete_test.parquet', table_delete.schema) as writer:
    writer.write_table(table_delete)

schema_tmp = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('a', pa.int32()),
	pa.field('b', pa.string())])

df_tmptest = pd.DataFrame({'id': [], 'a': [], 'b': []})
table_tmptest = pa.Table.from_pandas(df_tmptest, schema_tmp)
with pq.ParquetWriter('./parquet_modify/tmp_test.parquet', table_tmptest.schema) as writer:
    writer.write_table(table_tmptest)

schema_ft1 = pa.schema([
    pa.field('c1', pa.int32()),
    pa.field('c2', pa.string()),
	pa.field('c3', pa.timestamp('us'))])

ts_arr = np.array([], dtype=object)

df_ft1 = pd.DataFrame({'c1': [], 'c2': [], 'c3': ts_arr})
table_ft1 = pa.Table.from_pandas(df_ft1, schema_ft1)
with pq.ParquetWriter('./parquet_modify/ft1_table.parquet', schema_ft1) as writer:
    writer.write_table(table_ft1)

# for parquet_modify_4 directory
mdt2 = pa.list_(pa.float64())
mdt3 = pa.list_(pa.string())
mdt4 = pa.map_(pa.string(), pa.int32())
schema_listmap = pa.schema([
    pa.field('id', pa.int32()),
    pa.field('c1', mdt1),
	pa.field('c2', mdt4),
	pa.field('c3', mdt2),
	pa.field('c4', mdt3),
	pa.field('c5', mdt3)])

df_listmap = pd.DataFrame({'id': [1], 'c1': [[('f', 'foo')]], 'c2': [[('f', 1)]], 'c3': [[0.0,0.0]], 'c4': [['a','a']], 'c5': [['a','a']]})
table_listmap = pa.Table.from_pandas(df_listmap, schema_listmap)
with pq.ParquetWriter('./parquet_modify_4/ft2_table.parquet', table_listmap.schema) as writer:
    writer.write_table(table_listmap)

# for parquet_modify_5 directory
schema_new = pa.schema([
    pa.field('c1', pa.int32()),
    pa.field('c2', pa.string())])

df_new = pd.DataFrame({'c1': [], 'c2': []})
table_new = pa.Table.from_pandas(df_new, schema_new)
with pq.ParquetWriter('./parquet_modify_5/ft_new.parquet', table_new.schema) as writer:
    writer.write_table(table_new)
# for parquet_modify_6 directory
schema_s1 = pa.schema([
    pa.field('c1', pa.int32()),
    pa.field('c2', pa.string())])

df_sorted_int = pd.DataFrame({'c1': [], 'c2': []})
table_sorted_int = pa.Table.from_pandas(df_sorted_int, schema_s1)
with pq.ParquetWriter('./parquet_modify_6/ft_sorted_int.parquet', table_sorted_int.schema) as writer:
    writer.write_table(table_sorted_int)

df_sorted_text = pd.DataFrame({'c1': [], 'c2': []})
table_sorted_text = pa.Table.from_pandas(df_sorted_text, schema_s1)
with pq.ParquetWriter('./parquet_modify_6/ft_sorted_text.parquet', table_sorted_text.schema) as writer:
    writer.write_table(table_sorted_text)

schema_s2 = pa.schema([
    pa.field('c1', pa.int32()),
    pa.field('c2', pa.date32())])

df_sorted_date = pd.DataFrame({'c1': [0], 'c2': [date(2018, 1, 1)]})
table_sorted_date = pa.Table.from_pandas(df_sorted_date, schema_s2)
with pq.ParquetWriter('./parquet_modify_6/ft_sorted_date.parquet', table_sorted_date.schema) as writer:
    writer.write_table(table_sorted_date)

schema_s3 = pa.schema([
    pa.field('c1', pa.int32()),
    pa.field('c2', pa.timestamp('s'))])

df_sorted_time = pd.DataFrame({'c1': [0], 'c2': [datetime(2018, 1, 1)]})
table_sorted_time = pa.Table.from_pandas(df_sorted_time, schema_s3)
with pq.ParquetWriter('./parquet_modify_6/ft_sorted_time.parquet', table_sorted_time.schema) as writer:
    writer.write_table(table_sorted_time)
