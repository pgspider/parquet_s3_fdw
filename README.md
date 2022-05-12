# Parquet S3 Foreign Data Wrapper for PostgreSQL

This PostgreSQL extension is a Foreign Data Wrapper (FDW) for accessing Parquet file on local file system and [Amazon S3][2].
This version of parquet_s3_fdw can work for PostgreSQL 13.

Read-only Apache Parquet foreign data wrapper supporting S3 access for PostgreSQL.


## Installation
### 1. Install dependent libraries
`parquet_s3_fdw` requires `libarrow` and `libparquet` installed in your system (requires version 0.15+, for previous versions use branch [arrow-0.14](https://github.com/adjust/parquet_fdw/tree/arrow-0.14)). Please refer to [building guide](https://github.com/apache/arrow/blob/master/docs/source/developers/cpp/building.rst).

`AWS SDK for C++ (libaws-cpp-sdk-core libaws-cpp-sdk-s3)` is also required (Confirmed version is 1.8.14).

Attention!  
We reccomend to build `libarrow`, `libparquet` and `AWS SDK for C++` from the source code. We failed to link if using pre-compiled binaries because gcc version is different between arrow and AWS SDK.

### 2. Build and install parquet_s3_fdw
```sh
make install
```
or in case when PostgreSQL is installed in a custom location:
```sh
make install PG_CONFIG=/path/to/pg_config
```
It is possible to pass additional compilation flags through either custom
`CCFLAGS` or standard `PG_CFLAGS`, `PG_CXXFLAGS`, `PG_CPPFLAGS` variables.

## Usage
### Load extension
```sql
CREATE EXTENSION parquet_s3_fdw;
```

### Create server
```sql
CREATE SERVER parquet_s3_srv FOREIGN DATA WRAPPER parquet_s3_fdw;
```
If using [MinIO][3] instead of AWS S3, please use use_minio option for create server.
```sql
CREATE SERVER parquet_s3_srv FOREIGN DATA WRAPPER parquet_s3_fdw OPTIONS (use_minio 'true');
```

### Create user mapping
You have to specify user name and password if accessing Amazon S3.
```sql
CREATE USER MAPPING FOR public SERVER parquet_s3_srv OPTIONS (user 's3user', password 's3password');
```

### Create foreign table
Now you should be able to create foreign table from Parquet files. Currently `parquet_s3_fdw` supports the following column [types](https://github.com/apache/arrow/blob/master/cpp/src/arrow/type.h) (to be extended shortly):

|   Arrow type |  SQL type |
|-------------:|----------:|
|         INT8 |      INT2 |
|        INT16 |      INT2 |
|        INT32 |      INT4 |
|        INT64 |      INT8 |
|        FLOAT |    FLOAT4 |
|       DOUBLE |    FLOAT8 |
|    TIMESTAMP | TIMESTAMP |
|       DATE32 |      DATE |
|       STRING |      TEXT |
|       BINARY |     BYTEA |
|         LIST |     ARRAY |
|          MAP |     JSONB |

Currently `parquet_s3_fdw` doesn't support structs and nested lists.

Following options are supported:
* **filename** - space separated list of paths to Parquet files to read. You can specify the path on AWS S3 by starting with `s3://`. The mix of local path and S3 path is not supported;
* **dirname** - path to directory having Parquet files to read;
* **sorted** - space separated list of columns that Parquet files are presorted by; that would help postgres to avoid redundant sorting when running query with `ORDER BY` clause or in other cases when having a presorted set is beneficial (Group Aggregate, Merge Join);
* **files_in_order** - specifies that files specified by `filename` or returned by `files_func` are ordered according to `sorted` option and have no intersection rangewise; this allows to use `Gather Merge` node on top of parallel Multifile scan (default `false`);
* **use_mmap** - whether memory map operations will be used instead of file read operations (default `false`);
* **use_threads** - enables Apache Arrow's parallel columns decoding/decompression (default `false`);
* **files_func** - user defined function that is used by parquet_s3_fdw to retrieve the list of parquet files on each query; function must take one `JSONB` argument and return text array of full paths to parquet files;
* **files_func_arg** - argument for the function, specified by **files_func**.
* **max_open_files** - the limit for the number of Parquet files open simultaneously.

Foreign table may be created for a single Parquet file and for a set of files. It is also possible to specify a user defined function, which would return a list of file paths. Depending on the number of files and table options `parquet_s3_fdw` may use one of the following execution strategies:

| Strategy                | Description              |
|-------------------------|--------------------------|
| **Single File**         | Basic single file reader
| **Multifile**           | Reader which process Parquet files one by one in sequential manner |
| **Multifile Merge**     | Reader which merges presorted Parquet files so that the produced result is also ordered; used when `sorted` option is specified and the query plan implies ordering (e.g. contains `ORDER BY` clause) |
| **Caching Multifile Merge** | Same as `Multifile Merge`, but keeps the number of simultaneously open files limited; used when the number of specified Parquet files exceeds `max_open_files` |

GUC variables:
* **parquet_fdw.use_threads** - global switch that allow user to enable or disable threads (default `true`);
* **parquet_fdw.enable_multifile** - enable Multifile reader (default `true`).
* **parquet_fdw.enable_multifile_merge** - enable Multifile Merge reader (default `true`).

Example:
```sql
CREATE FOREIGN TABLE userdata (
    id           int,
    first_name   text,
    last_name    text
)
SERVER parquet_s3_srv
OPTIONS (
    filename 's3://bucket/dir/userdata1.parquet'
);
```

### Access foreign table
```sql
SELECT * FROM userdata;
```

## Parallel queries
`parquet_s3_fdw` also supports [parallel query execution](https://www.postgresql.org/docs/current/parallel-query.html) (not to confuse with multi-threaded decoding feature of Apache Arrow).

## Import
`parquet_s3_fdw` also supports [`IMPORT FOREIGN SCHEMA`](https://www.postgresql.org/docs/current/sql-importforeignschema.html) command to discover parquet files in the specified directory on filesystem and create foreign tables according to those files. It can be used as follows:

```sql
IMPORT FOREIGN SCHEMA "/path/to/directory"
FROM SERVER parquet_s3_srv
INTO public;
```

It is important that `remote_schema` here is a path to a local filesystem directory and is double quoted.

Another way to import parquet files into foreign tables is to use `import_parquet_s3` or `import_parquet_s3_explicit`:

```sql
CREATE FUNCTION import_parquet_s3(
    tablename   text,
    schemaname  text,
    servername  text,
    userfunc    regproc,
    args        jsonb,
    options     jsonb)

CREATE FUNCTION import_parquet_s3_explicit(
    tablename   text,
    schemaname  text,
    servername  text,
    attnames    text[],
    atttypes    regtype[],
    userfunc    regproc,
    args        jsonb,
    options     jsonb)
```

The only difference between `import_parquet_s3` and `import_parquet_s3_explicit` is that the latter allows to specify a set of attributes (columns) to import. `attnames` and `atttypes` here are the attributes names and attributes types arrays respectively (see the example below).

`userfunc` is a user-defined function. It must take a `jsonb` argument and return a text array of filesystem paths to parquet files to be imported. `args` is user-specified jsonb object that is passed to `userfunc` as its argument. A simple implementation of such function and its usage may look like this:

```sql
CREATE FUNCTION list_parquet_s3_files(args jsonb)
RETURNS text[] AS
$$
BEGIN
    RETURN array_agg(args->>'dir' || '/' || filename)
           FROM pg_ls_dir(args->>'dir') AS files(filename)
           WHERE filename ~~ '%.parquet';
END
$$
LANGUAGE plpgsql;

SELECT import_parquet_s3_explicit(
    'abc',
    'public',
    'parquet_srv',
    array['one', 'three', 'six'],
    array['int8', 'text', 'bool']::regtype[],
    'list_parquet_files',
    '{"dir": "/path/to/directory"}',
    '{"sorted": "one"}'
);
```

## Features
- Support SELECT of parquet file on local file system or Amazon S3.
- Support MinIO access instead of Amazon S3.
- Allow control over whether foreign servers keep connections open after transaction completion. This is controlled by keep_connections and defaults to on.
- Support parquet_s3_fdw function parquet_s3_fdw_get_connections() to report open foreign server connections.

## Schemaless mode
- The feature will enable user to use schemaless feature:
  - No specific foreign foreign schema (column difinition) for each parquet file.
  - The schemaless foreign table has only one jsonb column to represent the data from the parquet file by following rule:
    - Jsonb Key: parquet column name.
    - Jsonb Value: parquet column data.
- By use schemaless mode, there are several benefits:
  - Flexibility over data structure of parquet file: By merging all column data into one jsonb column, a schemaless foreign table can query any parquet file that has all column can be mapped with the postgres type.
  - No pre-defined foreign table schemas (column difinition). The lack of schema means that foreign table will query all column from parquet file — including those that user do not yet use.

### Schemaless mode usage
- Schemaless mode is enabled by `schemaless` option:
  - `schemaless` option is `true`: enable schemaless mode.
  - `schemaless` option is `false`: disable schemaless mode (We call it `non-schemaless` mode).
  - If `schemaless` option is not configured, default value is false.
  - `schemaless` option is supported in `CREATE FOREIGN TABLE`, `IMPORT FOREIGN SCHEMA`, `import_parquet_s3()` and `import_parquet_s3_explicit()`.
- Schemaless foreign table needs at least one jsonb column to represent data:
  - If there is more than 1 jsonb column, only one column is populated, all other columns are treated with NULL value.
  - If there is no jsonb column, all column are treated with NULL value.
  - Example:
    ```sql
    CREATE FOREIGN TABLE example_schemaless (
      id int,
      v jsonb
    ) OPTIONS (filename '/path/to/parquet_file', schemaless 'true');
    SELECT * FROM example_schemaless;
    id |                                                                v                                                                
    ----+---------------------------------------------------------------------------------------------------------------------------------
        | {"one": 1, "six": "t", "two": [1, 2, 3], "five": "2018-01-01", "four": "2018-01-01 00:00:00", "seven": 0.5, "three": "foo"}
        | {"one": 2, "six": "f", "two": [null, 5, 6], "five": "2018-01-02", "four": "2018-01-02 00:00:00", "seven": null, "three": "bar"}
    (2 rows)
    ```
- Create foreign table:
  With  `IMPORT FOREIGN SCHEMA`, `import_parquet_s3()` and `import_parquet_s3_explicit()`, foreign table will create with fixed column difinition like below:
  ```sql
  CREATE FOREIGN TABLE example (
    v jsonb
  ) OPTIONS (filename '/path/to/parquet_file', schemaless 'true');
  ```
- Query data:
  ```sql
  -- non-schemaless mode
  SELECT * FROM example;
   one |    two     | three |        four         |    five    | six | seven 
  -----+------------+-------+---------------------+------------+-----+-------
     1 | {1,2,3}    | foo   | 2018-01-01 00:00:00 | 2018-01-01 | t   |   0.5
     2 | {NULL,5,6} | bar   | 2018-01-02 00:00:00 | 2018-01-02 | f   |      
  (2 rows)
  -- schemaless mode
  SELECT * FROM example_schemaless;
                                                                    v
  ---------------------------------------------------------------------------------------------------------------------------------
   {"one": 1, "six": "t", "two": [1, 2, 3], "five": "2018-01-01", "four": "2018-01-01 00:00:00", "seven": 0.5, "three": "foo"}
   {"one": 2, "six": "f", "two": [null, 5, 6], "five": "2018-01-02", "four": "2018-01-02 00:00:00", "seven": null, "three": "bar"}
  (2 rows)
  ```

- Fetch values in jsonb expression:  
  - Use `->>` jsonb arrow operator which return text type. User may cast type the jsonb expression to get corresponding data representation.  
  - For example, `v->>'col'` expression of fetch value `col` will be column name `col` in parquet file and we call it `schemaless variable` or `slvar`.  
    ```sql
    SELECT v->>'two', sqrt((v->>'one')::int) FROM example_schemaless;
      ?column?   |        sqrt        
    --------------+--------------------
    [1, 2, 3]    |                  1
    [null, 5, 6] | 1.4142135623730951
    (2 rows)
    ```

- Some feature is different with `non-schemaless` mode
  - Rowgroup filter support: in schemaless mode, parquet_s3_fdw can support execute row group filter with some `WHERE` condition below:
    - `slvar::type {operator} const`. For example: `(v->>'int64_col')::int8 = 100`
    - `const {operator} slvar ::type`. For example: `100 = (v->>'int64_col')::int8`
    - `slvar::boolean is true/false`. For example: `(v->>'bool_col')::boolean is false`
    - `!(slvar::boolean)`. For example: `!(v->>'bool_col')::boolean`
    - Jsonb `exist` operator: `((v->>'col')::jsonb) ? element`, `(v->'col') ? element` and `v ? 'col'`
    - The cast function must be mapped with the parquet column type, otherwise, the filter will be skipped.
  - To use presort column of parquet file, user must be:
    - define column name in `sorted` option same as `non-schemaless mode`
    - Use `slvar` instead of column name in the `ORDER BY` clause.
    - If the sorted parquet column is not a text column, please add the explicit cast to the mapped type of this column.
    - For example:
      ```sql
      CREATE FOREIGN TABLE example_sorted (v jsonb)
      SERVER parquet_s3_srv
      OPTIONS (filename '/path/to/example1.parquet /path/to/example2.parquet', sorted 'int64_col', schemaless 'true');
      EXPLAIN (COSTS OFF) SELECT * FROM example_sorted ORDER BY (v->>'int64_col')::int8;
                QUERY PLAN           
      --------------------------------
      Foreign Scan on example_sorted
        Reader: Multifile Merge
        Row groups: 
          example1.parquet: 1, 2
          example2.parquet: 1
      (5 rows)
      ```
  - Support for arrow Nested List and Map: these type will be treated as nested jsonb value which can access by `->` operator.  
  For example:
    ```sql
    SELECT * FROM example_schemaless;
                                      v
    ----------------------------------------------------------------------------
    {"array_col": [19, 20], "jsonb_col": {"1": "foo", "2": "bar", "3": "baz"}}
    {"array_col": [21, 22], "jsonb_col": {"4": "test1", "5": "test2"}}
    (2 rows)

    SELECT v->'array_col'->1, v->'jsonb_col'->'1' FROM example3;
    ?column? | ?column? 
    ----------+----------
    20       | "foo"
    22       | 
    (2 rows)
    ```

  - Postgres cost for caculate `(jsonb->>'col')::type` is much larger than fetch column directly in `non-schemaless` mode, The query plan of `schemaless` mode can be different with `non-schemaless` mode in some complex query.

- For other feature, `schemaless` mode works same as `non-schemaless` mode.

## Limitations
- Modification (INSERT, UPDATE and DELETE) is not supported.
- Transaction is not supported.
- Cannot create a single foreign table using parquet files on both file system and Amazon S3.
- AWS region is hard-coded as "ap-northeast-1". If you want to use another region, you need to modify the source code by changing "AP_NORTHEAST_1" in parquet_s3_fdw_connection.cpp.
- For the query that return record type, parquet s3 fdw only fills data for columns which are refered in target list or clause. For other columns, they are filled as NULL.     
Example:    
    ```sql
    -- column c1 and c3 are refered in ORDER BY clause, so it will be filled with values. For other columns: c2,c4,c5,c6 filled as NULL.
    SELECT t1 FROM tbl t1 ORDER BY tbl.c3, tbl.c1;     
            t1              
    ------------------      
     (101,,00101,,,,)       
     (102,,00102,,,,)       
    (2 rows) 
    ```  

- The 4th and 5th arguments of `import_parquet_s3_explicit()` function are meaningless in `schemaless` mode.
  - These arguments should be defined as `NULL` value.
  - If these arguments is not NULL value the `WARNING` below will occur:
    ```
    WARNING: parquet_s3_fdw: attnames and atttypes are expected to be NULL. They are meaningless for schemaless table.
    HINT: Schemaless table imported always contain "v" column with "jsonb" type.
    ```
- `schemaless` mode does not support create partition table by `CREATE TABLE parent_tbl (v jsonb) PARTITION BY RANGE((v->>'a')::int)`.

## Contributing
Opening issues and pull requests on GitHub are welcome.

## License
Copyright (c) 2021, TOSHIBA Corporation  
Copyright (c) 2018 - 2019, adjust GmbH

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

See the [`LICENSE.md`][4] file for full details.

[1]: https://github.com/adjust/parquet_fdw
[2]: https://aws.amazon.com/s3/
[3]: https://min.io/
[4]: LICENSE.md
