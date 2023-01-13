# Parquet S3 Foreign Data Wrapper for PostgreSQL

This PostgreSQL extension is a Foreign Data Wrapper (FDW) for accessing Parquet file on local file system and [Amazon S3][2].
This version of parquet_s3_fdw can work for PostgreSQL 13, 14 and 15.

Read-only Apache Parquet foreign data wrapper supporting S3 access for PostgreSQL.


## Installation
### 1. Install dependent libraries
`parquet_s3_fdw` requires `libarrow` and `libparquet` installed in your system (requires version 0.15+, for previous versions use branch [arrow-0.14](https://github.com/adjust/parquet_fdw/tree/arrow-0.14)). Please refer to [building guide](https://github.com/apache/arrow/blob/master/docs/source/developers/cpp/building.rst).

`AWS SDK for C++ (libaws-cpp-sdk-core libaws-cpp-sdk-s3)` is also required (Confirmed version is 1.9.263).

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
* **region** - the value of AWS region used to connect to (default `ap-northeast-1`).
* **endpoint** - the address and port used to connect to (default `127.0.0.1:9000`).

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
- Support INSERT, DELETE, UPDATE (Foreign modification).
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

## Write-able FDW
The user can issue an insert, update and delete statement for the foreign table, which has set the key columns.

### Key columns
- in non-schemaless mode: The key columns can be set while creating a parquet_s3_fdw foreign table object with OPTIONS(key 'true'):
```sql
CREATE FOREIGN TABLE userdata (
    id1          int OPTIONS(key 'true'),
    id2          int OPTIONS(key 'true'),
    first_name   text,
    last_name    text
) SERVER parquet_s3_srv
OPTIONS (
    filename 's3://bucket/dir/userdata1.parquet'
);
```
- in schemaless mode The key columns can be set while creating a parquet_s3_fdw foreign table object with `key_columns` option:
```sql
CREATE FOREIGN TABLE userdata (
    v JSONB
) SERVER parquet_s3_srv
OPTIONS (
    filename 's3://bucket/dir/userdata1.parquet',
    schemaless 'true',
    key_columns 'id1 id2'
);
```
- `key_columns` option can be use in IMPORT FOREIGN SCHEMA feature:
```sql
-- in schemaless mode
IMPORT FOREIGN SCHEMA 's3://data/' FROM SERVER parquet_s3_srv INTO tmp_schema
OPTIONS (sorted 'c1', schemaless 'true', key_columns 'id1 id2');
-- corresponding CREATE FOREIGN TABLE
CREATE FOREIGN TABLE tbl1 (
      v jsonb
) SERVER parquet_s3_srv
OPTIONS (filename 's3://data/tbl1.parquet', sorted 'c1', schemaless 'true', key_columns 'id1 id2');

-- in non-schemaless mode
IMPORT FOREIGN SCHEMA 's3://data/' FROM SERVER parquet_s3_srv INTO tmp_schema
OPTIONS (sorted 'c1', schemaless 'true', key_columns 'id1 id2');
-- corresponding CREATE FOREIGN TABLE
CREATE FOREIGN TABLE tbl1 (
      id1 INT OPTIONS (key 'true'),
      id2 INT OPTIONS (key 'true'),
      c1  TEXT,
      c2  FLOAT
) SERVER parquet_s3_srv
OPTIONS (filename 's3://data/tbl1.parquet', sorted 'c1');
```
### insert_file_selector option
User defined function signature that is used by parquet_s3_fdw to retrieve the target parquet file on INSERT query:
```sql
CREATE FUNCTION insert_file_selector_func(one INT8, dirname text)
RETURNS TEXT AS
$$
    SELECT (dirname || '/example7.parquet')::TEXT;
$$
LANGUAGE SQL;

CREATE FOREIGN TABLE example_func (one INT8 OPTIONS (key 'true'), two TEXT)
SERVER parquet_s3_srv
OPTIONS (
    insert_file_selector 'insert_file_selector_func(one, dirname)',
    dirname '/tmp/data_local/data/test',
    sorted 'one');
```
- insert_file_selector function signature spec:
  - Syntax: `[function name]([arg name] , [arg name] ...)`
  - Default return type is `TEXT` (full paths to parquet file)
  - `[arg name]`: must be foreign table column name or `dirname`
  - args value:
    - `dirname` arg: value of dirname option.
    - `column` args: get from inserted slot by name.

### Sorted columns:
parquet_s3_fdw supports keeping the sorted column still sorted in the modify feature.

### Parquet file schema:
Basically, the parquet file schema is defined according to a list of column names and corresponding types, but in parquet_s3_fdw's scan, it assumes that all columns with the same name have the same type. So, in modify feature, this assumption will be use also.

### Type mapping from postgres to arrow type:
- primitive type mapping:
  |        SQL type        |   Arrow type |
  |-----------------------:|-------------:|
  |                   BOOL |         BOOL |
  |                   INT2 |        INT16 |
  |                   INT4 |        INT32 |
  |                   INT8 |        INT64 |
  |                 FLOAT4 |        FLOAT |
  |                 FLOAT8 |       DOUBLE |
  |  TIMESTAMP/TIMESTAMPTZ |    TIMESTAMP |
  |                   DATE |       DATE32 |
  |                   TEXT |       STRING |
  |                  BYTEA |       BINARY |
- Default time precision for arrow::TIMESTAMP is microsecond an in UTC timezone.
- LIST are created by its element type, just support primitive type for element.
- MAP are created by its jsonb element type:
  |  jsonb type   |   Arrow type |
  |--------------:|-------------:|
  |          text |       STRING |
  |       numeric |       FLOAT8 |
  |       boolean |         BOOL |
  |          null |       STRING |
  |   other types |       STRING |

- In schemaless mode:
  - The mapping for primitive jsonb type is same as MAP in non-schemaless mode.
  - For first nested jsonb in schemaless mode:
    |  jsonb type   |   Arrow type |
    |--------------:|-------------:|
    |         array |         LIST |
    |        object |          MAP |
  - Element type of LIST and MAP is same as MAP type in non-schemaless mode.

### INSERT
```sql
-- non-schemaless mode
CREATE FOREIGN TABLE example_insert (
    c1 INT2 OPTIONS (key 'true'),
    c2 TEXT,
    c3 BOOLEAN
) SERVER parquet_s3_srv OPTIONS (filename 's3://data/example_insert.parquet');

INSERT INTO example_insert VALUES (1, 'text1', true), (2, DEFAULT, false), ((select 3), (select i from (values('values are fun!')) as foo (i)), true);
INSERT 0 3

SELECT * FROM example_insert;
 c1 |       c2        | c3 
----+-----------------+----
  1 | text1           | t
  2 |                 | f
  3 | values are fun! | t
(3 rows)

-- schemaless mode
CREATE FOREIGN TABLE example_insert_schemaless (
    v JSONB
) SERVER parquet_s3_srv OPTIONS (filename 's3://data/example_insert.parquet', schemaless 'true', key_column 'c1');

INSERT INTO example_insert_schemaless VALUES ('{"c1": 1, "c2": "text1", "c3": true}'), ('{"c1": 2, "c2": null, "c3": false}'), ('{"c1": 3, "c2": "values are fun!", "c3": true}');

SELECT * FROM example_insert_schemaless;
                       v                       
-----------------------------------------------
 {"c1": 1, "c2": "text1", "c3": "t"}
 {"c1": 2, "c2": null, "c3": "f"}
 {"c1": 3, "c2": "values are fun!", "c3": "t"}
(3 rows)

```
- Select file to insert:
  - In case, option `insert_file_selector` exists, target file is the result of this function.
    - If target file does not exist, create new file with the same name of target file.
    - If target file exists, but its schema does not match with list columns of insert record, an error message will be raised.
  - In case, option `insert_file_selector` does not exist:
    - target file is the first file whose schema matches the inserted record (all columns of inserted record exist in  the target file).
    - If no file that meets its schema matches the columns of insert record and `dirname` option has specified. Creating new file with name format:
    ``` [foreign_table_name]_[date_time].parquet ```
    - Otherwise, an error message will be raised.
- The new file schema:
  - In non-schemaless mode, the new file will have all columns existed in foreign table.
  - In schemaless mode, the new file will have all column specify in jsonb value.
  - Column information:
    - Get from existed file list.
    - If column does not exist in any file: create bases on [pre-defined mapping type](#type-mapping-from-postgres-to-arrow-type).
### UPDATE/DELETE
```sql
-- non-schemaless mode
CREATE FOREIGN TABLE example (
    c1 INT2 OPTIONS (key 'true'),
    c2 TEXT,
    c3 BOOLEAN
) SERVER parquet_s3_srv OPTIONS (filename 's3://data/example.parquet');

SELECT * FROM example;
 c1 |       c2        | c3 
----+-----------------+----
  1 | text1           | t
  2 |                 | f
  3 | values are fun! | t
(3 rows)

UPDATE example SET c3 = false WHERE c2 = 'text1';
UPDATE 1

SELECT * FROM example;
 c1 |       c2        | c3 
----+-----------------+----
  1 | text1           | f
  2 |                 | f
  3 | values are fun! | t
(3 rows)

DELETE FROM example WHERE c1 = 2;
DELETE 1

SELECT * FROM example;
 c1 |       c2        | c3 
----+-----------------+----
  1 | text1           | f
  3 | values are fun! | t
(2 rows)

-- schemaless mode
CREATE FOREIGN TABLE example_schemaless (
    v JSONB
) SERVER parquet_s3_srv OPTIONS (filename 's3://data/example.parquet', schemaless 'true', key_columns 'c1');

SELECT * FROM example_schemaless;
                       v                       
-----------------------------------------------
 {"c1": 1, "c2": "text1", "c3": "t"}
 {"c1": 2, "c2": null, "c3": "f"}
 {"c1": 3, "c2": "values are fun!", "c3": "t"}
(3 rows)

UPDATE example_schemaless SET v='{"c3":false}' WHERE v->>'c2' = 'text1';
UPDATE 1

SELECT * FROM example_schemaless;
                       v                       
-----------------------------------------------
 {"c1": 1, "c2": "text1", "c3": "f"}
 {"c1": 2, "c2": null, "c3": "f"}
 {"c1": 3, "c2": "values are fun!", "c3": "t"}
(3 rows)

DELETE FROM example_schemaless WHERE (v->>'c1')::int = 2;
DELETE 1

SELECT * FROM example_schemaless;
                       v                       
-----------------------------------------------
 {"c1": 1, "c2": "text1", "c3": "f"}
 {"c1": 3, "c2": "values are fun!", "c3": "t"}
(2 rows)
```
## Limitations
- Transaction is not supported.
- Cannot create a single foreign table using parquet files on both file system and Amazon S3.
- The 4th and 5th arguments of `import_parquet_s3_explicit()` function are meaningless in `schemaless` mode.
  - These arguments should be defined as `NULL` value.
  - If these arguments is not NULL value the `WARNING` below will occur:
    ```
    WARNING: parquet_s3_fdw: attnames and atttypes are expected to be NULL. They are meaningless for schemaless table.
    HINT: Schemaless table imported always contain "v" column with "jsonb" type.
    ```
- `schemaless` mode does not support create partition table by `CREATE TABLE parent_tbl (v jsonb) PARTITION BY RANGE((v->>'a')::int)`.
- In modifying features:
  - `parquet_s3_fdw` modifies the parquet file by creating a modifiable cache data from the target parquet file and overwriting the old one:
    - Performance won't be good for large files.
    - When exact same file is modifying concurrently, the result would be inconsistent.
  - WITH CHECK OPTION, ON CONFLICT and RETURNING are not supported.
  - `sorted` columns only supports the following types: `int2`, `int4`, `int8`, `date`, `timestamp`, `float4`, `float8`.
  - `key` columns only supports the following types: `int2`, `int4`, `int8`, `date`, `timestamp`, `float4`, `float8` and `text`.
  - `key` columns values must be unique, `parquet_s3_fdw` does not support checking for unique values for key columns, user must do that.
  - `key` columns only required for UPDATE/DELETE.

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
