# Parquet S3 Foreign Data Wrapper for PostgreSQL

This PostgreSQL extension is a Foreign Data Wrapper (FDW) for accessing Parquet file on local file system and [Amazon S3][2].
This version of parquet_s3_fdw can work for PostgreSQL 13.

Parquet foreign data wrapper supporting S3 access for PostgreSQL.

This code is based on [`parquet_fdw`][1]created by adjust GmbH.

## Installation
### 1. Install dependent libraries
`parquet_s3_fdw` requires `libarrow` and `libparquet` installed in your system (requires version 0.15, for previous versions use branch [arrow-0.14](https://github.com/adjust/parquet_fdw/tree/arrow-0.14)). Please refer to [building guide](https://github.com/apache/arrow/blob/master/cpp/README.md).

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

| Parquet type |  SQL type |
|--------------|-----------|
|        INT32 |      INT4 |
|        INT64 |      INT8 |
|        FLOAT |    FLOAT4 |
|       DOUBLE |    FLOAT8 |
|    TIMESTAMP | TIMESTAMP |
|       DATE32 |      DATE |
|       STRING |      TEXT |
|       BINARY |     BYTEA |
|         LIST |     ARRAY |

Currently `parquet_s3_fdw` doesn't support structs and nested lists.

Following options are supported:
* **filename** - space separated list of paths to Parquet files to read. You can specify the path on AWS S3 by starting with `s3://`. The mix of local path and S3 path is not supported;
* **dirname** - path to directory having Parquet files to read;
* **sorted** - space separated list of columns that Parquet files are presorted by; that would help postgres to avoid redundant sorting when running query with `ORDER BY` clause or in other cases when having a presorted set is beneficial (Group Aggregate, Merge Join);
* **use_mmap** - whether memory map operations will be used instead of file read operations (default `false`);
* **use_threads** - enables `arrow`'s parallel columns decoding/decompression (default `false`).

GUC variables:
* **parquet_fdw.use_threads** - global switch that allow user to enable or disable threads (default `true`).

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
`parquet_s3_fdw` also supports [parallel query execution](https://www.postgresql.org/docs/current/parallel-query.html) (not to confuse with multi-threaded decoding feature of `arrow`). It is disabled by default; to enable it run `ANALYZE` command on the table. The reason behind this is that without statistics postgres may end up choosing a terrible parallel plan for certain queries which would be much worse than a serial one (e.g. grouping by a column with large number of distinct values).

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

## Limitations
- Modification (INSERT, UPDATE and DELETE) is not supported.
- Transaction is not supported.
- Cannot create a single foreign table using parquet files on both file system and Amazon S3.
- AWS region is hard-coded as "ap-northeast-1". If you want to use another region, you need to modify the source code by changing "AP_NORTHEAST_1" in parquet_s3_fdw_connection.cpp.

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
