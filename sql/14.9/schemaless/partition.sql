SET datestyle = 'ISO';
SET client_min_messages = WARNING;
SET log_statement TO 'none';
--Testcase 1:
CREATE EXTENSION parquet_s3_fdw;
--Testcase 2:
DROP ROLE IF EXISTS regress_parquet_s3_fdw;
--Testcase 3:
CREATE ROLE regress_parquet_s3_fdw LOGIN SUPERUSER;
--Testcase 4:
CREATE SERVER parquet_s3_srv FOREIGN DATA WRAPPER parquet_s3_fdw :USE_MINIO;
--Testcase 5:
CREATE USER MAPPING FOR regress_parquet_s3_fdw SERVER parquet_s3_srv :USER_PASSWORD;
SET ROLE regress_parquet_s3_fdw;

--Testcase 6:
CREATE TABLE example_part (
    v jsonb
)
PARTITION BY range((v->'date'));

--Testcase 7:
\set var '\"':PATH_FILENAME'\/data\/partition\/example_part1.parquet"'

--Testcase 8:
CREATE FOREIGN TABLE example_part1
PARTITION OF example_part FOR VALUES FROM ('"2018-01-01"') TO ('"2018-02-01"')
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'id date', schemaless 'true');

--Testcase 9:
\set var '\"':PATH_FILENAME'\/data\/partition\/example_part2.parquet"'

--Testcase 10:
CREATE FOREIGN TABLE example_part2
PARTITION OF example_part FOR VALUES FROM ('"2018-02-01"') TO ('"2018-03-01"')
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'id date', schemaless 'true');

-- Test that "sorted" option works if there is no ORDER BY
--Testcase 11:
EXPLAIN (COSTS OFF) SELECT * FROM example_part WHERE (v->>'id')::int = 1;
SELECT * FROM example_part WHERE (v->>'id')::int = 1;

--Testcase 12:
EXPLAIN (COSTS OFF) SELECT * FROM example_part WHERE (v->>'date')::timestamp = '2018-01-01';
SELECT * FROM example_part WHERE (v->>'date')::timestamp = '2018-01-01';

-- Test that "sorted" option works together with ORDER BY
--Testcase 13:
EXPLAIN (COSTS OFF) SELECT * FROM example_part WHERE (v->>'id')::int8 = 1 ORDER BY (v->>'date')::timestamp;
SELECT * FROM example_part WHERE (v->>'id')::int8 = 1 ORDER BY (v->>'date')::timestamp;

--Testcase 14:
-- The jsonb partition key is implicit text type. In schemaless mode, partition table cannot filter date value correct.
-- then it will scan both child partition tables.
EXPLAIN (COSTS OFF) SELECT * FROM example_part WHERE (v->>'date')::timestamp = '2018-01-01' ORDER BY (v->>'id')::int8;
SELECT * FROM example_part WHERE (v->>'date')::timestamp = '2018-01-01' ORDER BY (v->>'id')::int8;

--Testcase 15:
DROP TABLE example_part CASCADE;

--Testcase 16:
DROP EXTENSION parquet_s3_fdw CASCADE;
