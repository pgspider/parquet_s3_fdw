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
    id      int,
    date    timestamp,
    num     int
)
PARTITION BY range(date);

--Testcase 7:
\set var '\"':PATH_FILENAME'\/data\/partition\/example_part1.parquet"'

--Testcase 8:
CREATE FOREIGN TABLE example_part1
PARTITION OF example_part FOR VALUES FROM ('2018-01-01') TO ('2018-02-01')
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'id date');

--Testcase 9:
\set var '\"':PATH_FILENAME'\/data\/partition\/example_part2.parquet"'

--Testcase 10:
CREATE FOREIGN TABLE example_part2
PARTITION OF example_part FOR VALUES FROM ('2018-02-01') TO ('2018-03-01')
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'id date');

-- Test that "sorted" option works if there is no ORDER BY
--Testcase 11:
EXPLAIN (COSTS OFF) SELECT * FROM example_part WHERE id = 1;
SELECT * FROM example_part WHERE id = 1;

--Testcase 12:
EXPLAIN (COSTS OFF) SELECT * FROM example_part WHERE date = '2018-01-01';
SELECT * FROM example_part WHERE date = '2018-01-01';

-- Test that "sorted" option works together with ORDER BY
--Testcase 13:
EXPLAIN (COSTS OFF) SELECT * FROM example_part WHERE id = 1 ORDER BY date;
SELECT * FROM example_part WHERE id = 1 ORDER BY date;

--Testcase 14:
EXPLAIN (COSTS OFF) SELECT * FROM example_part WHERE date = '2018-01-01' ORDER BY id;
SELECT * FROM example_part WHERE date = '2018-01-01' ORDER BY id;

--Testcase 15:
DROP TABLE example_part CASCADE;

--Testcase 16:
DROP EXTENSION parquet_s3_fdw CASCADE;
