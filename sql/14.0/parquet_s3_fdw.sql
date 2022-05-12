SET datestyle = 'ISO';
SET client_min_messages = WARNING;
SET log_statement TO 'none';
--Testcase 1:
CREATE EXTENSION parquet_s3_fdw;
--Testcase 2:
DROP ROLE IF EXISTS regress_parquet_s3_fdw;
--Testcase 3:
CREATE ROLE regress_parquet_s3_fdw LOGIN SUPERUSER;

SET ROLE regress_parquet_s3_fdw;
--Testcase 4:
CREATE SERVER parquet_s3_srv FOREIGN DATA WRAPPER parquet_s3_fdw :USE_MINIO;
--Testcase 5:
CREATE USER MAPPING FOR regress_parquet_s3_fdw SERVER parquet_s3_srv :USER_PASSWORD;

SET ROLE regress_parquet_s3_fdw;
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 6:
CREATE FOREIGN TABLE example1 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one');

--Testcase 7:
SELECT * FROM example1;

-- no explicit columns mentions
--Testcase 8:
SELECT 1 as x FROM example1;
--Testcase 9:
SELECT count(*) as count FROM example1;

-- sorting
--Testcase 10:
EXPLAIN (COSTS OFF) SELECT * FROM example1 ORDER BY one;
--Testcase 11:
EXPLAIN (COSTS OFF) SELECT * FROM example1 ORDER BY three;

-- filtering
SET client_min_messages = DEBUG1;
--Testcase 12:
SELECT * FROM example1 WHERE one < 1;
--Testcase 13:
SELECT * FROM example1 WHERE one <= 1;
--Testcase 14:
SELECT * FROM example1 WHERE one > 6;
--Testcase 15:
SELECT * FROM example1 WHERE one >= 6;
--Testcase 16:
SELECT * FROM example1 WHERE one = 2;
--Testcase 17:
SELECT * FROM example1 WHERE one = 7;
--Testcase 18:
SELECT * FROM example1 WHERE six = true;
--Testcase 19:
SELECT * FROM example1 WHERE six = false;
--Testcase 20:
SELECT * FROM example1 WHERE seven < 1.5;
--Testcase 21:
SELECT * FROM example1 WHERE seven <= 1.5;
--Testcase 22:
SELECT * FROM example1 WHERE seven = 1.5;
--Testcase 23:
SELECT * FROM example1 WHERE seven > 1;
--Testcase 24:
SELECT * FROM example1 WHERE seven >= 1;
--Testcase 25:
SELECT * FROM example1 WHERE seven IS NULL;

-- prepared statements
--Testcase 26:
prepare prep(date) as select * from example1 where five < $1;
--Testcase 27:
execute prep('2018-01-03');
--Testcase 28:
execute prep('2018-01-01');

-- invalid options
SET client_min_messages = WARNING;
--Testcase 29:
CREATE FOREIGN TABLE example_fail (one INT8, two INT8[], three TEXT)
SERVER parquet_s3_srv;
--Testcase 30:
CREATE FOREIGN TABLE example_fail (one INT8, two INT8[], three TEXT)
SERVER parquet_s3_srv
OPTIONS (filename 'nonexistent.parquet', some_option '123');
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 31:
CREATE FOREIGN TABLE example_fail (one INT8, two INT8[], three TEXT)
SERVER parquet_s3_srv
OPTIONS (filename :'var', some_option '123');

-- type mismatch
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 32:
CREATE FOREIGN TABLE example_fail (one INT8[], two INT8, three TEXT)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one');
--Testcase 33:
SELECT one FROM example_fail;
--Testcase 34:
SELECT two FROM example_fail;

-- files_func
--Testcase 35:
CREATE FUNCTION list_parquet_s3_files(args JSONB)
RETURNS TEXT[] AS
$$
    SELECT ARRAY[args->>'dir' || '/example1.parquet', args->>'dir' || '/example2.parquet']::TEXT[];
$$
LANGUAGE SQL;
\set var '{"dir": "':PATH_FILENAME'/data/simple"}'
--Testcase 36:
CREATE FOREIGN TABLE example_func (one INT8, two INT8[], three TEXT)
SERVER parquet_s3_srv
OPTIONS (
    files_func 'list_parquet_s3_files',
    files_func_arg :'var',
    sorted 'one');
--Testcase 37:
SELECT * FROM example_func;

-- invalid files_func options
--Testcase 38:
CREATE FUNCTION int_array_func(args JSONB)
RETURNS INT[] AS
$$ SELECT ARRAY[1,2,3]::INT[]; $$
LANGUAGE SQL;
--Testcase 39:
CREATE FUNCTION no_args_func()
RETURNS TEXT[] AS
$$ SELECT ARRAY['s3://data/simple/example1.parquet']::TEXT[]; $$
LANGUAGE SQL;
--Testcase 40:
CREATE FOREIGN TABLE example_inv_func (one INT8, two INT8[], three TEXT)
SERVER parquet_s3_srv
OPTIONS (files_func 'int_array_func');
--Testcase 41:
CREATE FOREIGN TABLE example_inv_func (one INT8, two INT8[], three TEXT)
SERVER parquet_s3_srv
OPTIONS (files_func 'no_args_func');
--Testcase 42:
CREATE FOREIGN TABLE example_inv_func (one INT8, two INT8[], three TEXT)
SERVER parquet_s3_srv
OPTIONS (files_func 'list_parquet_s3_files', files_func_arg 'invalid json');
--Testcase 43:
DROP FUNCTION list_parquet_s3_files(JSONB);
--Testcase 44:
DROP FUNCTION int_array_func(JSONB);
--Testcase 45:
DROP FUNCTION no_args_func();

-- sequential multifile reader
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/simple/example2.parquet'
--Testcase 46:
CREATE FOREIGN TABLE example_seq (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_srv
OPTIONS (filename :'var');
--Testcase 47:
EXPLAIN (COSTS OFF) SELECT * FROM example_seq;
--Testcase 48:
SELECT * FROM example_seq;

-- multifile merge reader
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/simple/example2.parquet'
--Testcase 49:
CREATE FOREIGN TABLE example_sorted (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one');
--Testcase 50:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted ORDER BY one;
--Testcase 51:
SELECT * FROM example_sorted ORDER BY one;

-- caching multifile merge reader
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/simple/example2.parquet'
--Testcase 52:
CREATE FOREIGN TABLE example_sorted_caching (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', max_open_files '1');
--Testcase 53:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted_caching ORDER BY one;
--Testcase 54:
SELECT * FROM example_sorted_caching ORDER BY one;

-- parallel execution
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0.001;
EXPLAIN (COSTS OFF) SELECT * FROM example_seq;
--Testcase 56:
EXPLAIN (COSTS OFF) SELECT * FROM example_seq ORDER BY one;
--Testcase 57:
EXPLAIN (COSTS OFF) SELECT * FROM example_seq ORDER BY two;
--Testcase 58:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted;
--Testcase 59:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted ORDER BY one;
--Testcase 60:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted ORDER BY two;
ALTER FOREIGN TABLE example_sorted OPTIONS (ADD files_in_order 'true');
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted ORDER BY one;
--Testcase 61:
EXPLAIN (COSTS OFF) SELECT * FROM example1;
--Testcase 62:
SELECT SUM(one) FROM example1;

-- multiple sorting keys
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 63:
CREATE FOREIGN TABLE example_multisort (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one five');
--Testcase 64:
EXPLAIN (COSTS OFF) SELECT * FROM example_multisort ORDER BY one, five;
--Testcase 65:
SELECT * FROM example_multisort ORDER BY one, five;

-- maps
\set var :PATH_FILENAME'/data/complex/example3.parquet'
SET client_min_messages = DEBUG1;
--Testcase 66:
CREATE FOREIGN TABLE example3 (
    one     JSONB,
    two     JSONB,
    three   INT2)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one');

--Testcase 67:
SELECT * FROM example3;
--Testcase 68:
SELECT * FROM example3 WHERE three = 3;

-- analyze
ANALYZE example_sorted;

SET client_min_messages = WARNING;

--get version
--Testcase 69:
\df parquet_s3*
--Testcase 70:
SELECT * FROM public.parquet_s3_fdw_version();
--Testcase 71:
SELECT parquet_s3_fdw_version();

--Testcase 72:
DROP EXTENSION parquet_s3_fdw CASCADE;
