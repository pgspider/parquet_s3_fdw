--Testcase 1:
SET datestyle = 'ISO';
--Testcase 2:
SET client_min_messages = WARNING;
--Testcase 3:
SET log_statement TO 'none';
--Testcase 4:
CREATE EXTENSION parquet_s3_fdw;
--Testcase 5:
DROP ROLE IF EXISTS regress_parquet_s3_fdw;
--Testcase 6:
CREATE ROLE regress_parquet_s3_fdw LOGIN SUPERUSER;
--Testcase 7:
SET ROLE regress_parquet_s3_fdw;
--Testcase 8:
CREATE SERVER parquet_s3_srv FOREIGN DATA WRAPPER parquet_s3_fdw :USE_MINIO;
--Testcase 9:
CREATE USER MAPPING FOR regress_parquet_s3_fdw SERVER parquet_s3_srv :USER_PASSWORD;

-- ======================================================
-- singlefile reader
-- ======================================================
\set var :PATH_FILENAME'/data/simple/example1.parquet'

--
-- test option schemaless 'false'
--
--Testcase 10:
CREATE FOREIGN TABLE example1 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'false');

--Testcase 11:
EXPLAIN VERBOSE
SELECT * FROM example1;
--Testcase 12:
SELECT * FROM example1;

--
-- test option schemaless 'f'
--
--Testcase 13:
ALTER FOREIGN TABLE example1 OPTIONS (SET schemaless 'f');

--
-- test option schemaless not a bool value
--
--Testcase 14:
ALTER FOREIGN TABLE example1 OPTIONS (SET schemaless 'not_a_boolean');

--
-- test option schemaless 'true'
--
--Testcase 15:
ALTER FOREIGN TABLE example1 OPTIONS (SET schemaless 'true');
-- all column will be null if there has no jsonb column
--Testcase 16:
EXPLAIN VERBOSE
SELECT * FROM example1; -- null result
--Testcase 17:
SELECT * FROM example1; -- null result

--
-- test option schemaless 'true'
--
--Testcase 18:
ALTER FOREIGN TABLE example1 OPTIONS (SET schemaless 't');
--Testcase 19:
DROP FOREIGN TABLE example1;

--
-- wrong column specification
--
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 20:
CREATE FOREIGN TABLE example1 (
    cx int,
    v jsonb,
    cy text)
SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'true');

-- all column except jsonb column will be null.
--Testcase 21:
EXPLAIN VERBOSE
SELECT * FROM example1;
--Testcase 22:
SELECT * FROM example1;
--Testcase 23:
ALTER FOREIGN TABLE example1 DROP COLUMN cx;
--Testcase 24:
ALTER FOREIGN TABLE example1 DROP COLUMN cy;
--Testcase 25:
EXPLAIN VERBOSE
SELECT * FROM example1;
--Testcase 26:
SELECT * FROM example1;

--
-- change schemaless column name
--
--Testcase 27:
ALTER FOREIGN TABLE example1 RENAME v TO schemaless_col;
--Testcase 28:
EXPLAIN VERBOSE
SELECT * FROM example1;
--Testcase 29:
SELECT * FROM example1;
--Testcase 30:
DROP FOREIGN TABLE example1;

--
-- test for nested arrow operator
--
\set var :PATH_FILENAME'/data/complex/example3.parquet'

--Testcase 31:
CREATE FOREIGN TABLE example3 (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'true');
--Testcase 32:
EXPLAIN VERBOSE
SELECT v->'one'->'1', v->'two'->'2018-01-01' FROM example3;
--Testcase 33:
SELECT v->'one'->'1', v->'two'->'2018-01-01' FROM example3;
--Testcase 34:
SET client_min_messages = DEBUG1;

--
-- `exist` operator for nested jsonb
--
--Testcase 35:
EXPLAIN VERBOSE
SELECT v->'one' FROM example3 WHERE v->'one'?'1';
--Testcase 36:
SELECT v->'one' FROM example3 WHERE v->'one'?'1';
--Testcase 37:
EXPLAIN VERBOSE
SELECT v->'one' FROM example3 WHERE ((v->>'one')::jsonb)?'1';
--Testcase 38:
SELECT v->'one' FROM example3 WHERE ((v->>'one')::jsonb)?'1';
--Testcase 39:
EXPLAIN VERBOSE
SELECT v->'one' FROM example3 WHERE v->'one'?'10';
--Testcase 40:
SELECT v->'one' FROM example3 WHERE v->'one'?'10';
--Testcase 41:
EXPLAIN VERBOSE
SELECT v->'one' FROM example3 WHERE ((v->>'one')::jsonb)?'10';
--Testcase 42:
SELECT v->'one' FROM example3 WHERE ((v->>'one')::jsonb)?'10';
--Testcase 43:
DROP FOREIGN TABLE example3;

--
-- `exist` operator for schemaless column
--
-- parquet files not has same column list
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/complex/example4.parquet'
--Testcase 44:
CREATE FOREIGN TABLE example_multi (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'true');

--Testcase 45:
EXPLAIN VERBOSE
SELECT * FROM example_multi WHERE v?'jsonb_col';
--Testcase 46:
SELECT * FROM example_multi WHERE v?'jsonb_col';
--Testcase 47:
DROP FOREIGN TABLE example_multi;
--Testcase 48:
SET client_min_messages = WARNING;

--
-- test for nested jsonb array
--
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 49:
CREATE FOREIGN TABLE example1 (
    v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'true');

--Testcase 50:
EXPLAIN VERBOSE
SELECT v->'two'->1 FROM example1;
--Testcase 51:
SELECT v->'two'->1 FROM example1;
--Testcase 52:
DROP FOREIGN TABLE example1;

--
-- the actual column is not existed
--
--Testcase 53:
CREATE FOREIGN TABLE example1 (
    v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', schemaless 'true');

--Testcase 54:
EXPLAIN VERBOSE
SELECT v->>'not_a_column' FROM example1;
--Testcase 55:
SELECT v->>'not_a_column' FROM example1;
--Testcase 56:
DROP FOREIGN TABLE example1;

--
-- target parquet file content is changed
--
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 57:
CREATE FOREIGN TABLE example1 (
    v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'true');

--Testcase 58:
EXPLAIN VERBOSE
SELECT * FROM example1;
--Testcase 59:
SELECT * FROM example1;

\set var :PATH_FILENAME'/data/simple/example2.parquet'
--Testcase 60:
ALTER FOREIGN TABLE example1 OPTIONS (SET filename :'var'); -- change parquet file
--Testcase 61:
EXPLAIN VERBOSE
SELECT * FROM example1;
--Testcase 62:
SELECT * FROM example1;

-- delete first row of parquet file
--Testcase 63:
CREATE TABLE example1_temp (v JSONB);
--Testcase 64:
INSERT INTO example1_temp SELECT * FROM example1;
--Testcase 65:
ALTER FOREIGN TABLE example1 OPTIONS (key_columns 'three');
--Testcase 66:
DELETE FROM example1 WHERE (v->'one')::int = 1;

--Testcase 67:
EXPLAIN VERBOSE
SELECT * FROM example1;
--Testcase 68:
SELECT * FROM example1;
-- revert the change
--Testcase 69:
DELETE FROM example1;
--Testcase 128:
INSERT INTO example1 SELECT * FROM example1_temp;
--Testcase 129:
DROP TABLE example1_temp;

--Testcase 70:
EXPLAIN VERBOSE
SELECT * FROM example1;
--Testcase 71:
SELECT * FROM example1;

-- ======================================================
-- multifile reader
-- ======================================================
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/simple/example2.parquet'
-- test option schemaless 'false'
--Testcase 73:
CREATE FOREIGN TABLE example_multi (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'false');

--Testcase 74:
EXPLAIN VERBOSE
SELECT * FROM example_multi;
--Testcase 75:
SELECT * FROM example_multi;
--Testcase 76:
ALTER FOREIGN TABLE example_multi OPTIONS (SET schemaless 'true');
--Testcase 77:
EXPLAIN VERBOSE
SELECT * FROM example_multi;
--Testcase 78:
SELECT * FROM example_multi;
--Testcase 79:
DROP FOREIGN TABLE example_multi;

--Testcase 80:
CREATE FOREIGN TABLE example_multi (cx int, v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'true');

--Testcase 81:
EXPLAIN VERBOSE
SELECT * FROM example_multi; -- cx column is null
--Testcase 82:
SELECT * FROM example_multi; -- cx column is null
--Testcase 83:
ALTER FOREIGN TABLE example_multi DROP COLUMN cx;
--Testcase 84:
EXPLAIN VERBOSE
SELECT * FROM example_multi;
--Testcase 85:
SELECT * FROM example_multi;

--Testcase 86:
DROP FOREIGN TABLE example_multi;

-- parquet files not has same column list
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/complex/example4.parquet'
--Testcase 87:
CREATE FOREIGN TABLE example_multi (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'true');

--Testcase 88:
EXPLAIN VERBOSE
SELECT * FROM example_multi;
--Testcase 89:
SELECT * FROM example_multi;

--Testcase 90:
DROP FOREIGN TABLE example_multi;

-- ======================================================
-- multifile merge reader
-- ======================================================
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/simple/example2.parquet'

-- test option schemaless 'false'
--Testcase 91:
CREATE FOREIGN TABLE example_sorted (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', schemaless 'false');

--Testcase 92:
EXPLAIN VERBOSE
SELECT * FROM example_sorted ORDER BY one;
--Testcase 93:
SELECT * FROM example_sorted ORDER BY one;

--Testcase 94:
ALTER FOREIGN TABLE example_sorted OPTIONS (SET schemaless 'true');

-- in schemaless mode multifile merge reader can not work without jsonb column.
--Testcase 95:
EXPLAIN VERBOSE
SELECT * FROM example_sorted ORDER BY one;
--Testcase 96:
SELECT * FROM example_sorted ORDER BY one;

--Testcase 97:
DROP FOREIGN TABLE example_sorted;

--Testcase 98:
CREATE FOREIGN TABLE example_sorted (v1 jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', schemaless 'true');

--Testcase 99:
EXPLAIN VERBOSE
SELECT * FROM example_sorted ORDER BY (v1->>'one')::int8;
--Testcase 100:
SELECT * FROM example_sorted ORDER BY (v1->>'one')::int8;

-- sorted column is not existed
--Testcase 101:
ALTER FOREIGN TABLE example_sorted OPTIONS (SET sorted 'not_a_col');
--Testcase 102:
EXPLAIN VERBOSE
SELECT * FROM example_sorted ORDER BY (v1->>'not_a_col')::int8; -- should false
--Testcase 103:
SELECT * FROM example_sorted ORDER BY (v1->>'not_a_col')::int8; -- should false

-- sorted column is not existed in one file
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/complex/example4.parquet'
--Testcase 104:
ALTER FOREIGN TABLE example_sorted OPTIONS (SET sorted 'one');
--Testcase 105:
ALTER FOREIGN TABLE example_sorted OPTIONS (SET filename :'var');

--Testcase 106:
EXPLAIN VERBOSE
SELECT * FROM example_sorted ORDER BY (v1->>'one')::int8;
--Testcase 107:
SELECT * FROM example_sorted ORDER BY (v1->>'one')::int8;

--Testcase 108:
DROP FOREIGN TABLE example_sorted;

-- ======================================================
-- caching multifile merge reader
-- ======================================================
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/simple/example2.parquet'

-- test option schemaless 'false'
--Testcase 109:
CREATE FOREIGN TABLE example_sorted_caching (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', max_open_files '1', schemaless 'false');

--Testcase 110:
EXPLAIN VERBOSE
SELECT * FROM example_sorted_caching ORDER BY one;
--Testcase 111:
SELECT * FROM example_sorted_caching ORDER BY one;

--Testcase 112:
ALTER FOREIGN TABLE example_sorted_caching OPTIONS (SET schemaless 'true');

-- in schemaless mode caching multifile merge reader can not work without jsonb column.
-- there is no jsonb column to build pathkeys
--Testcase 113:
EXPLAIN VERBOSE
SELECT * FROM example_sorted_caching ORDER BY one;
--Testcase 114:
SELECT * FROM example_sorted_caching ORDER BY one;

--Testcase 115:
DROP FOREIGN TABLE example_sorted_caching;

--Testcase 116:
CREATE FOREIGN TABLE example_sorted_caching (v2 jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', max_open_files '1', schemaless 'true');

--Testcase 117:
EXPLAIN VERBOSE
SELECT * FROM example_sorted_caching ORDER BY (v2->>'one')::int8;
--Testcase 118:
SELECT * FROM example_sorted_caching ORDER BY (v2->>'one')::int8;

-- sorted column is not existed
--Testcase 119:
ALTER FOREIGN TABLE example_sorted_caching OPTIONS (SET sorted 'not_a_col');
--Testcase 120:
EXPLAIN VERBOSE
SELECT * FROM example_sorted_caching ORDER BY (v2->>'not_a_col')::int8; -- should false
--Testcase 121:
SELECT * FROM example_sorted_caching ORDER BY (v2->>'not_a_col')::int8; -- should false

-- sorted column is not existed in one file
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/complex/example4.parquet'
--Testcase 122:
ALTER FOREIGN TABLE example_sorted_caching OPTIONS (SET sorted 'one');
--Testcase 123:
ALTER FOREIGN TABLE example_sorted_caching OPTIONS (SET filename :'var');

--Testcase 124:
EXPLAIN VERBOSE
SELECT * FROM example_sorted_caching ORDER BY (v2->>'one')::int8;
--Testcase 125:
SELECT * FROM example_sorted_caching ORDER BY (v2->>'one')::int8;

--Testcase 126:
DROP FOREIGN TABLE example_sorted_caching;

--Testcase 127:
DROP EXTENSION parquet_s3_fdw CASCADE;
