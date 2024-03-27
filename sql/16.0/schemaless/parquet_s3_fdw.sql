--Testcase 74:
SET datestyle = 'ISO';
--Testcase 75:
SET client_min_messages = WARNING;
--Testcase 76:
SET log_statement TO 'none';
--Testcase 1:
CREATE EXTENSION parquet_s3_fdw;
--Testcase 2:
DROP ROLE IF EXISTS regress_parquet_s3_fdw;
--Testcase 3:
CREATE ROLE regress_parquet_s3_fdw LOGIN SUPERUSER;

--Testcase 77:
SET ROLE regress_parquet_s3_fdw;
--Testcase 4:
CREATE SERVER parquet_s3_srv FOREIGN DATA WRAPPER parquet_s3_fdw :USE_MINIO;
--Testcase 5:
CREATE USER MAPPING FOR regress_parquet_s3_fdw SERVER parquet_s3_srv :USER_PASSWORD;

--Testcase 78:
SET ROLE regress_parquet_s3_fdw;
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 6:
CREATE FOREIGN TABLE example1 (
    v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', schemaless 'true');

--Testcase 7:
SELECT * FROM example1;

-- no explicit columns mentions
--Testcase 8:
SELECT 1 as x FROM example1;
--Testcase 9:
SELECT count(*) as count FROM example1;

-- sorting
--Testcase 10:
EXPLAIN (COSTS OFF) SELECT * FROM example1 ORDER BY (v->>'one')::int8;
--Testcase 11:
EXPLAIN (COSTS OFF) SELECT * FROM example1 ORDER BY (v->>'three')::text;

-- filtering
--Testcase 79:
SET client_min_messages = DEBUG1;
--Testcase 12:
SELECT * FROM example1 WHERE (v->>'one')::bigint < 1;
--Testcase 13:
SELECT * FROM example1 WHERE (v->>'one')::bigint <= 1;
--Testcase 14:
SELECT * FROM example1 WHERE (v->>'one')::bigint > 6;
--Testcase 15:
SELECT * FROM example1 WHERE (v->>'one')::bigint >= 6;
--Testcase 16:
SELECT * FROM example1 WHERE (v->>'one')::bigint = 2;
--Testcase 17:
SELECT * FROM example1 WHERE (v->>'one')::bigint = 7;
--Testcase 18:
SELECT * FROM example1 WHERE (v->>'six')::boolean = true;
--Testcase 19:
SELECT * FROM example1 WHERE (v->>'six')::boolean = false;
--Testcase 20:
SELECT * FROM example1 WHERE (v->>'seven')::float8 < 1.5;
--Testcase 21:
SELECT * FROM example1 WHERE (v->>'seven')::float8 <= 1.5;
--Testcase 22:
SELECT * FROM example1 WHERE (v->>'seven')::float8 = 1.5;
--Testcase 23:
SELECT * FROM example1 WHERE (v->>'seven')::float8 > 1;
--Testcase 24:
SELECT * FROM example1 WHERE (v->>'seven')::float8 >= 1;
--Testcase 25:
SELECT * FROM example1 WHERE (v->>'seven')::float8 IS NULL;

-- prepared statements
--Testcase 26:
prepare prep(date) as select * from example1 where (v->>'five')::date < $1;
--Testcase 27:
execute prep('2018-01-03');
--Testcase 28:
execute prep('2018-01-01');

-- does not support filtering in string column (no row group skipped)
--Testcase 143:
SELECT * FROM example1 WHERE (v->>'three')::text = 'foo';
--Testcase 144:
SELECT * FROM example1 WHERE (v->>'three')::text > 'TRES';
--Testcase 145:
SELECT * FROM example1 WHERE (v->>'three')::text >= 'TRES';
--Testcase 146:
SELECT * FROM example1 WHERE (v->>'three')::text < 'BAZ';
--Testcase 147:
SELECT * FROM example1 WHERE (v->>'three')::text <= 'BAZ';
--Testcase 148:
SELECT * FROM example1 WHERE (v->>'three')::text COLLATE "C" = 'foo';
--Testcase 149:
SELECT * FROM example1 WHERE (v->>'three')::text COLLATE "C" > 'TRES';
--Testcase 150:
SELECT * FROM example1 WHERE (v->>'three')::text COLLATE "C" >= 'TRES';
--Testcase 151:
SELECT * FROM example1 WHERE (v->>'three')::text COLLATE "C" < 'BAZ';
--Testcase 152:
SELECT * FROM example1 WHERE (v->>'three')::text COLLATE "C" <= 'BAZ';

-- invalid options
--Testcase 80:
SET client_min_messages = WARNING;
--Testcase 29:
CREATE FOREIGN TABLE example_fail (v jsonb)
SERVER parquet_s3_srv;
--Testcase 30:
CREATE FOREIGN TABLE example_fail (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename 'nonexistent.parquet', some_option '123', schemaless 'true');
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 31:
CREATE FOREIGN TABLE example_fail (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', some_option '123', schemaless 'true');

-- type mismatch
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 32:
CREATE FOREIGN TABLE example_fail (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', schemaless 'true');
--Testcase 33:
SELECT (v->>'one')::int8[] FROM example_fail;
--Testcase 34:
SELECT (v->>'two')::int8 FROM example_fail;

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
CREATE FOREIGN TABLE example_func (v jsonb)
SERVER parquet_s3_srv
OPTIONS (
    files_func 'list_parquet_s3_files',
    files_func_arg :'var',
    sorted 'one',
    schemaless 'true');
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
CREATE FOREIGN TABLE example_inv_func (v jsonb)
SERVER parquet_s3_srv
OPTIONS (files_func 'int_array_func', schemaless 'true');
--Testcase 41:
CREATE FOREIGN TABLE example_inv_func (v jsonb)
SERVER parquet_s3_srv
OPTIONS (files_func 'no_args_func', schemaless 'true');
--Testcase 42:
CREATE FOREIGN TABLE example_inv_func (v jsonb)
SERVER parquet_s3_srv
OPTIONS (files_func 'list_parquet_s3_files', files_func_arg 'invalid json', schemaless 'true');
--Testcase 43:
DROP FUNCTION list_parquet_s3_files(JSONB);
--Testcase 44:
DROP FUNCTION int_array_func(JSONB);
--Testcase 45:
DROP FUNCTION no_args_func();

-- sequential multifile reader
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/simple/example2.parquet'
--Testcase 46:
CREATE FOREIGN TABLE example_seq (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'true');
--Testcase 47:
EXPLAIN (COSTS OFF) SELECT * FROM example_seq;
--Testcase 48:
SELECT * FROM example_seq;

-- multifile merge reader
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/simple/example2.parquet'
--Testcase 49:
CREATE FOREIGN TABLE example_sorted (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', schemaless 'true');
--Testcase 50:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted ORDER BY (v->>'one')::int8;
--Testcase 51:
SELECT * FROM example_sorted ORDER BY (v->>'one')::int8;

-- caching multifile merge reader
\set var :PATH_FILENAME'/data/simple/example1.parquet ':PATH_FILENAME'/data/simple/example2.parquet'
--Testcase 52:
CREATE FOREIGN TABLE example_sorted_caching (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', max_open_files '1', schemaless 'true');
--Testcase 53:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted_caching ORDER BY (v->>'one')::int8;
--Testcase 54:
SELECT * FROM example_sorted_caching ORDER BY (v->>'one')::int8;

-- parallel execution
--Testcase 81:
SET parallel_setup_cost = 0;
--Testcase 82:
SET parallel_tuple_cost = 0.001;
--Testcase 55:
EXPLAIN (COSTS OFF) SELECT * FROM example_seq;
--Testcase 56:
EXPLAIN (COSTS OFF) SELECT * FROM example_seq ORDER BY (v->>'one')::int8;
--Testcase 57:
EXPLAIN (COSTS OFF) SELECT * FROM example_seq ORDER BY (v->>'two')::int8;
--Testcase 58:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted;
--Testcase 59:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted ORDER BY (v->>'one')::int8;
--Testcase 60:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted ORDER BY (v->>'two')::int8[];

--Testcase 83:
ALTER FOREIGN TABLE example_sorted OPTIONS (ADD files_in_order 'true');
--Testcase 84:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted ORDER BY (v->>'one')::int8;

--Testcase 61:
EXPLAIN (COSTS OFF) SELECT * FROM example1;
--Testcase 62:
SELECT SUM((v->>'one')::int8) FROM example1;

-- multiple sorting keys
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 63:
CREATE FOREIGN TABLE example_multisort (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one five', schemaless 'true');
--Testcase 64:
EXPLAIN (COSTS OFF) SELECT * FROM example_multisort ORDER BY (v->>'one')::int8, (v->>'five')::date;
--Testcase 65:
SELECT * FROM example_multisort ORDER BY (v->>'one')::int8, (v->>'five')::date;

-- maps
\set var :PATH_FILENAME'/data/complex/example3.parquet'
--Testcase 85:
SET client_min_messages = DEBUG1;
--Testcase 66:
CREATE FOREIGN TABLE example3 (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one', schemaless 'true');

--Testcase 67:
SELECT * FROM example3;
--Testcase 68:
SELECT * FROM example3 WHERE (v->>'three')::int4 = 3;

-- analyze
ANALYZE example_sorted;

--Testcase 86:
SET client_min_messages = WARNING;

--get version
--Testcase 69:
\df parquet_s3*
--Testcase 70:
SELECT * FROM public.parquet_s3_fdw_version();
--Testcase 71:
SELECT parquet_s3_fdw_version();

--Testcase 73:
DROP FOREIGN TABLE example1;

-- ====================================================================
-- Check that userid to use when querying the remote table is correctly
-- propagated into foreign rels.
-- In local test parquet file, the query will still success because it
-- does not use a connection.
-- ====================================================================
-- create empty_owner without access information to detect incorrect UserID.
--Testcase 87:
CREATE ROLE empty_owner LOGIN SUPERUSER;
--Testcase 88:
SET ROLE empty_owner;

\set var :PATH_FILENAME'/data/simple/example1.parquet'

--Testcase 89:
CREATE FOREIGN TABLE example1 (
    v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', key_columns 'one', sorted 'one', schemaless 'true');

--Testcase 90:
CREATE VIEW v4 AS SELECT * FROM example1;

--Testcase 91:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;

-- If undefine user owner, postgres core defaults to using the current user to query.
-- For Foreign Scan, Foreign Modify.
--Testcase 92:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;

--Testcase 93:
INSERT INTO v4 VALUES ('{"one":7, "two":[20,21,22], "three":"view", "four":"2023-01-01", "five":"2023-01-01", "six":true, "seven": 2}');
--Testcase 94:
UPDATE v4 SET v = '{"three":"update"}';
--Testcase 95:
DELETE FROM v4;

-- For Import Foreign Schema, postgres fixed using current user.
--Testcase 96:
CREATE SCHEMA s_test;

\set var '\"':PATH_FILENAME'\/ported_postgres\"'
IMPORT FOREIGN SCHEMA :var FROM SERVER parquet_s3_srv INTO s_test OPTIONS (sorted 'c1');

--Testcase 97:
CREATE FUNCTION list_parquet_s3_files(args jsonb)
RETURNS text[] as
$$
    SELECT array_agg(args->>'dir' || filename)
    FROM (VALUES
        ('/example1.parquet', 'simple'),
        ('/example2.parquet', 'simple'),
        ('/example3.parquet', 'complex')
    ) AS files(filename, filetype)
    WHERE filetype = args->>'type';
$$
LANGUAGE SQL;

\set var  '{"dir": "':PATH_FILENAME'/data/simple", "type": "simple"}'
--Testcase 98:
SELECT import_parquet_s3(
    'example_import',
    's_test',
    'parquet_s3_srv',
    'list_parquet_s3_files',
    :'var',
    '{"sorted": "one"}');

--Testcase 99:
DROP FUNCTION list_parquet_s3_files;

--Testcase 100:
DROP SCHEMA s_test CASCADE;

-- For Acquire Sample Rows
ANALYZE example1;

--Testcase 101:
CREATE ROLE regress_view_owner_another;
--Testcase 102:
ALTER VIEW v4 OWNER TO regress_view_owner_another;
--Testcase 103:
ALTER FOREIGN TABLE example1 OWNER TO regress_view_owner_another;
GRANT SELECT ON example1 TO regress_view_owner_another;
GRANT INSERT ON example1 TO regress_view_owner_another;
GRANT UPDATE ON example1 TO regress_view_owner_another;
GRANT DELETE ON example1 TO regress_view_owner_another;

-- It fails as expected due to the lack of a user mapping for that user.
-- For Foreign Scan, Foreign Modify.
--Testcase 104:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;
--Testcase 105:
INSERT INTO v4 VALUES ('{"one":7, "two":[20,21,22], "three":"view", "four":"2023-01-01", "five":"2023-01-01", "six":true, "seven": 2}');
--Testcase 106:
UPDATE v4 SET v = '{"three":"update"}';
--Testcase 107:
DELETE FROM v4;

-- For Import Foreign Schema, postgres fixed using current user.
--Testcase 108:
CREATE SCHEMA s_test;
\set var '\"':PATH_FILENAME'\/ported_postgres\"'
IMPORT FOREIGN SCHEMA :var FROM SERVER parquet_s3_srv INTO s_test OPTIONS (sorted 'c1');

--Testcase 109:
CREATE FUNCTION list_parquet_s3_files(args jsonb)
RETURNS text[] as
$$
    SELECT array_agg(args->>'dir' || filename)
    FROM (VALUES
        ('/example1.parquet', 'simple'),
        ('/example2.parquet', 'simple'),
        ('/example3.parquet', 'complex')
    ) AS files(filename, filetype)
    WHERE filetype = args->>'type';
$$
LANGUAGE SQL;

\set var  '{"dir": "':PATH_FILENAME'/data/simple", "type": "simple"}'
--Testcase 110:
SELECT import_parquet_s3(
    'example_import',
    's_test',
    'parquet_s3_srv',
    'list_parquet_s3_files',
    :'var',
    '{"sorted": "one"}');

--Testcase 111:
DROP FUNCTION list_parquet_s3_files;
--Testcase 112:
DROP SCHEMA s_test CASCADE;

-- For Acquire Sample Rows
ANALYZE example1;

-- Identify the correct user, but it fails due to the lack access informations.
--Testcase 113:
CREATE USER MAPPING FOR regress_view_owner_another SERVER parquet_s3_srv;
-- For Foreign Scan, Foreign Modify.
--Testcase 114:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;
--Testcase 115:
INSERT INTO v4 VALUES ('{"one":7, "two":[20,21,22], "three":"view", "four":"2023-01-01", "five":"2023-01-01", "six":true, "seven": 2}');
--Testcase 116:
UPDATE v4 SET v = '{"three":"update"}';
--Testcase 117:
DELETE FROM v4;

-- For Import Foreign Schema, postgres fixed using current user.
--Testcase 118:
CREATE SCHEMA s_test;
\set var '\"':PATH_FILENAME'\/ported_postgres\"'
IMPORT FOREIGN SCHEMA :var FROM SERVER parquet_s3_srv INTO s_test OPTIONS (sorted 'c1');

--Testcase 119:
CREATE FUNCTION list_parquet_s3_files(args jsonb)
RETURNS text[] as
$$
    SELECT array_agg(args->>'dir' || filename)
    FROM (VALUES
        ('/example1.parquet', 'simple'),
        ('/example2.parquet', 'simple'),
        ('/example3.parquet', 'complex')
    ) AS files(filename, filetype)
    WHERE filetype = args->>'type';
$$
LANGUAGE SQL;

\set var  '{"dir": "':PATH_FILENAME'/data/simple", "type": "simple"}'
--Testcase 120:
SELECT import_parquet_s3(
    'example_import',
    's_test',
    'parquet_s3_srv',
    'list_parquet_s3_files',
    :'var',
    '{"sorted": "one"}');

--Testcase 121:
DROP FUNCTION list_parquet_s3_files;
--Testcase 122:
DROP SCHEMA s_test CASCADE;

-- For Acquire Sample Rows
ANALYZE example1;

--Testcase 123:
DROP USER MAPPING FOR regress_view_owner_another SERVER parquet_s3_srv;

-- Should not get that error once a user mapping is created and have enough information.
--Testcase 124:
CREATE USER MAPPING FOR regress_view_owner_another SERVER parquet_s3_srv :USER_PASSWORD;
-- For Foreign Scan, Foreign Modify.
--Testcase 125:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;
--Testcase 126:
INSERT INTO v4 VALUES ('{"one":7, "two":[20,21,22], "three":"view", "four":"2023-01-01", "five":"2023-01-01", "six":true, "seven": 2}');
--Testcase 127:
UPDATE v4 SET v = '{"three":"update"}';
-- Delete 1 row to avoid ANALYZE empty table issue.
--Testcase 128:
DELETE FROM v4 WHERE (v->>'one')::int = 7;

-- For Import Foreign Schema, postgres fixed using current user.
--Testcase 129:
CREATE SCHEMA s_test;
\set var '\"':PATH_FILENAME'\/ported_postgres\"'
IMPORT FOREIGN SCHEMA :var FROM SERVER parquet_s3_srv INTO s_test OPTIONS (sorted 'c1');
--Testcase 130:
DROP SCHEMA s_test CASCADE;

--Testcase 131:
CREATE FUNCTION list_parquet_s3_files(args jsonb)
RETURNS text[] as
$$
    SELECT array_agg(args->>'dir' || filename)
    FROM (VALUES
        ('/example1.parquet', 'simple'),
        ('/example2.parquet', 'simple'),
        ('/example3.parquet', 'complex')
    ) AS files(filename, filetype)
    WHERE filetype = args->>'type';
$$
LANGUAGE SQL;

--Testcase 132:
CREATE SCHEMA s_test;
\set var  '{"dir": "':PATH_FILENAME'/data/simple", "type": "simple"}'
--Testcase 133:
SELECT import_parquet_s3(
    'example_import',
    's_test',
    'parquet_s3_srv',
    'list_parquet_s3_files',
    :'var',
    '{"sorted": "one"}');

--Testcase 134:
DROP FUNCTION list_parquet_s3_files;
--Testcase 135:
DROP SCHEMA s_test CASCADE;

-- For Acquire Sample Rows
ANALYZE example1;

-- Clean
--Testcase 136:
DROP VIEW v4;
--Testcase 137:
DROP USER MAPPING FOR regress_view_owner_another SERVER parquet_s3_srv;
--Testcase 138:
DROP OWNED BY regress_view_owner_another;
--Testcase 139:
DROP OWNED BY empty_owner;
--Testcase 140:
DROP ROLE regress_view_owner_another;
-- current user cannot be dropped
--Testcase 141:
SET ROLE regress_parquet_s3_fdw;
--Testcase 142:
DROP ROLE empty_owner;

--Testcase 153:
RESET parallel_setup_cost;
--Testcase 154:
RESET parallel_tuple_cost;
-- ===================================================================
-- test case-sensitive column name
-- ===================================================================
\set var :PATH_FILENAME'/data/column_name/case-sensitive.parquet'
--Testcase 155:
CREATE FOREIGN TABLE case_sensitive (
    v jsonb
) SERVER parquet_s3_srv
OPTIONS (filename :'var', schemaless 'true');

--Testcase 156:
\dS+ case_sensitive;
-- Select all data from table, expect correct data for all json object fields
--Testcase 157:
SELECT * FROM case_sensitive;
-- Extract jsonb expression with case-sensitive name
--Testcase 158:
SELECT v->>'UPPER', v->>'lower', v->>'MiXiNg' FROM case_sensitive;
-- Select some fields that do not exist in parquet file
--Testcase 159:
SELECT v->>'upper', v->>'LOWER', v->>'mIxInG' FROM case_sensitive;

-- Test sorted option with case-sensitive columns
-- Single sorting key
--Testcase 160:
ALTER FOREIGN TABLE case_sensitive OPTIONS (ADD sorted '"UPPER"');
--Testcase 161:
\dS+ case_sensitive;
--Testcase 162:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY v->>'UPPER';
--Testcase 163:
SELECT * FROM case_sensitive ORDER BY v->>'UPPER';
-- Try to ORDER BY non-sorted column
--Testcase 164:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY v->>'MiXiNg';
--Testcase 165:
SELECT * FROM case_sensitive ORDER BY v->>'MiXiNg';
-- Multiple sorting key
--Testcase 166:
ALTER FOREIGN TABLE case_sensitive OPTIONS (SET sorted '"UPPER" lower "MiXiNg"');
--Testcase 167:
\dS+ case_sensitive;
--Testcase 168:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY v->>'UPPER', v->>'lower', v->>'MiXiNg';
--Testcase 169:
SELECT * FROM case_sensitive ORDER BY v->>'UPPER', v->>'lower', v->>'MiXiNg';
-- Clean-up
--Testcase 170:
DROP FOREIGN TABLE case_sensitive;

--Testcase 72:
DROP EXTENSION parquet_s3_fdw CASCADE;
