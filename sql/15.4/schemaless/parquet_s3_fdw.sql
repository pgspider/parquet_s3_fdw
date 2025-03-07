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
SET parallel_setup_cost = 0;
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

ALTER FOREIGN TABLE example_sorted OPTIONS (ADD files_in_order 'true');
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

SET client_min_messages = WARNING;

--Testcase 237:
RESET parallel_setup_cost;
--Testcase 238:
RESET parallel_tuple_cost;
-- ===================================================================
-- test case-sensitive column name
-- ===================================================================
\set var :PATH_FILENAME'/data/column_name/case-sensitive.parquet'
--Testcase 239:
CREATE FOREIGN TABLE case_sensitive (
    "UPPER" text,
    lower text,
    "MiXiNg" text
) SERVER parquet_s3_srv
OPTIONS (filename :'var');

--Testcase 240:
\dS+ case_sensitive;
-- Select all data from table, expect correct data for all columns.
--Testcase 241:
SELECT * FROM case_sensitive;

-- Add some new case-sensitive columns which does not exist in parquet file,
-- expect NULL data for that column.
--Testcase 242:
ALTER FOREIGN TABLE case_sensitive ADD COLUMN upper text, ADD COLUMN "LOWER" text, ADD COLUMN "mIxInG" text;
--Testcase 243:
\dS+ case_sensitive;
--Testcase 244:
SELECT * FROM case_sensitive;

-- Test column name mapping feature with case-sensitive columns.
-- 2 columns "UPPER", "upper" of the foreign table map to column "UPPER" of parquet file.
-- 2 columns "lower", "LOWER" of the foreign table map to column "LOWER" of parquet file.
-- 2 columns "MiXiNg", "mIxInG" of the foreign table map to column "MiXiNg" of parquet file.
--Testcase 245:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN upper OPTIONS (ADD column_name 'UPPER');
--Testcase 246:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "LOWER" OPTIONS (ADD column_name 'lower');
--Testcase 247:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "mIxInG" OPTIONS (ADD column_name 'MiXiNg');
--Testcase 248:
\dS+ case_sensitive;
--Testcase 249:
SELECT * FROM case_sensitive;

-- Test sorted option with case-sensitive columns
--Testcase 250:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN upper OPTIONS (DROP column_name);
--Testcase 251:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "LOWER" OPTIONS (DROP column_name);
--Testcase 252:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "mIxInG" OPTIONS (DROP column_name);
-- Single sorting key
--Testcase 253:
ALTER FOREIGN TABLE case_sensitive OPTIONS (ADD sorted '"UPPER"');
--Testcase 254:
\dS+ case_sensitive;
--Testcase 255:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY "UPPER";
--Testcase 256:
SELECT * FROM case_sensitive ORDER BY "UPPER";
-- Try to ORDER BY non-sorted column
--Testcase 257:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY "MiXiNg";
--Testcase 258:
SELECT * FROM case_sensitive ORDER BY "MiXiNg";
-- Multiple sorting key
--Testcase 259:
ALTER FOREIGN TABLE case_sensitive OPTIONS (SET sorted '"UPPER" lower "MiXiNg"');
--Testcase 260:
\dS+ case_sensitive;
--Testcase 261:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY "UPPER", lower, "MiXiNg";
--Testcase 262:
SELECT * FROM case_sensitive ORDER BY "UPPER", lower, "MiXiNg";

-- Combine column name mapping feature with sorted options for case-sensitive columns
--Testcase 263:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN upper OPTIONS (ADD column_name 'UPPER');
--Testcase 264:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "LOWER" OPTIONS (ADD column_name 'lower');
--Testcase 265:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "mIxInG" OPTIONS (ADD column_name 'MiXiNg');
-- Single sorting key
--Testcase 266:
ALTER FOREIGN TABLE case_sensitive OPTIONS (SET sorted 'upper');
--Testcase 267:
\dS+ case_sensitive;
--Testcase 268:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY upper;
--Testcase 269:
SELECT * FROM case_sensitive ORDER BY upper;
-- Try to ORDER BY non-sorted column
--Testcase 270:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY "mIxInG";
--Testcase 271:
SELECT * FROM case_sensitive ORDER BY "mIxInG";
-- Multiple sorting key
--Testcase 272:
ALTER FOREIGN TABLE case_sensitive OPTIONS (SET sorted 'upper "LOWER" "mIxInG"');
--Testcase 273:
\dS+ case_sensitive;
--Testcase 274:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY upper, "LOWER", "mIxInG";
--Testcase 275:
SELECT * FROM case_sensitive ORDER BY upper, "LOWER", "mIxInG";

-- Clean-up
--Testcase 276:
DROP FOREIGN TABLE case_sensitive;

--get version
--Testcase 69:
\df parquet_s3*
--Testcase 70:
SELECT * FROM public.parquet_s3_fdw_version();
--Testcase 71:
SELECT parquet_s3_fdw_version();

--Testcase 72:
DROP EXTENSION parquet_s3_fdw CASCADE;
