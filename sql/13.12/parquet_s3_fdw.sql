--Testcase 92:
SET datestyle = 'ISO';
--Testcase 93:
SET client_min_messages = WARNING;
--Testcase 94:
SET log_statement TO 'none';
--Testcase 1:
CREATE EXTENSION parquet_s3_fdw;
--Testcase 2:
DROP ROLE IF EXISTS regress_parquet_s3_fdw;
--Testcase 3:
CREATE ROLE regress_parquet_s3_fdw LOGIN SUPERUSER;

--Testcase 95:
SET ROLE regress_parquet_s3_fdw;
--Testcase 4:
CREATE SERVER parquet_s3_srv FOREIGN DATA WRAPPER parquet_s3_fdw :USE_MINIO;
--Testcase 5:
CREATE USER MAPPING FOR regress_parquet_s3_fdw SERVER parquet_s3_srv :USER_PASSWORD;

--Testcase 96:
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
--Testcase 97:
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

-- does not support filtering in string column (no row group skipped)
--Testcase 227:
SELECT * FROM example1 WHERE three = 'foo';
--Testcase 228:
SELECT * FROM example1 WHERE three > 'TRES';
--Testcase 229:
SELECT * FROM example1 WHERE three >= 'TRES';
--Testcase 230:
SELECT * FROM example1 WHERE three < 'BAZ';
--Testcase 231:
SELECT * FROM example1 WHERE three <= 'BAZ';
--Testcase 232:
SELECT * FROM example1 WHERE three COLLATE "C" = 'foo';
--Testcase 233:
SELECT * FROM example1 WHERE three COLLATE "C" > 'TRES';
--Testcase 234:
SELECT * FROM example1 WHERE three COLLATE "C" >= 'TRES';
--Testcase 235:
SELECT * FROM example1 WHERE three COLLATE "C" < 'BAZ';
--Testcase 236:
SELECT * FROM example1 WHERE three COLLATE "C" <= 'BAZ';

-- invalid options
--Testcase 98:
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
-- test multiple columns of foreign table map to the same column of parquet file when caching
-- multifile merge reader
--Testcase 157:
ALTER FOREIGN TABLE example_sorted_caching ADD COLUMN eight INT8 OPTIONS (column_name 'one');
--Testcase 158:
\dS+ example_sorted_caching;
--Testcase 159:
EXPLAIN (COSTS OFF) SELECT * FROM example_sorted_caching ORDER BY one;;
--Testcase 160:
SELECT * FROM example_sorted_caching ORDER BY one;; -- one and eight are both mapped to 'one' column in the data file
-- revert back
--Testcase 161:
ALTER FOREIGN TABLE example_sorted_caching DROP COLUMN eight;

-- parallel execution
--Testcase 99:
SET parallel_setup_cost = 0;
--Testcase 100:
SET parallel_tuple_cost = 0.001;
--Testcase 73:
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
--Testcase 101:
ALTER FOREIGN TABLE example_sorted OPTIONS (ADD files_in_order 'true');
--Testcase 74:
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
--Testcase 102:
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

--Testcase 103:
SET client_min_messages = WARNING;

-- ===================================================================
-- test column options
-- ===================================================================
\set var :PATH_FILENAME'/data/column_name/ftcol.parquet'
--Testcase 104:
CREATE FOREIGN TABLE ftcol (
    c1     INT8,
    c2     INT8,
    c3     TEXT)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'c1');

--Testcase 105:
EXPLAIN (COSTS OFF) SELECT * FROM ftcol;
--Testcase 106:
SELECT * FROM ftcol; -- c1 is blank

-- test adding wrong column option name
--Testcase 107:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c1 OPTIONS (wrong_column_name 'C 1'); -- error

-- test adding correct column_name option
--Testcase 108:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c1 OPTIONS (column_name 'C 1');
--Testcase 109:
\dS+ ftcol;

-- test data is displayed after remapping
--Testcase 110:
EXPLAIN (COSTS OFF) SELECT * FROM ftcol;
--Testcase 111:
SELECT * FROM ftcol; -- c1 is mapped to 'C 1' in the data file

-- test multiple columns of foreign table map to the same column of parquet file
--Testcase 162:
ALTER FOREIGN TABLE ftcol ADD COLUMN c4 INT8 OPTIONS (column_name 'C 1');
--Testcase 163:
\dS+ ftcol;
--Testcase 164:
EXPLAIN (COSTS OFF) SELECT * FROM ftcol;
--Testcase 165:
SELECT * FROM ftcol; -- c1 and c4 are both mapped to 'C 1' column in the data file
-- revert back
--Testcase 166:
ALTER FOREIGN TABLE ftcol DROP COLUMN c4;

-- test sorted column
--Testcase 112:
INSERT INTO ftcol VALUES (0, 4, 'foo'); -- auto sorted without ORDER BY clause
--Testcase 113:
SELECT * FROM ftcol;

-- test sorted column with ORDER BY
--Testcase 114:
EXPLAIN (COSTS OFF)
SELECT * FROM ftcol ORDER BY c1;
--Testcase 115:
SELECT * FROM ftcol ORDER BY c1;

-- test change column mapping, column name is case-sensitive
--Testcase 116:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c1 OPTIONS (drop column_name);
--Testcase 117:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c1 OPTIONS (column_name 'C2');
--Testcase 118:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c2 OPTIONS (column_name 'c 1');
--Testcase 119:
SELECT * FROM ftcol; -- c1 and c2 are emtpy
--Testcase 277:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c1 OPTIONS (SET column_name 'c2');
--Testcase 278:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c2 OPTIONS (SET column_name 'C 1');
--Testcase 279:
SELECT * FROM ftcol; -- c1 and c2 are swapped out

-- test if column in the data file is not existed, empty result
--Testcase 120:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c3 OPTIONS (column_name 'c10');
--Testcase 121:
SELECT * FROM ftcol; -- c3 column empty
--Testcase 122:
INSERT INTO ftcol VALUES (5, 6, 'foobaz'); -- error
--Testcase 123:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c3 OPTIONS (drop column_name);

-- test scanning with mapping column in WHERE clause
--Testcase 124:
SELECT * FROM ftcol;
--Testcase 125:
SELECT * FROM ftcol WHERE c2 = 0;

-- test key column option
--Testcase 126:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c2 OPTIONS (key 'no such value'); -- ERROR
--Testcase 127:
ALTER FOREIGN TABLE ftcol ALTER COLUMN c2 OPTIONS (key 'true'); -- OK

-- test deleting with mapping column in WHERE clause
--Testcase 128:
DELETE FROM ftcol WHERE c2 = 0;
--Testcase 129:
SELECT * FROM ftcol;

-- test updating with mapping column in WHERE clause
--Testcase 130:
UPDATE ftcol SET c1 = 10 WHERE c2 = 1;
--Testcase 131:
SELECT * FROM ftcol;
-- reset table value for next test
--Testcase 132:
UPDATE ftcol SET c1 = 1 WHERE c2 = 1;

-- test with row group filter
--Testcase 133:
SET client_min_messages = DEBUG1;
--Testcase 134:
SELECT * FROM example1;

--Testcase 135:
ALTER FOREIGN TABLE example1 RENAME COLUMN one TO new_one;
--Testcase 136:
ALTER FOREIGN TABLE example1 OPTIONS (SET sorted 'new_one');
--Testcase 137:
SELECT * FROM example1;

--Testcase 138:
ALTER FOREIGN TABLE example1 ALTER COLUMN new_one OPTIONS (column_name 'one');
--Testcase 139:
SELECT * FROM example1;
--Testcase 140:
SELECT * FROM example1 WHERE new_one < 1;
--Testcase 141:
SELECT * FROM example1 WHERE new_one <= 1;
--Testcase 142:
SELECT * FROM example1 WHERE new_one > 6;
--Testcase 143:
SELECT * FROM example1 WHERE new_one >= 6;
--Testcase 144:
SELECT * FROM example1 WHERE new_one = 2;
--Testcase 145:
SELECT * FROM example1 WHERE new_one = 7;
-- Clean-up
--Testcase 146:
DROP FOREIGN TABLE ftcol;
--Testcase 147:
SET client_min_messages = WARNING;

-- test ignoring dropped columns when inserting/deleting data
--Testcase 148:
ALTER FOREIGN TABLE example_multisort ALTER COLUMN one OPTIONS (key 'true');
--Testcase 83:
\dS+ example_multisort;

--Testcase 84:
DELETE FROM example_multisort; -- OK
--Testcase 85:
INSERT INTO example_multisort VALUES(7, '{19,20,21}' , 'seven' , '2018-01-07 00:00:00.00001' , '2018-01-07' , false); -- OK
--Testcase 86:
SELECT * FROM example_multisort;

--Testcase 149:
ALTER FOREIGN TABLE example_multisort DROP COLUMN one;
--Testcase 87:
\dS+ example_multisort;
--Testcase 150:
ALTER FOREIGN TABLE example_multisort ADD COLUMN one INT8 OPTIONS (key 'true');
--Testcase 88:
\dS+ example_multisort;

--Testcase 89:
DELETE FROM example_multisort; -- OK
-- need to specify columns because the column order is changed
--Testcase 90:
INSERT INTO example_multisort(one, two, three, four, five, six) VALUES  (7, '{19,20,21}', 'seven' , '2018-01-07 00:00:00.00001' , '2018-01-07' , false); -- OK
--Testcase 91:
SELECT * FROM example_multisort;
-- ===================================================================
-- test 'sorted' option
-- ===================================================================
\set var :PATH_FILENAME'/data/column_name/ftcol.parquet'
--Testcase 75:
CREATE FOREIGN TABLE ftcol (
    "C 1" int,
    c2 int,
    c3 text
) SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted '"C 1"');

-- test sorted option with a column name has space character and is double quoted
--Testcase 76:
\dS+ ftcol;
--Testcase 77:
SELECT * FROM ftcol;

-- test sorted option with a list of column name separated by a space character
-- if a column name has space character, it must be double quoted.
--Testcase 151:
ALTER FOREIGN TABLE ftcol OPTIONS (set sorted '"C 1" c2');
--Testcase 78:
SELECT * FROM ftcol;

-- test sorted option with a column name has space character but not double quoted
--Testcase 152:
ALTER FOREIGN TABLE ftcol OPTIONS (set sorted 'C 1 c2');
--Testcase 79:
SELECT * FROM ftcol; -- error

-- test sorted option with a column name has space character but missing a double quote
--Testcase 153:
ALTER FOREIGN TABLE ftcol OPTIONS (set sorted '"C 1 c2');
--Testcase 80:
SELECT * FROM ftcol; -- error

-- test sorted option with a list of column name but not separated by space character
--Testcase 154:
ALTER FOREIGN TABLE ftcol OPTIONS (set sorted '"C 1", c2');
--Testcase 81:
SELECT * FROM ftcol; -- error

-- reset sorted option to the default value
--Testcase 155:
ALTER FOREIGN TABLE ftcol OPTIONS (set sorted '"C 1"');
--Testcase 82:
SELECT * FROM ftcol;
-- Clean-up
--Testcase 156:
DROP FOREIGN TABLE ftcol;

-- Test analyze empty table
--Testcase 211:
DROP FOREIGN TABLE example1;
\set var :PATH_FILENAME'/data/simple/example1.parquet'
--Testcase 212:
CREATE FOREIGN TABLE example1 (
    one     INT8 OPTIONS (key 'true'),
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'one');

--Testcase 213:
SET ROLE regress_parquet_s3_fdw;
--Testcase 214:
DELETE FROM example1;
--Testcase 215:
SELECT * FROM example1;
--Testcase 216:
ANALYZE example1;
--Testcase 167:
RESET parallel_setup_cost;
--Testcase 168:
RESET parallel_tuple_cost;
-- ===================================================================
-- test case-sensitive column name
-- ===================================================================
\set var :PATH_FILENAME'/data/column_name/case-sensitive.parquet'
--Testcase 169:
CREATE FOREIGN TABLE case_sensitive (
    "UPPER" text,
    lower text,
    "MiXiNg" text
) SERVER parquet_s3_srv
OPTIONS (filename :'var');

--Testcase 170:
\dS+ case_sensitive;
-- Select all data from table, expect correct data for all columns.
--Testcase 171:
SELECT * FROM case_sensitive;

-- Add some new case-sensitive columns which does not exist in parquet file,
-- expect NULL data for that column.
--Testcase 172:
ALTER FOREIGN TABLE case_sensitive ADD COLUMN upper text, ADD COLUMN "LOWER" text, ADD COLUMN "mIxInG" text;
--Testcase 173:
\dS+ case_sensitive;
--Testcase 174:
SELECT * FROM case_sensitive;

-- Test column name mapping feature with case-sensitive columns.
-- 2 columns "UPPER", "upper" of the foreign table map to column "UPPER" of parquet file.
-- 2 columns "lower", "LOWER" of the foreign table map to column "LOWER" of parquet file.
-- 2 columns "MiXiNg", "mIxInG" of the foreign table map to column "MiXiNg" of parquet file.
--Testcase 175:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN upper OPTIONS (ADD column_name 'UPPER');
--Testcase 176:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "LOWER" OPTIONS (ADD column_name 'lower');
--Testcase 177:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "mIxInG" OPTIONS (ADD column_name 'MiXiNg');
--Testcase 178:
\dS+ case_sensitive;
--Testcase 179:
SELECT * FROM case_sensitive;

-- Test sorted option with case-sensitive columns
--Testcase 180:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN upper OPTIONS (DROP column_name);
--Testcase 181:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "LOWER" OPTIONS (DROP column_name);
--Testcase 182:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "mIxInG" OPTIONS (DROP column_name);
-- Single sorting key
--Testcase 183:
ALTER FOREIGN TABLE case_sensitive OPTIONS (ADD sorted '"UPPER"');
--Testcase 184:
\dS+ case_sensitive;
--Testcase 185:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY "UPPER";
--Testcase 186:
SELECT * FROM case_sensitive ORDER BY "UPPER";
-- Try to ORDER BY non-sorted column
--Testcase 187:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY "MiXiNg";
--Testcase 188:
SELECT * FROM case_sensitive ORDER BY "MiXiNg";
-- Multiple sorting key
--Testcase 189:
ALTER FOREIGN TABLE case_sensitive OPTIONS (SET sorted '"UPPER" lower "MiXiNg"');
--Testcase 190:
\dS+ case_sensitive;
--Testcase 191:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY "UPPER", lower, "MiXiNg";
--Testcase 192:
SELECT * FROM case_sensitive ORDER BY "UPPER", lower, "MiXiNg";

-- Combine column name mapping feature with sorted options for case-sensitive columns
--Testcase 193:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN upper OPTIONS (ADD column_name 'UPPER');
--Testcase 194:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "LOWER" OPTIONS (ADD column_name 'lower');
--Testcase 195:
ALTER FOREIGN TABLE case_sensitive ALTER COLUMN "mIxInG" OPTIONS (ADD column_name 'MiXiNg');
-- Single sorting key
--Testcase 196:
ALTER FOREIGN TABLE case_sensitive OPTIONS (SET sorted 'upper');
--Testcase 197:
\dS+ case_sensitive;
--Testcase 198:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY upper;
--Testcase 199:
SELECT * FROM case_sensitive ORDER BY upper;
-- Try to ORDER BY non-sorted column
--Testcase 200:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY "mIxInG";
--Testcase 201:
SELECT * FROM case_sensitive ORDER BY "mIxInG";
-- Multiple sorting key
--Testcase 202:
ALTER FOREIGN TABLE case_sensitive OPTIONS (SET sorted 'upper "LOWER" "mIxInG"');
--Testcase 203:
\dS+ case_sensitive;
--Testcase 204:
EXPLAIN VERBOSE
SELECT * FROM case_sensitive ORDER BY upper, "LOWER", "mIxInG";
--Testcase 205:
SELECT * FROM case_sensitive ORDER BY upper, "LOWER", "mIxInG";

-- Clean-up
--Testcase 206:
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
