--Testcase 1:
CREATE EXTENSION parquet_s3_fdw;
--Testcase 2:
CREATE SERVER parquet_s3_srv FOREIGN DATA WRAPPER parquet_s3_fdw :USE_MINIO;
--Testcase 3:
CREATE USER MAPPING FOR CURRENT_USER SERVER parquet_s3_srv :USER_PASSWORD;

--
-- Test for options
--
-- Create valid foreign table with dirname option
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_f'
--Testcase 4:
CREATE FOREIGN TABLE tmp_table (
    id INT OPTIONS (key 'true'),
    a text
) SERVER parquet_s3_srv OPTIONS (dirname :'var');
--Testcase 5:
SELECT * FROM tmp_table;

--Testcase 6:
INSERT INTO tmp_table VALUES (30, 'c');
--Testcase 7:
SELECT * FROM tmp_table;

--Testcase 8:
UPDATE tmp_table SET a = 'updated!' WHERE id = 30;
--Testcase 9:
SELECT * FROM tmp_table;

--Testcase 10:
DELETE FROM tmp_table WHERE id = 30;
--Testcase 11:
SELECT * FROM tmp_table;

--Testcase 12:
DROP FOREIGN TABLE tmp_table;

-- Create valid foreign table with filename option
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp_table.parquet'
--Testcase 13:
CREATE FOREIGN TABLE tmp_table (
    id INT OPTIONS (key 'true'),
    a text
) SERVER parquet_s3_srv OPTIONS (filename :'var');
--Testcase 14:
SELECT * FROM tmp_table;

--Testcase 15:
INSERT INTO tmp_table VALUES (30, 'c');
--Testcase 16:
SELECT * FROM tmp_table;

--Testcase 17:
UPDATE tmp_table SET a = 'updated!' WHERE id = 30;
--Testcase 18:
SELECT * FROM tmp_table;

--Testcase 19:
DELETE FROM tmp_table WHERE id = 30;
--Testcase 20:
SELECT * FROM tmp_table;

--Testcase 21:
DROP FOREIGN TABLE tmp_table;

--
-- Check invalid specified foreign tables for modify: use key option for un-supported types
-- use key option for un-supported types: boolean, jsonb, array
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp2_table.parquet'
--Testcase 22:
CREATE FOREIGN TABLE tmp2_table (
    id jsonb OPTIONS (key 'true'),
    a text
) SERVER parquet_s3_srv OPTIONS (filename :'var');
--Testcase 23:
SELECT * FROM tmp2_table;

--Testcase 24:
INSERT INTO tmp2_table VALUES ('{"3": "baz"}', 'c');
--Testcase 25:
SELECT * FROM tmp2_table;

--Testcase 26:
UPDATE tmp2_table SET a = 'updated!' WHERE a = 'b'; -- should fail: un-supported types
--Testcase 27:
SELECT * FROM tmp2_table;

--Testcase 28:
DELETE FROM tmp2_table WHERE a = 'b'; -- should fail: un-supported types
--Testcase 29:
SELECT * FROM tmp2_table;

-- clean-up
--Testcase 30:
ALTER FOREIGN TABLE tmp2_table ALTER COLUMN id OPTIONS (DROP key);
--Testcase 31:
ALTER FOREIGN TABLE tmp2_table ALTER COLUMN a OPTIONS (key 'true');
--Testcase 32:
DELETE FROM tmp2_table WHERE a = 'c';
--Testcase 33:
SELECT * FROM tmp2_table;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp3_table.parquet'
--Testcase 34:
CREATE FOREIGN TABLE tmp3_table (
    id int[] OPTIONS (key 'true'),
    a text
) SERVER parquet_s3_srv OPTIONS (filename :'var');
--Testcase 35:
SELECT * FROM tmp3_table;

--Testcase 36:
INSERT INTO tmp3_table VALUES ('{5, 6}', 'c');
--Testcase 37:
SELECT * FROM tmp3_table;

--Testcase 38:
UPDATE tmp3_table SET a = 'updated!' WHERE a = 'b'; -- should fail: un-supported types
--Testcase 39:
SELECT * FROM tmp3_table;

--Testcase 40:
DELETE FROM tmp3_table WHERE a = 'b'; -- should fail: un-supported types
--Testcase 41:
SELECT * FROM tmp3_table;

-- clean-up
--Testcase 42:
ALTER FOREIGN TABLE tmp3_table ALTER COLUMN id OPTIONS (DROP key);
--Testcase 43:
ALTER FOREIGN TABLE tmp3_table ALTER COLUMN a OPTIONS (key 'true');
--Testcase 44:
DELETE FROM tmp3_table WHERE a = 'c';
--Testcase 45:
SELECT * FROM tmp3_table;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp4_table.parquet'
--Testcase 46:
CREATE FOREIGN TABLE tmp4_table (
    id boolean OPTIONS (key 'true'),
    a text
) SERVER parquet_s3_srv OPTIONS (filename :'var');
--Testcase 47:
SELECT * FROM tmp4_table;

--Testcase 48:
INSERT INTO tmp4_table VALUES (true, 'e');
--Testcase 49:
SELECT * FROM tmp4_table;

--Testcase 50:
UPDATE tmp4_table SET a = 'updated!' WHERE a = 'b'; -- should fail: un-supported types
--Testcase 51:
SELECT * FROM tmp4_table;

--Testcase 52:
DELETE FROM tmp4_table WHERE a = 'b'; -- should fail: un-supported types
--Testcase 53:
SELECT * FROM tmp4_table;

-- clean-up
--Testcase 54:
ALTER FOREIGN TABLE tmp4_table ALTER COLUMN id OPTIONS (DROP key);
--Testcase 55:
ALTER FOREIGN TABLE tmp4_table ALTER COLUMN a OPTIONS (key 'true');
--Testcase 56:
DELETE FROM tmp4_table WHERE a = 'e';
--Testcase 57:
SELECT * FROM tmp4_table;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp5_table.parquet'
--Testcase 58:
CREATE FOREIGN TABLE tmp5_table (
    id char[] OPTIONS (key 'true'),
    a text
) SERVER parquet_s3_srv OPTIONS (filename :'var');
--Testcase 59:
SELECT * FROM tmp5_table;

--Testcase 60:
INSERT INTO tmp5_table VALUES ('{"e", "f"}', 'c');
--Testcase 61:
SELECT * FROM tmp5_table;

--Testcase 62:
UPDATE tmp5_table SET a = 'updated!' WHERE a = 'b'; -- should fail: un-supported types
--Testcase 63:
SELECT * FROM tmp5_table;

--Testcase 64:
DELETE FROM tmp5_table WHERE a = 'b'; -- should fail: un-supported types
--Testcase 65:
SELECT * FROM tmp5_table;

-- clean-up
--Testcase 66:
ALTER FOREIGN TABLE tmp5_table ALTER COLUMN id OPTIONS (DROP key);
--Testcase 67:
ALTER FOREIGN TABLE tmp5_table ALTER COLUMN a OPTIONS (key 'true');
--Testcase 68:
DELETE FROM tmp5_table WHERE a = 'c';
--Testcase 69:
SELECT * FROM tmp5_table;

-- Directory not exists
\set var :PATH_FILENAME'/data/test-modify/does_not_exist/'
--Testcase 70:
CREATE FOREIGN TABLE tmp6_table (
    id char[] OPTIONS (key 'true'),
    a text
) SERVER parquet_s3_srv OPTIONS (dirname :'var');
--Testcase 71:
SELECT * FROM tmp6_table;

--Testcase 72:
DROP FOREIGN TABLE tmp2_table;
--Testcase 73:
DROP FOREIGN TABLE tmp3_table;
--Testcase 74:
DROP FOREIGN TABLE tmp4_table;
--Testcase 75:
DROP FOREIGN TABLE tmp5_table;
--Testcase 76:
DROP FOREIGN TABLE tmp6_table;
--
-- Modification with type of key column
--
--

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_int2.parquet'
--Testcase 77:
CREATE FOREIGN TABLE ft1_int2 (
    c1 INT2 OPTIONS (key 'true'),
    c2 TEXT,
    c3 BOOLEAN
) SERVER parquet_s3_srv OPTIONS (filename :'var');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_int4.parquet'
--Testcase 78:
CREATE FOREIGN TABLE ft1_int4 (
    c1 INT4 OPTIONS (key 'true'),
    c2 TEXT,
    c3 BOOLEAN
) SERVER parquet_s3_srv OPTIONS (filename :'var');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_int8.parquet'
--Testcase 79:
CREATE FOREIGN TABLE ft1_int8 (
    c1 INT8 OPTIONS (key 'true'),
    c2 TEXT,
    c3 BOOLEAN
) SERVER parquet_s3_srv OPTIONS (filename :'var');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_float4.parquet'
--Testcase 80:
CREATE FOREIGN TABLE ft1_float4 (
    c1 FLOAT4 OPTIONS (key 'true'),
    c2 TEXT,
    c3 BOOLEAN
) SERVER parquet_s3_srv OPTIONS (filename :'var');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_float8.parquet'
--Testcase 81:
CREATE FOREIGN TABLE ft1_float8 (
    c1 FLOAT8 OPTIONS (key 'true'),
    c2 TEXT,
    c3 BOOLEAN
) SERVER parquet_s3_srv OPTIONS (filename :'var');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_date.parquet'
--Testcase 82:
CREATE FOREIGN TABLE ft1_date (
    c1 DATE OPTIONS (key 'true'),
    c2 TEXT,
    c3 FLOAT8
) SERVER parquet_s3_srv OPTIONS (filename :'var');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_text.parquet'
--Testcase 83:
CREATE FOREIGN TABLE ft1_text (
    c1 TEXT OPTIONS (key 'true'),
    c2 TEXT,
    c3 FLOAT8
) SERVER parquet_s3_srv OPTIONS (filename :'var');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_timestamp.parquet'
--Testcase 84:
CREATE FOREIGN TABLE ft1_timestamp (
    c1 TIMESTAMP OPTIONS (key 'true'),
    c2 TEXT,
    c3 FLOAT8
) SERVER parquet_s3_srv OPTIONS (filename :'var');
--
-- There are some pre-initialized data
-- So, delete them all so that the query result does not depend on the initialized data
--Testcase 85:
DELETE FROM ft1_int2;
--Testcase 86:
SELECT * FROM ft1_int2;
--Testcase 87:
DELETE FROM ft1_int4;
--Testcase 88:
SELECT * FROM ft1_int4;
--Testcase 89:
DELETE FROM ft1_int8;
--Testcase 90:
SELECT * FROM ft1_int8;
--Testcase 91:
DELETE FROM ft1_float4;
--Testcase 92:
SELECT * FROM ft1_float4;
--Testcase 93:
DELETE FROM ft1_float8;
--Testcase 94:
SELECT * FROM ft1_float8;
--Testcase 95:
DELETE FROM ft1_date;
--Testcase 96:
SELECT * FROM ft1_date;
--Testcase 97:
DELETE FROM ft1_text;
--Testcase 98:
SELECT * FROM ft1_text;
--Testcase 99:
DELETE FROM ft1_timestamp;
--Testcase 100:
SELECT * FROM ft1_timestamp;
--
-- insert with DEFAULT in the target_list
--
--Testcase 101:
INSERT INTO ft1_int2 (c1, c2, c3) VALUES (DEFAULT, DEFAULT, DEFAULT); -- should fail key can not be null
--Testcase 102:
INSERT INTO ft1_int2 (c1, c2, c3) VALUES (DEFAULT, 'test1'); -- should fail
--Testcase 103:
INSERT INTO ft1_int2 (c1) VALUES (DEFAULT, DEFAULT); -- should fail
--Testcase 104:
INSERT INTO ft1_int4 (c1, c2, c3) VALUES (DEFAULT, DEFAULT, DEFAULT); -- should fail key can not be null
--Testcase 105:
INSERT INTO ft1_int8 (c1, c2, c3) VALUES (DEFAULT, DEFAULT, DEFAULT); -- should fail key can not be null
--Testcase 106:
INSERT INTO ft1_float4 (c1, c2, c3) VALUES (DEFAULT, DEFAULT, DEFAULT); -- should fail key can not be null
--Testcase 107:
INSERT INTO ft1_float8 (c1, c2, c3) VALUES (DEFAULT, DEFAULT, DEFAULT); -- should fail key can not be null
--Testcase 108:
INSERT INTO ft1_text (c1, c2, c3) VALUES (DEFAULT, DEFAULT, DEFAULT); -- should fail key can not be null
--Testcase 109:
INSERT INTO ft1_date (c1, c2, c3) VALUES (DEFAULT, DEFAULT, DEFAULT); -- should fail key can not be null
--Testcase 110:
INSERT INTO ft1_timestamp (c1, c2, c3) VALUES (DEFAULT, DEFAULT, DEFAULT); -- should fail key can not be null

--Testcase 111:
SELECT * FROM ft1_int2;
--Testcase 112:
SELECT * FROM ft1_int4;
--Testcase 113:
SELECT * FROM ft1_int8;
--Testcase 114:
SELECT * FROM ft1_float4;
--Testcase 115:
SELECT * FROM ft1_float8;
--Testcase 116:
SELECT * FROM ft1_date;
--Testcase 117:
SELECT * FROM ft1_text;
--Testcase 118:
SELECT * FROM ft1_timestamp;

--
-- VALUES test
--
--Testcase 119:
INSERT INTO ft1_int2 VALUES (1, 'text1', true), (2, DEFAULT, false), ((select 3), (select i from (values('values are fun!')) as foo (i)), true);
--Testcase 120:
SELECT * FROM ft1_int2;

--Testcase 121:
INSERT INTO ft1_int4 VALUES (1000, 'text1', true), (2000, DEFAULT, false), ((select 3000), (select i from (values('fun!')) as foo (i)), true);
--Testcase 122:
SELECT * FROM ft1_int4;

--Testcase 123:
INSERT INTO ft1_int8 VALUES (100000, 'text1', true), (200000, DEFAULT, false), ((select 300000), (select i from (values('values!')) as foo (i)), true);
--Testcase 124:
SELECT * FROM ft1_int8;

--Testcase 125:
INSERT INTO ft1_float4 VALUES (0.1, 'text1', true), (0.2, DEFAULT, false), ((select 0.3), (select i from (values('values!')) as foo (i)), true);
--Testcase 126:
SELECT * FROM ft1_float4;

--Testcase 127:
INSERT INTO ft1_float8 VALUES (0.1, 'text1', true), (0.2, DEFAULT, false), ((select 0.3), (select i from (values('values!')) as foo (i)), true);
--Testcase 128:
SELECT * FROM ft1_float8;

--Testcase 129:
INSERT INTO ft1_text VALUES ('s1', 'txt', 1.0), ((select i from (values('fun!')) as foo (i)), 'fun!', 2.0), ('s2', DEFAULT, 3.0);
--Testcase 130:
SELECT * FROM ft1_text;

--Testcase 131:
INSERT INTO ft1_date VALUES ('2022-01-01', '2022-01-01', 1.0), ('2022-01-02', '2022-01-02', DEFAULT), ((select i from (values (date('2022-01-03'))) as foo (i)), (SELECT 'extext'), 2.0);
--Testcase 132:
SELECT * FROM ft1_date;

--Testcase 133:
INSERT INTO ft1_timestamp VALUES ('2020-01-01 10:00:00', '2020-01-01', 1.0), ('2020-01-02 00:00:00', '2020-01-02', DEFAULT), ((select i from (values(timestamp '2020-01-03 17:00:00')) as foo (i)), (SELECT 'extext'), 2.0);
--Testcase 134:
SELECT * FROM ft1_timestamp;

--
-- TOASTed value test
--
--Testcase 135:
INSERT INTO ft1_int2 VALUES (4, repeat('x', 10000), true);
--Testcase 136:
SELECT c1, char_length(c2), c3 FROM ft1_int2;

--Testcase 137:
INSERT INTO ft1_text VALUES (repeat('x', 10000), 'yr', 1.0);
--Testcase 138:
SELECT char_length(c1), c2, c3 FROM ft1_text;

-- clean up
--Testcase 139:
DELETE FROM ft1_int2;
--Testcase 140:
SELECT * FROM ft1_int2;

--Testcase 141:
DELETE FROM ft1_int4;
--Testcase 142:
SELECT * FROM ft1_int4;

--Testcase 143:
DELETE FROM ft1_int8;
--Testcase 144:
SELECT * FROM ft1_int8;

--Testcase 145:
DELETE FROM ft1_float4;
--Testcase 146:
SELECT * FROM ft1_float4;

--Testcase 147:
DELETE FROM ft1_float8;
--Testcase 148:
SELECT * FROM ft1_float8;

--Testcase 149:
DELETE FROM ft1_date;
--Testcase 150:
SELECT * FROM ft1_date;

--Testcase 151:
DELETE FROM ft1_text;
--Testcase 152:
SELECT * FROM ft1_text;

--Testcase 153:
DELETE FROM ft1_timestamp;
--Testcase 154:
SELECT * FROM ft1_timestamp;

--Testcase 155:
DROP FOREIGN TABLE ft1_int2;
--Testcase 156:
DROP FOREIGN TABLE ft1_int4;
--Testcase 157:
DROP FOREIGN TABLE ft1_int8;
--Testcase 158:
DROP FOREIGN TABLE ft1_float4;
--Testcase 159:
DROP FOREIGN TABLE ft1_float8;
--Testcase 160:
DROP FOREIGN TABLE ft1_date;
--Testcase 161:
DROP FOREIGN TABLE ft1_text;
--Testcase 162:
DROP FOREIGN TABLE ft1_timestamp;

--
-- UPDATE syntax tests
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/update_test.parquet'
--Testcase 163:
CREATE FOREIGN TABLE update_test (
    id SERIAL OPTIONS (key 'true'),
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
) SERVER parquet_s3_srv OPTIONS (filename :'var');

--Testcase 164:
INSERT INTO update_test(a, b, c) VALUES (5, 10, 'foo');
--Testcase 165:
INSERT INTO update_test(b, a) VALUES (15, 10);
--Testcase 166:
SELECT * FROM update_test;

--Testcase 167:
UPDATE update_test SET a = DEFAULT, b = DEFAULT;
--Testcase 168:
SELECT * FROM update_test;

-- aliases for the UPDATE target table
--Testcase 169:
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;
--Testcase 170:
SELECT * FROM update_test;

--Testcase 171:
UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;
--Testcase 172:
SELECT * FROM update_test;

--
-- Test VALUES in FROM
--
--Testcase 173:
UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;
--Testcase 174:
SELECT * FROM update_test;

-- fail, wrong data type
--Testcase 175:
UPDATE update_test SET a = v.* FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

--
-- Test multiple-set-clause syntax
--
--Testcase 176:
INSERT INTO update_test(a, b, c) SELECT a,b+1,c FROM update_test;
--Testcase 177:
SELECT * FROM update_test;

--Testcase 178:
UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';
--Testcase 179:
SELECT * FROM update_test;

--Testcase 180:
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;
--Testcase 181:
SELECT * FROM update_test;

-- fail, multi assignment to same column:
--Testcase 182:
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;

-- uncorrelated sub-select:
--Testcase 183:
UPDATE update_test
  SET (b,a) = (select a,b from update_test where b = 41 and c = 'car')
  WHERE a = 100 AND b = 20;
--Testcase 184:
SELECT * FROM update_test;

-- correlated sub-select:
--Testcase 185:
UPDATE update_test o
  SET (b,a) = (select a+1,b from update_test i
               where i.a=o.a and i.b=o.b and i.c is not distinct from o.c);
--Testcase 186:
SELECT * FROM update_test;

-- fail, multiple rows supplied:
--Testcase 187:
UPDATE update_test SET (b,a) = (select a+1,b from update_test);
-- set to null if no rows supplied:
--Testcase 188:
UPDATE update_test SET (b,a) = (select a+1,b from update_test where a = 1000)
  WHERE a = 11;
--Testcase 189:
SELECT * FROM update_test;

-- expansion should work in this context:
--Testcase 190:
UPDATE update_test SET (a,b) = ROW(v.*) FROM (VALUES(21, 100)) AS v(i, j)
  WHERE update_test.a = v.i;
-- you might expect this to work, but syntactically it's not a RowExpr:
--Testcase 191:
UPDATE update_test SET (a,b) = (v.*) FROM (VALUES(21, 101)) AS v(i, j)
  WHERE update_test.a = v.i;

-- if an alias for the target table is specified, don't allow references
-- to the original table name
--Testcase 192:
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10;

-- Make sure that we can update to a TOASTed value.
--Testcase 193:
UPDATE update_test SET c = repeat('x', 10000) WHERE c = 'car';
--Testcase 194:
SELECT a, b, char_length(c) FROM update_test;

-- Check multi-assignment with a Result node to handle a one-time filter.
--Testcase 195:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
--Testcase 196:
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
--Testcase 197:
SELECT a, b, char_length(c) FROM update_test;

-- clean up
--Testcase 198:
DELETE FROM update_test;
--Testcase 199:
DROP FOREIGN TABLE update_test;

--
-- DELETE
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/delete_test.parquet'
--Testcase 200:
CREATE FOREIGN TABLE delete_test (
    id SERIAL OPTIONS (key 'true'),
    a INT,
    b text
) SERVER parquet_s3_srv OPTIONS (filename :'var');

--Testcase 201:
INSERT INTO delete_test (a) VALUES (10);
--Testcase 202:
INSERT INTO delete_test (a, b) VALUES (50, repeat('x', 10000));
--Testcase 203:
INSERT INTO delete_test (a) VALUES (100);

-- allow an alias to be specified for DELETE's target table
--Testcase 204:
DELETE FROM delete_test AS dt WHERE dt.a > 75;

-- if an alias is specified, don't allow the original table name
-- to be referenced
--Testcase 205:
DELETE FROM delete_test dt WHERE delete_test.a > 25;

--Testcase 206:
SELECT id, a, char_length(b) FROM delete_test;

-- delete a row with a TOASTed value
--Testcase 207:
DELETE FROM delete_test WHERE a > 25;

--Testcase 208:
SELECT id, a, char_length(b) FROM delete_test;

-- clean up
--Testcase 209:
DELETE FROM delete_test;
--Testcase 210:
DROP FOREIGN TABLE delete_test;

--
-- Check invalid specified foreign tables for modify: no key option, on conflict, with check, returning
-- Create parquet file exists with (0,0,'init')
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp_test.parquet'
--Testcase 211:
CREATE FOREIGN TABLE tmp_test (
    id INT,
    a INT,
    b text
) SERVER parquet_s3_srv OPTIONS (filename :'var');

--Testcase 212:
SELECT * FROM tmp_test;
--Testcase 213:
INSERT INTO tmp_test VALUES (1, 1, 'test'); -- OK
--Testcase 214:
SELECT * FROM tmp_test;
--Testcase 215:
UPDATE tmp_test SET a = 2 WHERE id = 1; -- should fail
--Testcase 216:
SELECT * FROM tmp_test;
--Testcase 217:
DELETE FROM tmp_test WHERE a = 2; -- should fail
--Testcase 218:
SELECT * FROM tmp_test;
--Testcase 219:
DROP FOREIGN TABLE tmp_test;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp_test.parquet'
--Testcase 220:
CREATE FOREIGN TABLE tmp_test (
    id INT OPTIONS (key 'true'),
    a INT,
    b text
) SERVER parquet_s3_srv OPTIONS (filename :'var');

--Testcase 221:
insert into tmp_test values(10, 10, 'Crowberry') on conflict (id) do nothing; -- unsupported
--Testcase 222:
insert into tmp_test values (11, 11, 'Apple') on conflict (id) do update set a = 11; -- unsupported
--Testcase 223:
SELECT * FROM tmp_test;
--Testcase 224:
WITH aaa AS (SELECT 12 AS x, 12 AS y, 'Foo' AS z) INSERT INTO tmp_test
  VALUES (13, 13, 'Bar') ON CONFLICT(id)
  DO UPDATE SET (a, b) = (SELECT y, z FROM aaa); -- unsupported
--Testcase 225:
UPDATE tmp_test set a = a + 20 returning id, b, a; -- unsupported
--Testcase 226:
DELETE FROM tmp_test WHERE id = 10 RETURNING id, a, b; -- unsupported

--Testcase 227:
CREATE VIEW tmp_view AS SELECT id, a, b FROM tmp_test WHERE id > 2 ORDER BY id WITH CHECK OPTION;
--Testcase 228:
INSERT INTO tmp_view VALUES (2, 2, 'Mango');  -- unsupported
--Testcase 229:
SELECT * FROM tmp_view;
--Testcase 230:
INSERT INTO tmp_view VALUES (5, 5, 'Pine');  -- unsupported
--Testcase 231:
UPDATE tmp_view SET a = 20 WHERE b = 'Pine';  -- unsupported
--Testcase 232:
DELETE FROM tmp_view WHERE id = 2;
--Testcase 233:
SELECT * FROM tmp_view;

--Testcase 234:
DROP VIEW tmp_view;
--Testcase 235:
DROP FOREIGN TABLE tmp_test;

--
-- Modification with input multi files: input folder
-- Exist some files with same schema: t1_table.parquet, t2_table.parquet, t3_table.parquet
-- Init with id, a
-- t4_table.parquet with different schema: init with id, b
--
\set dir :PATH_FILENAME'/data/test-modify/parquet_modify_2'
\set file_schema1 :PATH_FILENAME'/data/test-modify/parquet_modify_2/t1_table.parquet ' :PATH_FILENAME'/data/test-modify/parquet_modify_2/t2_table.parquet ' :PATH_FILENAME'/data/test-modify/parquet_modify_2/t3_table.parquet '
\set file_schema2 :PATH_FILENAME'/data/test-modify/parquet_modify_2/t4_table.parquet'
--Testcase 236:
CREATE FOREIGN TABLE t_table (
    id INT OPTIONS (key 'true'),
    a text,
    b text
) SERVER parquet_s3_srv OPTIONS (dirname :'dir');

--Testcase 237:
CREATE FOREIGN TABLE t_table_1 (
    id INT OPTIONS (key 'true'),
    a text
) SERVER parquet_s3_srv OPTIONS (filename :'file_schema1');

--Testcase 238:
CREATE FOREIGN TABLE t_table_2 (
    id INT OPTIONS (key 'true'),
    b text
) SERVER parquet_s3_srv OPTIONS (filename :'file_schema2');

--Testcase 239:
SELECT * FROM t_table ORDER BY id;
--Testcase 240:
INSERT INTO t_table(id, a) VALUES (10, 'test');
--Testcase 241:
INSERT INTO t_table(id, a) VALUES (20, 'test');
--Testcase 242:
SELECT * FROM t_table_1 ORDER BY id; -- new value inserted to t_table_1

--Testcase 243:
INSERT INTO t_table(id, b) VALUES (30, 'test');
--Testcase 244:
SELECT * FROM t_table_2 ORDER BY id;  -- new value inserted to t_table_2
--Testcase 245:
SELECT * FROM t_table ORDER BY id;

-- Create new file to keep other file schema
--Testcase 246:
INSERT INTO t_table(id, a, b) VALUES (40, 'foo', 'bar'); -- no file can keep this record
--Testcase 247:
SELECT * FROM t_table ORDER BY id;
--Testcase 248:
SELECT * FROM t_table_1 ORDER BY id; -- no new record inserted
--Testcase 249:
SELECT * FROM t_table_2 ORDER BY id; -- no new record inserted

--Testcase 250:
UPDATE t_table SET a = 'WEJO@' WHERE id = 10;
--Testcase 251:
SELECT * FROM t_table ORDER BY id;
--Testcase 252:
UPDATE t_table SET a = '20' WHERE id = 20;
--Testcase 253:
SELECT * FROM t_table ORDER BY id;

--Testcase 254:
UPDATE t_table SET b = 'updated text' WHERE id = 30;
--Testcase 255:
SELECT * FROM t_table ORDER BY id;

--Testcase 256:
DELETE FROM t_table WHERE id = 10;
--Testcase 257:
SELECT * FROM t_table ORDER BY id;

-- clean up
--Testcase 258:
DELETE FROM t_table WHERE id > 10;

--Testcase 259:
DROP FOREIGN TABLE t_table;
--Testcase 260:
DROP FOREIGN TABLE t_table_1;
--Testcase 261:
DROP FOREIGN TABLE t_table_2;

--
-- Modification with input multi files: input files
-- Exist some files with same schema: t1_table.parquet, t2_table.parquet, t3_table.parquet
-- Init with id, a
-- t4_table.parquet with different schema: id, b
--
\set files :PATH_FILENAME'/data/test-modify/parquet_modify_3/t1_table.parquet ' :PATH_FILENAME'/data/test-modify/parquet_modify_3/t2_table.parquet ' :PATH_FILENAME'/data/test-modify/parquet_modify_3/t3_table.parquet ' :PATH_FILENAME'/data/test-modify/parquet_modify_3/t4_table.parquet'
\set file_schema1 :PATH_FILENAME'/data/test-modify/parquet_modify_3/t1_table.parquet ' :PATH_FILENAME'/data/test-modify/parquet_modify_3/t2_table.parquet ' :PATH_FILENAME'/data/test-modify/parquet_modify_3/t3_table.parquet'
\set file_schema2 :PATH_FILENAME'/data/test-modify/parquet_modify_3/t4_table.parquet'

--Testcase 262:
CREATE FOREIGN TABLE t_table (
    id INT OPTIONS (key 'true'),
    a TEXT,
    b TEXT
) SERVER parquet_s3_srv OPTIONS (filename :'files');

--Testcase 263:
CREATE FOREIGN TABLE t_table_1 (
    id INT OPTIONS (key 'true'),
    a TEXT
) SERVER parquet_s3_srv OPTIONS (filename :'file_schema1');

--Testcase 264:
CREATE FOREIGN TABLE t_table_2 (
    id INT OPTIONS (key 'true'),
    b TEXT
) SERVER parquet_s3_srv OPTIONS (filename :'file_schema2');

--Testcase 265:
SELECT * FROM t_table ORDER BY id;
--Testcase 266:
INSERT INTO t_table(id, a) VALUES (10, 'test');
--Testcase 267:
INSERT INTO t_table(id, a) VALUES (20, 'test');
--Testcase 268:
SELECT * FROM t_table_1 ORDER BY id; -- new value inserted to t_table_1

--Testcase 269:
INSERT INTO t_table(id, b) VALUES (30, 'test');
--Testcase 270:
SELECT * FROM t_table_2 ORDER BY id;  -- new value inserted to t_table_2
--Testcase 271:
SELECT * FROM t_table ORDER BY id;

--Testcase 272:
INSERT INTO t_table(id, a, b) VALUES (40, 'foo', 'bar'); -- should fail no file can keep this record
--Testcase 273:
SELECT * FROM t_table ORDER BY id; -- no new record inserted
--Testcase 274:
SELECT * FROM t_table_1 ORDER BY id; -- no new record inserted
--Testcase 275:
SELECT * FROM t_table_2 ORDER BY id; -- no new record inserted

--Testcase 276:
UPDATE t_table SET a = 'WEJO@' WHERE id = 10;
--Testcase 277:
SELECT * FROM t_table ORDER BY id;
--Testcase 278:
UPDATE t_table SET a = '20' WHERE id = 20;
--Testcase 279:
SELECT * FROM t_table ORDER BY id;

--Testcase 280:
UPDATE t_table SET b = 'updated text' WHERE id = 30;
--Testcase 281:
SELECT * FROM t_table ORDER BY id;

--Testcase 282:
DELETE FROM t_table WHERE id = 10;
--Testcase 283:
SELECT * FROM t_table ORDER BY id;

-- clean up
--Testcase 284:
DELETE FROM t_table WHERE id > 10;

--Testcase 285:
DROP FOREIGN TABLE t_table;
--Testcase 286:
DROP FOREIGN TABLE t_table_1;
--Testcase 287:
DROP FOREIGN TABLE t_table_2;

--
-- Modification with foreign table include multi keys
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_table.parquet'
--Testcase 288:
CREATE FOREIGN TABLE ft1_table (
    c1 INT8,
    c2 TEXT OPTIONS (key 'true'),
    c3 TIMESTAMP OPTIONS (key 'true')
) SERVER parquet_s3_srv OPTIONS (filename :'var');

--Testcase 289:
SELECT * FROM ft1_table;
--Testcase 290:
INSERT INTO ft1_table VALUES (1, 'foo', '2022-08-08 14:00:00');
--Testcase 291:
INSERT INTO ft1_table VALUES (2, 'baz', '2022-08-08 14:00:00');
--Testcase 292:
INSERT INTO ft1_table VALUES (3, 'foo', '2022-08-08 14:14:14');
--Testcase 293:
INSERT INTO ft1_table VALUES (2, 'baz', '2022-08-08 14:14:14');
--Testcase 294:
INSERT INTO ft1_table VALUES (5, 'foo', '2022-08-08 15:00:00');
--Testcase 295:
INSERT INTO ft1_table VALUES (5, 'baz', '2022-08-08 15:00:00');
--Testcase 296:
SELECT * FROM ft1_table;

--Testcase 297:
DELETE FROM ft1_table WHERE c1 = 1;
--Testcase 298:
SELECT * FROM ft1_table;

--Testcase 299:
UPDATE ft1_table SET c2 = c2 || '_UPDATE' WHERE c1 = 5;
--Testcase 300:
SELECT * FROM ft1_table;

-- clean up
--Testcase 301:
DELETE FROM ft1_table;
--Testcase 302:
DROP FOREIGN TABLE ft1_table;

--
-- Modification with value type is LIST/MAP
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_4/ft2_table.parquet'
--Testcase 303:
CREATE FOREIGN TABLE ft2_table (
    id SERIAL OPTIONS (key 'true'),
    c1 jsonb,
    c2 json,
    c3 float8[],
    c4 varchar(5)[],
    c5 name[]
) SERVER parquet_s3_srv OPTIONS (filename :'var');

--Testcase 304:
DELETE FROM ft2_table;
--Testcase 305:
INSERT INTO ft2_table (c1, c2, c3, c4, c5) VALUES ('{"a": {}}', '{"1": 2}', '{}', '{}', '{}');
--Testcase 306:
INSERT INTO ft2_table (c1, c2, c3, c4, c5) VALUES ('{"a": "aaa in bbb"}', '{"a":1}', '{"3.4", "6.7"}', '{"abc","abcde"}', '{"foobar"}');

--Testcase 307:
SELECT * FROM ft2_table;

--Testcase 308:
UPDATE ft2_table SET c3 = '{"1.0", "2.0", "3.0"}' WHERE id = 1;
--Testcase 309:
UPDATE ft2_table SET c5 = '{"name1", "name2", "name3"}' WHERE id = 2;

--Testcase 310:
SELECT * FROM ft2_table;

--Testcase 311:
DELETE FROM ft2_table WHERE id = 1;
--Testcase 312:
SELECT * FROM ft2_table;
-- clean up
--Testcase 313:
DELETE FROM ft2_table;
--Testcase 314:
DROP FOREIGN TABLE ft2_table;

--
-- Test insert to new file: auto gen new file with format [dirpath]/[table_name]-[current_time].parquet
-- or  pointed by insert_file_selector option
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_5'
--Testcase 315:
CREATE FOREIGN TABLE ft_new (
    c1 SERIAL OPTIONS (key 'true'),
    c2 TEXT
) SERVER parquet_s3_srv OPTIONS (dirname :'var');

--Testcase 316:
SELECT * FROM ft_new;
--Testcase 317:
INSERT INTO ft_new (c2) VALUES ('a');
--Testcase 318:
INSERT INTO ft_new (c2) VALUES ('ajawe22A#AJFEkaef');
--Testcase 319:
INSERT INTO ft_new (c2) VALUES ('24656565323');
--Testcase 320:
INSERT INTO ft_new (c2) VALUES ('-1209012');
--Testcase 321:
INSERT INTO ft_new (c2) VALUES ('a');

--Testcase 322:
SELECT * FROM ft_new ORDER BY c1;

--Testcase 323:
UPDATE ft_new SET c2 = 'oneonwe' WHERE c1 > 3;
--Testcase 324:
SELECT * FROM ft_new ORDER BY c1;

--Testcase 325:
DELETE FROM ft_new WHERE c1 > 4;
--Testcase 326:
SELECT * FROM ft_new ORDER BY c1;

-- clean up
--Testcase 327:
DELETE FROM ft_new;
--Testcase 328:
DROP FOREIGN TABLE ft_new;

-- created new file pointed by insert_file_selector option
\set new_file :PATH_FILENAME'/data/test-modify/parquet_modify_5/new_file.parquet'
--Testcase 329:
CREATE FUNCTION selector(c1 INT, dirname text)
RETURNS TEXT AS
$$
    SELECT dirname || '/new_file.parquet';
$$
LANGUAGE SQL;
--Testcase 330:
CREATE FOREIGN TABLE ft_new (
    c1 SERIAL OPTIONS (key 'true'),
    c2 TEXT
) SERVER parquet_s3_srv OPTIONS (insert_file_selector 'selector(c1, dirname)', dirname :'var');

-- new file was not created
--Testcase 331:
CREATE FOREIGN TABLE new_file (
    c1 SERIAL OPTIONS (key 'true'),
    c2 TEXT
) SERVER parquet_s3_srv OPTIONS (filename :'new_file'); -- should fail

--Testcase 332:
INSERT INTO ft_new (c2) VALUES ('b');
--Testcase 333:
INSERT INTO ft_new (c2) VALUES ('_@#AJFEkaef');
--Testcase 334:
INSERT INTO ft_new (c2) VALUES ('2_!(#)');
--Testcase 335:
INSERT INTO ft_new (c2) VALUES ('anu');
--Testcase 336:
INSERT INTO ft_new (c2) VALUES ('swrr');
--Testcase 337:
SELECT * FROM ft_new;

--Testcase 338:
UPDATE ft_new SET c2 = 'oneonwe' WHERE c1 > 3;
--Testcase 339:
SELECT * FROM ft_new;

--Testcase 340:
DELETE FROM ft_new WHERE c1 > 4;
--Testcase 341:
SELECT * FROM ft_new;

-- new file was created
--Testcase 342:
CREATE FOREIGN TABLE new_file (
    c1 SERIAL OPTIONS (key 'true'),
    c2 TEXT
) SERVER parquet_s3_srv OPTIONS (filename :'new_file'); -- OK
--Testcase 343:
SELECT * FROM new_file;

--Testcase 344:
DELETE FROM ft_new;
--Testcase 345:
DROP FOREIGN TABLE new_file;
--Testcase 346:
DROP FOREIGN TABLE ft_new;
--Testcase 347:
DROP FUNCTION selector;

-- Raise error when not specify dirname option, and no schema match
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_5/new_file.parquet'
--Testcase 348:
CREATE FOREIGN TABLE ft_new (
    c1 SERIAL OPTIONS (key 'true'),
    c2 INT,
    c3 DATE
) SERVER parquet_s3_srv OPTIONS (filename :'var');

--Testcase 349:
INSERT INTO ft_new(c2) VALUES (11);
--Testcase 350:
INSERT INTO ft_new(c2) VALUES (12);
--Testcase 351:
SELECT * FROM ft_new;
--Testcase 352:
INSERT INTO ft_new(c2, c3) VALUES (13, '2001-02-02'); -- should fail
--Testcase 353:
UPDATE ft_new SET c3 = '2001-02-05'; -- should fail
--Testcase 354:
SELECT * FROM ft_new;

--Testcase 355:
DELETE FROM ft_new;
--Testcase 356:
DROP FOREIGN TABLE ft_new;

--
-- Test INSERT/UPDATE value with 'sorted' option
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_int.parquet'
--Testcase 357:
CREATE FOREIGN TABLE ft_sorted_int (
    c1 INT OPTIONS (key 'true'),
    c2 TEXT
) SERVER parquet_s3_srv OPTIONS (filename :'var', sorted 'c1');

--Testcase 358:
INSERT INTO ft_sorted_int VALUES (1, 'one');
--Testcase 359:
INSERT INTO ft_sorted_int VALUES (2, 'two');
--Testcase 360:
INSERT INTO ft_sorted_int VALUES (10, 'ten');
--Testcase 361:
INSERT INTO ft_sorted_int VALUES (100, 'one hundred');
--Testcase 362:
INSERT INTO ft_sorted_int VALUES (20, 'twenty');
--Testcase 363:
SELECT * FROM ft_sorted_int;

--Testcase 364:
UPDATE ft_sorted_int SET c1 = 1000 WHERE c2 = 'one';
--Testcase 365:
SELECT * FROM ft_sorted_int;

--Testcase 366:
DELETE FROM ft_sorted_int WHERE c1 = 10;
--Testcase 367:
SELECT * FROM ft_sorted_int;

-- clean up
--Testcase 368:
DELETE FROM ft_sorted_int;
--Testcase 369:
DROP FOREIGN TABLE ft_sorted_int;

-- test with un-support sorted column type
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_text.parquet'
--Testcase 370:
CREATE FOREIGN TABLE ft_sorted_text (
    c1 INT OPTIONS (key 'true'),
    c2 TEXT
) SERVER parquet_s3_srv OPTIONS (filename :'var');

--Testcase 371:
INSERT INTO ft_sorted_text VALUES (1, 'one');
--Testcase 372:
INSERT INTO ft_sorted_text VALUES (2, 'two');
--Testcase 373:
INSERT INTO ft_sorted_text VALUES (10, 'ten');
--Testcase 374:
ALTER FOREIGN TABLE ft_sorted_text OPTIONS (sorted 'c2');
--Testcase 375:
INSERT INTO ft_sorted_text VALUES (100, 'one hundred'); -- should fail
--Testcase 376:
INSERT INTO ft_sorted_text VALUES (20, 'twenty'); -- should fail
--Testcase 377:
SELECT * FROM ft_sorted_text;

--Testcase 378:
UPDATE ft_sorted_text SET c1 = 1000 WHERE c2 = 'one'; -- should fail
--Testcase 379:
SELECT * FROM ft_sorted_text;

--Testcase 380:
DELETE FROM ft_sorted_text WHERE c1 = 10; -- OK
--Testcase 381:
SELECT * FROM ft_sorted_text;

-- clean up
--Testcase 382:
DELETE FROM ft_sorted_text;
--Testcase 383:
DROP FOREIGN TABLE ft_sorted_text;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_date.parquet'
--Testcase 384:
CREATE FOREIGN TABLE ft_sorted_date (
    c1 INT OPTIONS (key 'true'),
    c2 date
) SERVER parquet_s3_srv OPTIONS (filename :'var', sorted 'c2');

-- clean-up first
--Testcase 385:
DELETE FROM ft_sorted_date;

--Testcase 386:
INSERT INTO ft_sorted_date VALUES (1, '2020-01-01');
--Testcase 387:
INSERT INTO ft_sorted_date VALUES (2, '2000-01-01');
--Testcase 388:
INSERT INTO ft_sorted_date VALUES (3, '2021-09-01');
--Testcase 389:
INSERT INTO ft_sorted_date VALUES (4, '1990-11-11');
--Testcase 390:
INSERT INTO ft_sorted_date VALUES (5, '2022-07-07');
--Testcase 391:
SELECT * FROM ft_sorted_date;

--Testcase 392:
UPDATE ft_sorted_date SET c2 = '2022-01-01' WHERE c2 = '2000-01-01';
--Testcase 393:
SELECT * FROM ft_sorted_date;

--Testcase 394:
DELETE FROM ft_sorted_date WHERE c1 = 1;
--Testcase 395:
SELECT * FROM ft_sorted_date;

-- clean up
--Testcase 396:
DELETE FROM ft_sorted_date;
--Testcase 397:
DROP FOREIGN TABLE ft_sorted_date;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_time.parquet'
--Testcase 398:
CREATE FOREIGN TABLE ft_sorted_time (
    c1 INT OPTIONS (key 'true'),
    c2 timestamp
) SERVER parquet_s3_srv OPTIONS (filename :'var', sorted 'c2');

-- clean-up first
--Testcase 399:
DELETE FROM ft_sorted_time;
--Testcase 400:
INSERT INTO ft_sorted_time VALUES (1, '2020-01-01 00:00:00');
--Testcase 401:
INSERT INTO ft_sorted_time VALUES (2, '2000-01-01 10:00:00');
--Testcase 402:
INSERT INTO ft_sorted_time VALUES (3, '2021-09-01 20:00:00');
--Testcase 403:
INSERT INTO ft_sorted_time VALUES (4, '1990-11-11 08:00:00');
--Testcase 404:
INSERT INTO ft_sorted_time VALUES (5, '2022-07-07 07:00:00');
--Testcase 405:
SELECT * FROM ft_sorted_time;

--Testcase 406:
UPDATE ft_sorted_time SET c1 = 10 WHERE c2 = '2020-01-01 00:00:00';
--Testcase 407:
SELECT * FROM ft_sorted_time;

--Testcase 408:
DELETE FROM ft_sorted_time WHERE c1 = 1;
--Testcase 409:
SELECT * FROM ft_sorted_time;

-- clean up
--Testcase 410:
DELETE FROM ft_sorted_time;
--Testcase 411:
DROP FOREIGN TABLE ft_sorted_time;

--
-- un-supported column data type, multiple sorted columns
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_7/'
--Testcase 412:
CREATE FOREIGN TABLE ft_sorted_types (
    c1 INT OPTIONS (key 'true'),
    c2 TEXT,
    c3 float8,
    c4 jsonb,
    c5 INT[]
) SERVER parquet_s3_srv OPTIONS (dirname :'var', sorted 'c2');

--Testcase 413:
INSERT INTO ft_sorted_types(c1, c2) VALUES (1, 'foo'); -- should fail
--Testcase 414:
DROP FOREIGN TABLE ft_sorted_types;

--Testcase 415:
CREATE FOREIGN TABLE ft_sorted_types (
    c1 INT OPTIONS (key 'true'),
    c2 TEXT,
    c3 float8,
    c4 jsonb,
    c5 INT[]
) SERVER parquet_s3_srv OPTIONS (dirname :'var', sorted 'c4');
--Testcase 416:
INSERT INTO ft_sorted_types(c1, c4) VALUES (1, '{"a": "1"}'); -- should fail
--Testcase 417:
DROP FOREIGN TABLE ft_sorted_types;

--Testcase 418:
CREATE FOREIGN TABLE ft_sorted_types (
    c1 INT OPTIONS (key 'true'),
    c2 TEXT,
    c3 float8,
    c4 jsonb,
    c5 INT[]
) SERVER parquet_s3_srv OPTIONS (dirname :'var', sorted 'c5');
--Testcase 419:
INSERT INTO ft_sorted_types(c1, c5) VALUES (1, '{1, 2}'); -- should fail
--Testcase 420:
DROP FOREIGN TABLE ft_sorted_types;

--Testcase 421:
CREATE FUNCTION selector(c1 INT, dirname text)
RETURNS TEXT AS
$$
    SELECT CASE
           WHEN c1 % 2 = 0 THEN dirname || 'ft_sorted_1.parquet'
           ELSE dirname || 'ft_sorted_2.parquet'
           END;
$$
LANGUAGE SQL;

--Testcase 422:
CREATE FOREIGN TABLE ft_sorted_types (
    c1 INT OPTIONS (key 'true'),
    c2 TEXT,
    c3 float8,
    c4 jsonb,
    c5 INT[]
) SERVER parquet_s3_srv OPTIONS (dirname :'var', sorted 'c1 c3', insert_file_selector 'selector(c1 , dirname)');
--Testcase 423:
INSERT INTO ft_sorted_types VALUES (1, 'foo', 1.0, '{"a": 1}', '{1, 2}');
--Testcase 424:
INSERT INTO ft_sorted_types VALUES (3, 'foo', 20.0, '{"a": 2}', '{12, 22}');
--Testcase 425:
INSERT INTO ft_sorted_types VALUES (5, 'foo', 40.0, '{"a": 4}', '{14, 24}');
--Testcase 426:
INSERT INTO ft_sorted_types VALUES (5, 'foo', 30.0, '{"a": 5}', '{15, 25}');
--Testcase 427:
INSERT INTO ft_sorted_types VALUES (7, 'foo', 55.5, '{"a": 6}', '{16, 26}');

-- should failed, can not find position
--Testcase 428:
INSERT INTO ft_sorted_types VALUES (11, 'foo', 10.5, '{"a": 6}', '{16, 26}');

--Testcase 429:
INSERT INTO ft_sorted_types VALUES (2, 'foo', 10.0, '{"a": 10}', '{13, 23}');
--Testcase 430:
INSERT INTO ft_sorted_types VALUES (40, 'foo', 100.2, '{"a": 11}', '{21, 32}');
--Testcase 431:
INSERT INTO ft_sorted_types VALUES (6, 'foo', 60.2, '{"a": 12}', '{1, 32}');
--Testcase 432:
INSERT INTO ft_sorted_types VALUES (6, 'foo', 14.2, '{"a": 13}', '{2, 32}');
--Testcase 433:
INSERT INTO ft_sorted_types VALUES (14, 'foo', 72.2, '{"a": 14}', '{21, 2}');

-- should failed, can not find position
--Testcase 434:
INSERT INTO ft_sorted_types VALUES (18, 'foo', 130.2, '{"a": 15}', '{1, 3}');

--Testcase 435:
SELECT * FROM ft_sorted_types ORDER BY c1;

--Testcase 436:
EXPLAIN VERBOSE
SELECT * FROM ft_sorted_types ORDER BY c1, c3;
--Testcase 437:
SELECT * FROM ft_sorted_types ORDER BY c1, c3;

--Testcase 438:
EXPLAIN VERBOSE
SELECT * FROM ft_sorted_types ORDER BY c1;
--Testcase 439:
SELECT * FROM ft_sorted_types ORDER BY c1;

-- clean up
--Testcase 440:
DELETE FROM ft_sorted_types;
--Testcase 441:
DROP FOREIGN TABLE ft_sorted_types;
--Testcase 442:
DROP FUNCTION selector;

--
--INSERT with user defined function
--
--Testcase 443:
CREATE FUNCTION selector(c1 INT, dirname text)
RETURNS TEXT AS
$$
    SELECT CASE
           WHEN c1 % 2 = 0 THEN dirname || 'ft_sorted_int.parquet'
           ELSE dirname || 'ft_sorted_text.parquet'
           END;
$$
LANGUAGE SQL;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify_6/'
\set file1 :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_int.parquet'
\set file2 :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_text.parquet'
--Testcase 444:
CREATE FOREIGN TABLE ft_user_defined (
    c1 INT OPTIONS (key 'true'),
    c2 TEXT
) SERVER parquet_s3_srv OPTIONS (insert_file_selector 'selector(c1 , dirname)', dirname :'var', sorted 'c1');

--Testcase 445:
CREATE FOREIGN TABLE ft_user_defined_1 (
    c1 INT OPTIONS (key 'true'),
    c2 TEXT
) SERVER parquet_s3_srv OPTIONS (filename :'file1', sorted 'c1');

--Testcase 446:
CREATE FOREIGN TABLE ft_user_defined_2 (
    c1 INT OPTIONS (key 'true'),
    c2 TEXT
) SERVER parquet_s3_srv OPTIONS (filename :'file2', sorted 'c1');

--Testcase 447:
INSERT INTO ft_user_defined VALUES (9, 'aMC');
--Testcase 448:
INSERT INTO ft_user_defined VALUES (11, '!_KWRN@QIEPAE');
--Testcase 449:
INSERT INTO ft_user_defined VALUES (12, 'JAVA');
--Testcase 450:
INSERT INTO ft_user_defined VALUES (13, 'awefrq3');

--Testcase 451:
SELECT * FROM ft_user_defined ORDER BY c1;
--Testcase 452:
SELECT * FROM ft_user_defined_1 ORDER BY c1;
--Testcase 453:
SELECT * FROM ft_user_defined_2 ORDER BY c1;

-- clean up
--Testcase 454:
DELETE FROM ft_user_defined WHERE c1 >= 9;
--Testcase 455:
DROP FOREIGN TABLE ft_user_defined;
--Testcase 456:
DROP FOREIGN TABLE ft_user_defined_1;
--Testcase 457:
DROP FOREIGN TABLE ft_user_defined_2;
--Testcase 458:
DROP FUNCTION selector;
--
-- test for both key and key_columns defined
--

\set var :PATH_FILENAME'/data/test-modify/parquet_modify_f'
--Testcase 459:
CREATE FOREIGN TABLE tmp_table (
    id INT OPTIONS (key 'true'),
    a text
) SERVER parquet_s3_srv OPTIONS (dirname :'var', key_columns 'a');

--Testcase 460:
SELECT * FROM tmp_table;

-- in non-schemaless mode key_columns will be ignore
-- NULL check only for id column
-- should fail: key column must not be null
--Testcase 461:
INSERT INTO tmp_table VALUES (NULL, 'c');
-- OK, ignore key_columns option
--Testcase 462:
INSERT INTO tmp_table VALUES (5, NULL);
--Testcase 463:
SELECT * FROM tmp_table;

-- clean up
--Testcase 464:
DELETE FROM tmp_table WHERE id = 5;
--Testcase 465:
DROP FOREIGN TABLE tmp_table;

-- IMPORT FOREIGN SCHEMA with key_columns option
\set var '\"':PATH_FILENAME'/data/test-modify/parquet_modify/"'
--Testcase 466:
CREATE SCHEMA tmp_schema;
IMPORT FOREIGN SCHEMA :var FROM SERVER parquet_s3_srv INTO tmp_schema OPTIONS (sorted 'c1', key_columns 'id c1 c2');
--Testcase 467:
\det+ tmp_schema.*
--Testcase 468:
\d tmp_schema.*
--Testcase 469:
DROP SCHEMA tmp_schema CASCADE;

--CLEAN
--Testcase 470:
DROP USER MAPPING FOR CURRENT_USER SERVER parquet_s3_srv;
--Testcase 471:
DROP SERVER parquet_s3_srv CASCADE;
--Testcase 472:
DROP EXTENSION parquet_s3_fdw CASCADE;

-- revert data
\! rm -rf /tmp/data_local/data/test-modify || true
\! cp -a data/ /tmp/data_local/
