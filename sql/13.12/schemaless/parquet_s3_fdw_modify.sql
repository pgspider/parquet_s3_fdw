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
CREATE FOREIGN TABLE tmp_table (v jsonb) SERVER parquet_s3_srv OPTIONS (dirname :'var', key_columns 'id', schemaless 'true');

--Testcase 5:
SELECT * FROM tmp_table;

--Testcase 6:
INSERT INTO tmp_table VALUES ('{"id":30, "a":"c"}');
--Testcase 7:
SELECT * FROM tmp_table;

--Testcase 8:
UPDATE tmp_table SET v = '{"a":"updated!"}' WHERE (v->>'id')::int = 30;
--Testcase 9:
SELECT * FROM tmp_table;

--Testcase 10:
DELETE FROM tmp_table WHERE (v->>'id')::int = 30;
--Testcase 11:
SELECT * FROM tmp_table;

--Testcase 12:
DROP FOREIGN TABLE tmp_table;

-- Create valid foreign table with filename option
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp_table.parquet'
--Testcase 13:
CREATE FOREIGN TABLE tmp_table (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'id', schemaless 'true');

--Testcase 14:
SELECT * FROM tmp_table;

--Testcase 15:
INSERT INTO tmp_table VALUES ('{"id":30, "a":"c"}');
--Testcase 16:
SELECT * FROM tmp_table;

--Testcase 17:
UPDATE tmp_table SET v = '{"a":"updated!"}' WHERE (v->>'id')::int = 30;
--Testcase 18:
SELECT * FROM tmp_table;

--Testcase 19:
DELETE FROM tmp_table WHERE (v->>'id')::int = 30;
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
CREATE FOREIGN TABLE tmp2_table (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'id', schemaless 'true');

--Testcase 23:
SELECT * FROM tmp2_table;

--Testcase 24:
INSERT INTO tmp2_table VALUES ('{"id": {"3": "baz"}, "a":"c"}');
--Testcase 25:
SELECT * FROM tmp2_table;

--Testcase 26:
UPDATE tmp2_table SET v = '{"a":"updated!"}' WHERE v->>'a' = 'b'; -- should fail: un-supported types
--Testcase 27:
SELECT * FROM tmp2_table;

--Testcase 28:
DELETE FROM tmp2_table WHERE v->>'a' = 'b'; -- should fail: un-supported types
--Testcase 29:
SELECT * FROM tmp2_table;

-- clean-up
--Testcase 30:
ALTER FOREIGN TABLE tmp2_table OPTIONS (SET key_columns 'a');
--Testcase 31:
DELETE FROM tmp2_table WHERE v->>'a' = 'c';
--Testcase 32:
SELECT * FROM tmp2_table;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp3_table.parquet'
--Testcase 33:
CREATE FOREIGN TABLE tmp3_table (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'id', schemaless 'true');

--Testcase 34:
SELECT * FROM tmp3_table;

--Testcase 35:
INSERT INTO tmp3_table VALUES ('{"id": [5, 6], "a":"c"}');
--Testcase 36:
SELECT * FROM tmp3_table;

--Testcase 37:
UPDATE tmp3_table SET v = '{"a":"updated!"}' WHERE v->>'a' = 'b'; -- should fail: un-supported types
--Testcase 38:
SELECT * FROM tmp3_table;

--Testcase 39:
DELETE FROM tmp3_table WHERE v->>'a' = 'b'; -- should fail: un-supported types
--Testcase 40:
SELECT * FROM tmp3_table;

-- clean-up
--Testcase 41:
ALTER FOREIGN TABLE tmp3_table OPTIONS (SET key_columns 'a');
--Testcase 42:
DELETE FROM tmp3_table WHERE v->>'a' = 'c';
--Testcase 43:
SELECT * FROM tmp3_table;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp4_table.parquet'
--Testcase 44:
CREATE FOREIGN TABLE tmp4_table (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'id', schemaless 'true');

--Testcase 45:
SELECT * FROM tmp4_table;

--Testcase 46:
INSERT INTO tmp4_table VALUES ('{"id": true, "a":"e"}');
--Testcase 47:
SELECT * FROM tmp4_table;

--Testcase 48:
UPDATE tmp4_table SET v = '{"a":"updated!"}' WHERE v->>'a' = 'b'; -- should fail: un-supported types
--Testcase 49:
SELECT * FROM tmp4_table;

--Testcase 50:
DELETE FROM tmp4_table WHERE v->>'a' = 'b'; -- should fail: un-supported types
--Testcase 51:
SELECT * FROM tmp4_table;

-- clean-up
--Testcase 52:
ALTER FOREIGN TABLE tmp4_table OPTIONS (SET key_columns 'a');
--Testcase 53:
DELETE FROM tmp4_table WHERE v->>'a' = 'e';
--Testcase 54:
SELECT * FROM tmp4_table;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp5_table.parquet'
--Testcase 55:
CREATE FOREIGN TABLE tmp5_table (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'id', schemaless 'true');

--Testcase 56:
SELECT * FROM tmp5_table;

--Testcase 57:
INSERT INTO tmp5_table VALUES ('{"id": ["e", "f"], "a":"c"}');
--Testcase 58:
SELECT * FROM tmp5_table;

--Testcase 59:
UPDATE tmp5_table SET v = '{"a":"updated!"}' WHERE v->>'a' = 'b'; -- should fail: un-supported types
--Testcase 60:
SELECT * FROM tmp5_table;

--Testcase 61:
DELETE FROM tmp5_table WHERE v->>'a' = 'b'; -- should fail: un-supported types
--Testcase 62:
SELECT * FROM tmp5_table;

-- clean-up
--Testcase 63:
ALTER FOREIGN TABLE tmp5_table OPTIONS (SET key_columns 'a');
--Testcase 64:
DELETE FROM tmp5_table WHERE v->>'a' = 'c';
--Testcase 65:
SELECT * FROM tmp5_table;

-- Directory not exists
\set var :PATH_FILENAME'/data/test-modify/does_not_exist/'
--Testcase 66:
CREATE FOREIGN TABLE tmp6_table (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'id', schemaless 'true');

--Testcase 67:
SELECT * FROM tmp6_table;

--Testcase 68:
DROP FOREIGN TABLE tmp2_table;
--Testcase 69:
DROP FOREIGN TABLE tmp3_table;
--Testcase 70:
DROP FOREIGN TABLE tmp4_table;
--Testcase 71:
DROP FOREIGN TABLE tmp5_table;
--Testcase 72:
DROP FOREIGN TABLE tmp6_table;
--
-- Modification with type of key column
--
--

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_int2.parquet'
--Testcase 73:
CREATE FOREIGN TABLE ft1_int2 (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c1', schemaless 'true');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_int4.parquet'
--Testcase 74:
CREATE FOREIGN TABLE ft1_int4 (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c1', schemaless 'true');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_int8.parquet'
--Testcase 75:
CREATE FOREIGN TABLE ft1_int8 (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c1', schemaless 'true');
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_float4.parquet'
--Testcase 76:
CREATE FOREIGN TABLE ft1_float4 (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c1', schemaless 'true');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_float8.parquet'
--Testcase 77:
CREATE FOREIGN TABLE ft1_float8 (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c1', schemaless 'true');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_date.parquet'
--Testcase 78:
CREATE FOREIGN TABLE ft1_date (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c1', schemaless 'true');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_text.parquet'
--Testcase 79:
CREATE FOREIGN TABLE ft1_text (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c1', schemaless 'true');

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_timestamp.parquet'
--Testcase 80:
CREATE FOREIGN TABLE ft1_timestamp (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c1', schemaless 'true');
--
-- There are some pre-initialized data
-- So, delete them all so that the query result does not depend on the initialized data
--Testcase 81:
DELETE FROM ft1_int2;
--Testcase 82:
SELECT * FROM ft1_int2;
--Testcase 83:
DELETE FROM ft1_int4;
--Testcase 84:
SELECT * FROM ft1_int4;
--Testcase 85:
DELETE FROM ft1_int8;
--Testcase 86:
SELECT * FROM ft1_int8;
--Testcase 87:
DELETE FROM ft1_float4;
--Testcase 88:
SELECT * FROM ft1_float4;
--Testcase 89:
DELETE FROM ft1_float8;
--Testcase 90:
SELECT * FROM ft1_float8;
--Testcase 91:
DELETE FROM ft1_date;
--Testcase 92:
SELECT * FROM ft1_date;
--Testcase 93:
DELETE FROM ft1_text;
--Testcase 94:
SELECT * FROM ft1_text;
--Testcase 95:
DELETE FROM ft1_timestamp;
--Testcase 96:
SELECT * FROM ft1_timestamp;
--
-- insert with DEFAULT in the target_list
--
--Testcase 97:
INSERT INTO ft1_int2 VALUES ('{}'); -- should fail key can not be null
--Testcase 98:
INSERT INTO ft1_int2 VALUES ('{"c3": "test1"}'); -- should fail key can not be null
--Testcase 99:
INSERT INTO ft1_int2 VALUES ('{}'); -- should fail key can not be null
--Testcase 100:
INSERT INTO ft1_int4 VALUES ('{}'); -- should fail key can not be null
--Testcase 101:
INSERT INTO ft1_int8 VALUES ('{}'); -- should fail key can not be null
--Testcase 102:
INSERT INTO ft1_float4 VALUES ('{}'); -- should fail key can not be null
--Testcase 103:
INSERT INTO ft1_float8 VALUES ('{}'); -- should fail key can not be null
--Testcase 104:
INSERT INTO ft1_text VALUES ('{}'); -- should fail key can not be null
--Testcase 105:
INSERT INTO ft1_date VALUES ('{}'); -- should fail key can not be null
--Testcase 106:
INSERT INTO ft1_timestamp VALUES ('{}'); -- should fail key can not be null

--Testcase 107:
SELECT * FROM ft1_int2;
--Testcase 108:
SELECT * FROM ft1_int4;
--Testcase 109:
SELECT * FROM ft1_int8;
--Testcase 110:
SELECT * FROM ft1_float4;
--Testcase 111:
SELECT * FROM ft1_float8;
--Testcase 112:
SELECT * FROM ft1_date;
--Testcase 113:
SELECT * FROM ft1_text;
--Testcase 114:
SELECT * FROM ft1_timestamp;

--
-- VALUES test
--
--Testcase 115:
INSERT INTO ft1_int2 VALUES ('{"c1": 1, "c2": "text1", "c3": true}'), ('{"c1": 2, "c2": null, "c3": false}'), ('{"c1": 3, "c2": "values are fun!", "c3": true}');
--Testcase 116:
SELECT * FROM ft1_int2;

--Testcase 117:
INSERT INTO ft1_int4 VALUES ('{"c1": 1000, "c2": "text1", "c3": true}'), ('{"c1": 2000, "c3": false}'), ('{"c1": 3000, "c2": "fun!", "c3": true}');
--Testcase 118:
SELECT * FROM ft1_int4;

--Testcase 119:
INSERT INTO ft1_int8 VALUES ('{"c1": 100000, "c2": "text1", "c3": true}'), ('{"c1": 200000, "c3": false}'), ('{"c1": 300000, "c2": "values!", "c3": true}');
--Testcase 120:
SELECT * FROM ft1_int8;

--Testcase 121:
INSERT INTO ft1_float4 VALUES ('{"c1": 0.1, "c2": "text1", "c3": true}'), ('{"c1": 0.2, "c3": false}'), (json_build_object('c1', (select 0.3), 'c2', (select i from (values('values!')) as foo (i)),'c3', true));
--Testcase 122:
SELECT * FROM ft1_float4;

--Testcase 123:
INSERT INTO ft1_float8 VALUES ('{"c1": 0.1, "c2": "text1", "c3": true}'), ('{"c1": 0.2, "c3": false}'), (json_build_object('c1', (select 0.3), 'c2', (select i from (values('values!')) as foo (i)),'c3', true));
--Testcase 124:
SELECT * FROM ft1_float8;

--Testcase 125:
INSERT INTO ft1_text VALUES ('{"c1": "s1", "c2": "txt", "c3": 1.0}'), ('{"c1": "fun!", "c2": "fun!", "c3": 2.0}'), ('{"c1": "s2", "c2": null, "c3": 3.0}');
--Testcase 126:
SELECT * FROM ft1_text;

--Testcase 127:
INSERT INTO ft1_date VALUES ('{"c1": "2022-01-02", "c2": "2022-01-02", "c3": 1.0}'), ('{"c1": "2022-01-02", "c2": "2022-01-02", "c3": null}'), ('{"c1": "2022-01-03", "c2": "extext", "c3": 2.0}');
--Testcase 128:
SELECT * FROM ft1_date;

--Testcase 129:
INSERT INTO ft1_timestamp VALUES ('{"c1": "2020-01-01 10:00:00", "c2": "2020-01-01", "c3": 1.0}'), ('{"c1": "2020-01-02 10:00:00", "c2": "2020-01-02", "c3": null}'), ('{"c1": "2020-01-03 17:00:00", "c2": "extext", "c3": 2.0}');
--Testcase 130:
SELECT * FROM ft1_timestamp;

--
-- TOASTed value test
--
--Testcase 131:
INSERT INTO ft1_int2 VALUES (jsonb_build_object('c1', 4, 'c2', repeat('x', 10000), 'c3', true));
--Testcase 132:
INSERT INTO ft1_text VALUES (jsonb_build_object('c1', repeat('x', 10000), 'c2', 'yr', 'c3', 1.0));

--Testcase 133:
SELECT (v->>'c1')::int, char_length(v->>'c2'), (v->>'c3')::boolean FROM ft1_int2;
--Testcase 134:
SELECT char_length(v->>'c1'), v->>'c2', (v->>'c3')::double precision FROM ft1_text;

-- clean up
--Testcase 135:
DELETE FROM ft1_int2;
--Testcase 136:
SELECT * FROM ft1_int2;

--Testcase 137:
DELETE FROM ft1_int4;
--Testcase 138:
SELECT * FROM ft1_int4;

--Testcase 139:
DELETE FROM ft1_int8;
--Testcase 140:
SELECT * FROM ft1_int8;

--Testcase 141:
DELETE FROM ft1_float4;
--Testcase 142:
SELECT * FROM ft1_float4;

--Testcase 143:
DELETE FROM ft1_float8;
--Testcase 144:
SELECT * FROM ft1_float8;

--Testcase 145:
DELETE FROM ft1_date;
--Testcase 146:
SELECT * FROM ft1_date;

--Testcase 147:
DELETE FROM ft1_text;
--Testcase 148:
SELECT * FROM ft1_text;

--Testcase 149:
DELETE FROM ft1_timestamp;
--Testcase 150:
SELECT * FROM ft1_timestamp;

--Testcase 151:
DROP FOREIGN TABLE ft1_int2;
--Testcase 152:
DROP FOREIGN TABLE ft1_int4;
--Testcase 153:
DROP FOREIGN TABLE ft1_int8;
--Testcase 154:
DROP FOREIGN TABLE ft1_float4;
--Testcase 155:
DROP FOREIGN TABLE ft1_float8;
--Testcase 156:
DROP FOREIGN TABLE ft1_date;
--Testcase 157:
DROP FOREIGN TABLE ft1_text;
--Testcase 158:
DROP FOREIGN TABLE ft1_timestamp;

--
-- UPDATE syntax tests
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/update_test.parquet'
--Testcase 159:
CREATE FOREIGN TABLE update_test (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'id', schemaless 'true');

--Testcase 160:
INSERT INTO update_test VALUES ('{"id": 1, "a": 5, "b": 10, "c": "foo"}');
--Testcase 161:
INSERT INTO update_test VALUES ('{"id": 2, "b": 15, "a": 10, "c": null}');
--Testcase 162:
SELECT * FROM update_test;

--Testcase 163:
UPDATE update_test SET v = '{"a": 10, "b": null}';
--Testcase 164:
SELECT * FROM update_test;

-- aliases for the UPDATE target table
--Testcase 165:
UPDATE update_test AS t SET v = '{"b": 10}' WHERE (t.v->>'a')::int = 10;
--Testcase 166:
SELECT * FROM update_test;

--Testcase 167:
UPDATE update_test t SET v = json_build_object('b', (t.v->>'b')::int + 10) WHERE (t.v->>'a')::int = 10;
--Testcase 168:
SELECT * FROM update_test;

--
-- Test VALUES in FROM
--
--Testcase 169:
UPDATE update_test SET v = json_build_object('a', v.i) FROM (VALUES(100, 20)) AS v(i, j)
  WHERE (update_test.v->>'b')::int = v.j;
--Testcase 170:
SELECT * FROM update_test;

-- fail, wrong data type:
--Testcase 171:
UPDATE update_test SET v = json_build_object('a', v.*) FROM (VALUES(100, 20)) AS v(i, j)
  WHERE (update_test.v->>'b')::int = v.j;

--
-- Test multiple-set-clause syntax
--
--Testcase 172:
INSERT INTO update_test SELECT json_build_object('id', (v->>'id')::int + 2, 'a', (v->>'a')::int, 'b', (v->>'b')::int + 1, 'c', v->>'c') FROM update_test;
--Testcase 173:
SELECT * FROM update_test;

--Testcase 174:
UPDATE update_test SET v = json_build_object('c', 'bugle', 'b', (v->>'b')::int + 11, 'a', 10) WHERE v->>'c' = 'foo';
--Testcase 175:
SELECT * FROM update_test;

--Testcase 176:
UPDATE update_test SET v = json_build_object('c', 'car', 'b', (v->>'a')::int + (v->>'b')::int, 'a', (v->>'a')::int + 1) WHERE (v->>'a')::int = 10;
--Testcase 177:
SELECT * FROM update_test;

-- fail, multi assignment to same column:
--Testcase 178:
UPDATE update_test SET v = json_build_object('c', 'car', 'b', (v->>'a')::int + (v->>'b')::int), v = json_build_object('b', (v->>'a')::int + 1) WHERE (v->>'a')::int = 10;

-- uncorrelated sub-select:
--Testcase 179:
UPDATE update_test
  SET (v) = (select jsonb_build_object('b', (v->>'a')::int, 'a', (v->>'b')::int) from update_test where (v->>'b')::int = 41 and v->>'c' = 'car')
  WHERE (v->>'a')::int = 100 AND (v->>'b')::int = 20;
--Testcase 180:
SELECT * FROM update_test;

-- correlated sub-select:
--Testcase 181:
UPDATE update_test o
  SET (v) = (select jsonb_build_object('b', (v->>'a')::int + 1, 'a', (v->>'b')::int) from update_test i 
            where (i.v->>'a')::int = (o.v->>'a')::int and (i.v->>'b')::int = (o.v->>'b')::int and i.v->>'c' is not distinct from o.v->>'c');
--Testcase 182:
SELECT * FROM update_test;

-- fail, multiple rows supplied:
--Testcase 183:
UPDATE update_test SET (v) = (select jsonb_build_object('b', (v->>'a')::int + 1, 'a', (v->>'b')::int) from update_test);
-- set to null if no rows supplied:
--Testcase 184:
UPDATE update_test SET (v) = (select jsonb_build_object('b', (v->>'a')::int + 1, 'a', (v->>'b')::int) from update_test WHERE (v->>'a')::int = 1000)
  WHERE (v->>'a')::int = 11;
--Testcase 185:
SELECT * FROM update_test;

-- expansion should work in this context:
--Testcase 186:
UPDATE update_test SET (v) = row(jsonb_build_object('a', v.i, 'b', v.j))  FROM (VALUES(21, 100)) AS v(i, j)
  WHERE (update_test.v->>'a')::int = v.i;
-- you might expect this to work, but syntactically it's not a RowExpr:
--Testcase 187:
UPDATE update_test SET v = json_build_object('a', v.*) FROM (VALUES(21, 101)) AS v(i, j)
WHERE (update_test.v->>'a')::int = v.i;

-- if an alias for the target table is specified, don't allow references
-- to the original table name
--Testcase 188:
UPDATE update_test AS t SET v = json_build_object('b', (update_test.v->>'b')::int + 10) WHERE (t.v->>'a')::int = 10;

-- Make sure that we can update to a TOASTed value.
--Testcase 189:
UPDATE update_test SET v = jsonb_build_object('c', repeat('x', 10000)) WHERE v->>'c' = 'car';
--Testcase 190:
SELECT (v->>'a')::int, (v->>'b')::int, char_length(v->>'c') FROM update_test;

-- Check multi-assignment with a Result node to handle a one-time filter.
--Testcase 191:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE update_test t
  SET (v) = (SELECT jsonb_build_object('b', (v->>'a')::int, 'a', (v->>'b')::int) FROM update_test s WHERE (s.v->>'a')::int = (t.v->>'a')::int)
  WHERE CURRENT_USER = SESSION_USER;
--Testcase 192:
UPDATE update_test t
  SET (v) = (SELECT jsonb_build_object('b', (v->>'a')::int, 'a', (v->>'b')::int) FROM update_test s WHERE (s.v->>'a')::int = (t.v->>'a')::int)
  WHERE CURRENT_USER = SESSION_USER;
--Testcase 193:
SELECT (v->>'a')::int, (v->>'b')::int, char_length(v->>'c') FROM update_test;

-- clean up
--Testcase 194:
DELETE FROM update_test;
--Testcase 195:
DROP FOREIGN TABLE update_test;

--
-- DELETE
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/delete_test.parquet'
--Testcase 196:
CREATE FOREIGN TABLE delete_test (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'id', schemaless 'true');

--Testcase 197:
INSERT INTO delete_test VALUES ('{"id": 1, "a": 10}');
--Testcase 198:
INSERT INTO delete_test VALUES (jsonb_build_object('id', 2, 'a', 50, 'b', repeat('x', 10000)));
--Testcase 199:
INSERT INTO delete_test VALUES ('{"id": 3, "a": 100}');

-- allow an alias to be specified for DELETE's target table
--Testcase 200:
DELETE FROM delete_test AS dt WHERE (dt.v->>'a')::int > 75;

-- if an alias is specified, don't allow the original table name
-- to be referenced
--Testcase 201:
DELETE FROM delete_test dt WHERE (delete_test.v->>'a')::int > 25;

--Testcase 202:
SELECT (v->>'id')::int, (v->>'a')::int, char_length(v->>'b') FROM delete_test;

-- delete a row with a TOASTed value
--Testcase 203:
DELETE FROM delete_test WHERE (v->>'a')::int > 25;

--Testcase 204:
SELECT (v->>'id')::int, (v->>'a')::int, char_length(v->>'b') FROM delete_test;

-- clean up
--Testcase 205:
DELETE FROM delete_test;
--Testcase 206:
DROP FOREIGN TABLE delete_test;

--
-- Check invalid specified foreign tables for modify: no key option, on conflict, with check, returning
-- Create parquet file exists with (0,0,'init')
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp_test.parquet'
--Testcase 207:
CREATE FOREIGN TABLE tmp_test (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', schemaless 'true');

--Testcase 208:
SELECT * FROM tmp_test;
--Testcase 209:
INSERT INTO tmp_test VALUES ('{"id": 1, "a": 1, "b": "test"}'); -- OK
--Testcase 210:
SELECT * FROM tmp_test;
--Testcase 211:
UPDATE tmp_test SET v = '{"a": 2}' WHERE (v->>'id')::int = 1; -- should fail
--Testcase 212:
SELECT * FROM tmp_test;
--Testcase 213:
DELETE FROM tmp_test WHERE (v->>'a')::int = 2; -- should fail
--Testcase 214:
SELECT * FROM tmp_test;
--Testcase 215:
DROP FOREIGN TABLE tmp_test;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify/tmp_test.parquet'
--Testcase 216:
CREATE FOREIGN TABLE tmp_test (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'id', schemaless 'true');

--Testcase 217:
INSERT INTO tmp_test VALUES ('{"id": 10, "a": 10, "b": "Crowberry"}') ON CONFLICT (v) DO NOTHING; -- unsupported
--Testcase 218:
INSERT INTO tmp_test VALUES ('{"id": 11, "a": 11, "b": "Apple"}') ON CONFLICT (v) DO UPDATE SET v = '{"a": 11}'; -- unsupported
--Testcase 219:
SELECT * FROM tmp_test;
--Testcase 220:
WITH aaa AS (SELECT 12 AS x, 12 AS y, 'Foo' AS z) INSERT INTO tmp_test
 VALUES ('{"id": 13, "a": 13, "b": "Bar"}') ON CONFLICT (v)
 DO UPDATE SET (v) = (SELECT json_build_object('id', y, 'a', y, 'b', z) FROM aaa); -- unsupported
--Testcase 221:
UPDATE tmp_test SET v = jsonb_build_object ('a', (v->'a')::int + 20) RETURNING (v->>'id')::int, (v->>'a')::int, v->>'b';
--Testcase 222:
DELETE FROM tmp_test WHERE (v->>'id')::int = 10 RETURNING (v->>'id')::int, (v->>'a')::int, v->>'b';

--Testcase 223:
CREATE VIEW tmp_view AS SELECT v FROM tmp_test WHERE (v->>'id')::int > 2 ORDER BY (v->>'id')::int WITH CHECK OPTION;
--Testcase 224:
INSERT INTO tmp_view VALUES ('{"id": 2, "a": 2, "b": "Mango"}');  -- unsupported
--Testcase 225:
SELECT * FROM tmp_view;
--Testcase 226:
INSERT INTO tmp_view VALUES ('{"id": 5, "a": 5, "b": "Pine"}');  -- unsupported
--Testcase 227:
UPDATE tmp_view SET v = '{"a": 20}' WHERE v->>'b' = 'Pine';  -- unsupported
--Testcase 228:
DELETE FROM tmp_view WHERE (v->>'id')::int = 2;
--Testcase 229:
SELECT * FROM tmp_view;

--Testcase 230:
DROP VIEW tmp_view;
--Testcase 231:
DROP FOREIGN TABLE tmp_test;

--
-- Modification with input multi files: input folder
-- Exist some files with same schema: t1_table.parquet, t2_table.parquet, t3_table.parquet
-- Init with id, a
-- t4_table.parquet with different schema: init with id, b
--
\set dir :PATH_FILENAME'/data/test-modify/parquet_modify_2'
\set file_schema1 :PATH_FILENAME'/data/test-modify/parquet_modify_2/t1_table.parquet ' :PATH_FILENAME'/data/test-modify/parquet_modify_2/t2_table.parquet ' :PATH_FILENAME'/data/test-modify/parquet_modify_2/t3_table.parquet'
\set file_schema2 :PATH_FILENAME'/data/test-modify/parquet_modify_2/t4_table.parquet'
--Testcase 232:
CREATE FOREIGN TABLE t_table (
    v jsonb
)SERVER parquet_s3_srv OPTIONS (dirname :'dir', key_columns 'id', schemaless 'true');
--Testcase 233:
CREATE FOREIGN TABLE t_table_1 (
    v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'file_schema1', key_columns 'id', schemaless 'true');

--Testcase 234:
CREATE FOREIGN TABLE t_table_2 (
    v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'file_schema2', key_columns 'id', schemaless 'true');

--Testcase 235:
SELECT * FROM t_table ORDER BY (v->>'id')::int;
--Testcase 236:
INSERT INTO t_table VALUES ('{"id": 10, "a": "test"}');
--Testcase 237:
INSERT INTO t_table VALUES ('{"id": 20, "a": "test"}');
--Testcase 238:
SELECT * FROM t_table_1 ORDER BY (v->>'id')::int; -- new value inserted to t_table_1

--Testcase 239:
INSERT INTO t_table VALUES ('{"id": 30, "b": "test"}');
--Testcase 240:
SELECT * FROM t_table_2 ORDER BY (v->>'id')::int;  -- new value inserted to t_table_2
--Testcase 241:
SELECT * FROM t_table ORDER BY (v->>'id')::int;

-- Create new file to keep other file schema
--Testcase 242:
INSERT INTO t_table VALUES ('{"id": 40, "a": "foo", "b": "bar"}'); -- no file can keep this record
--Testcase 243:
SELECT * FROM t_table ORDER BY (v->>'id')::int;
--Testcase 244:
SELECT * FROM t_table_1 ORDER BY (v->>'id')::int; -- no new record inserted
--Testcase 245:
SELECT * FROM t_table_2 ORDER BY (v->>'id')::int; -- no new record inserted

--Testcase 246:
UPDATE t_table SET v = '{"a": "WEJO@"}' WHERE (v->>'id')::int = 10;
--Testcase 247:
SELECT * FROM t_table ORDER BY (v->>'id')::int;
--Testcase 248:
UPDATE t_table SET v = '{"a": "20"}' WHERE (v->>'id')::int = 20;
--Testcase 249:
SELECT * FROM t_table ORDER BY (v->>'id')::int;

--Testcase 250:
UPDATE t_table SET v = '{"b": "20"}' WHERE (v->>'id')::int = 30;
--Testcase 251:
SELECT * FROM t_table ORDER BY (v->>'id')::int;

--Testcase 252:
DELETE FROM t_table WHERE (v->>'id')::int = 10;
--Testcase 253:
SELECT * FROM t_table ORDER BY (v->>'id')::int;

-- clean up
--Testcase 254:
DELETE FROM t_table WHERE (v->>'id')::int > 10;

--Testcase 255:
DROP FOREIGN TABLE t_table;
--Testcase 256:
DROP FOREIGN TABLE t_table_1;
--Testcase 257:
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

--Testcase 258:
CREATE FOREIGN TABLE t_table (
  v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'files', key_columns 'id', schemaless 'true');

--Testcase 259:
CREATE FOREIGN TABLE t_table_1 (
  v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'file_schema1', key_columns 'id', schemaless 'true');

--Testcase 260:
CREATE FOREIGN TABLE t_table_2 (
  v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'file_schema2', key_columns 'id', schemaless 'true');

--Testcase 261:
SELECT * FROM t_table ORDER BY (v->>'id')::int;
--Testcase 262:
INSERT INTO t_table VALUES ('{"id": 10, "a": "test"}');
--Testcase 263:
INSERT INTO t_table VALUES ('{"id": 20, "a": "test"}');
--Testcase 264:
SELECT * FROM t_table_1 ORDER BY (v->>'id')::int; -- new value inserted to t_table_1

--Testcase 265:
INSERT INTO t_table VALUES ('{"id": 30, "a": "test"}');
--Testcase 266:
SELECT * FROM t_table_2 ORDER BY (v->>'id')::int;  -- new value inserted to t_table_2
--Testcase 267:
SELECT * FROM t_table ORDER BY (v->>'id')::int;

--Testcase 268:
INSERT INTO t_table VALUES ('{"id": 40, "a": "foo", "b": "bar"}'); -- should fail no file can keep this record
--Testcase 269:
SELECT * FROM t_table ORDER BY (v->>'id')::int; -- no new record inserted
--Testcase 270:
SELECT * FROM t_table_1 ORDER BY (v->>'id')::int; -- no new record inserted
--Testcase 271:
SELECT * FROM t_table_2 ORDER BY (v->>'id')::int; -- no new record inserted

--Testcase 272:
UPDATE t_table SET v = '{"a": "WEJO@"}' WHERE (v->>'id')::int = 10;
--Testcase 273:
SELECT * FROM t_table ORDER BY (v->>'id')::int;
--Testcase 274:
UPDATE t_table SET v = '{"a": "20"}' WHERE (v->>'id')::int = 20;
--Testcase 275:
SELECT * FROM t_table ORDER BY (v->>'id')::int;

--Testcase 276:
UPDATE t_table SET v = '{"b": "20"}' WHERE (v->>'id')::int = 30;
--Testcase 277:
SELECT * FROM t_table ORDER BY (v->>'id')::int;

--Testcase 278:
DELETE FROM t_table WHERE (v->>'id')::int = 10;
--Testcase 279:
SELECT * FROM t_table ORDER BY (v->>'id')::int;

-- clean up
--Testcase 280:
DELETE FROM t_table WHERE (v->>'id')::int > 10;

--Testcase 281:
DROP FOREIGN TABLE t_table;
--Testcase 282:
DROP FOREIGN TABLE t_table_1;
--Testcase 283:
DROP FOREIGN TABLE t_table_2;

--
-- Modification with foreign table include multi keys
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify/ft1_table.parquet'
--Testcase 284:
CREATE FOREIGN TABLE ft1_table (
  v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c2 c3', schemaless 'true');

--Testcase 285:
SELECT * FROM ft1_table;
--Testcase 286:
INSERT INTO ft1_table VALUES ('{"c1": 1, "c2": "foo", "c3": "2022-08-08 14:00:00"}');
--Testcase 287:
INSERT INTO ft1_table VALUES ('{"c1": 2, "c2": "baz", "c3": "2022-08-08 14:00:00"}');
--Testcase 288:
INSERT INTO ft1_table VALUES ('{"c1": 3, "c2": "foo", "c3": "2022-08-08 14:14:14"}');
--Testcase 289:
INSERT INTO ft1_table VALUES ('{"c1": 2, "c2": "baz", "c3": "2022-08-08 14:14:14"}');
--Testcase 290:
INSERT INTO ft1_table VALUES ('{"c1": 5, "c2": "foo", "c3": "2022-08-08 15:00:00"}');
--Testcase 291:
INSERT INTO ft1_table VALUES ('{"c1": 5, "c2": "baz", "c3": "2022-08-08 15:00:00"}');
--Testcase 292:
SELECT * FROM ft1_table;

--Testcase 293:
DELETE FROM ft1_table WHERE (v->>'c1')::int = 1;
--Testcase 294:
SELECT * FROM ft1_table;

--Testcase 295:
UPDATE ft1_table SET v = json_build_object('c2', v->>'c2' || '_UPDATE') WHERE (v->>'c1')::int = 5;
--Testcase 296:
SELECT * FROM ft1_table;

-- clean up
--Testcase 297:
DELETE FROM ft1_table;
--Testcase 298:
DROP FOREIGN TABLE ft1_table;

--
-- Modification with value type is LIST/MAP
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_4/ft2_table.parquet'
--Testcase 299:
CREATE FOREIGN TABLE ft2_table (
  v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'id', schemaless 'true');

--Testcase 300:
DELETE FROM ft2_table;
--Testcase 301:
INSERT INTO ft2_table VALUES ('{"id": 1, "c1": {"a": {}}, "c2": {"1": 2}, "c3": [], "c4": [], "c5": []}');
--Testcase 302:
INSERT INTO ft2_table VALUES ('{"id": 2, "c1": {"a": "aaa in bbb"}, "c2": {"a":1}, "c3": ["3.4", "6.7"], "c4": ["abc","abcde"], "c5": ["foobar"]}');

--Testcase 303:
SELECT * FROM ft2_table;

--Testcase 304:
UPDATE ft2_table SET v = '{"c3": [1.0, 2.0, 3.0]}' WHERE (v->>'id')::int = 1;
--Testcase 305:
UPDATE ft2_table SET v = '{"c5": ["name1", "name2", "name3"]}' WHERE (v->>'id')::int = 2;

--Testcase 306:
SELECT * FROM ft2_table;

--Testcase 307:
DELETE FROM ft2_table WHERE (v->>'id')::int = 1;
--Testcase 308:
SELECT * FROM ft2_table;
-- clean up
--Testcase 309:
DELETE FROM ft2_table;
--Testcase 310:
DROP FOREIGN TABLE ft2_table;

--
-- Test insert to new file: auto gen new file with format [dirpath]/[table_name]-[current_time].parquet
-- or  pointed by insert_file_selector option
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_5'
--Testcase 311:
CREATE FOREIGN TABLE ft_new (
  v jsonb
) SERVER parquet_s3_srv OPTIONS (dirname :'var', key_columns 'c1', schemaless 'true');

--Testcase 312:
SELECT * FROM ft_new;
--Testcase 313:
INSERT INTO ft_new VALUES ('{"c1": 1, "c2": "a"}');
--Testcase 314:
INSERT INTO ft_new VALUES ('{"c1": 2, "c2": "ajawe22A#AJFEkaef"}');
--Testcase 315:
INSERT INTO ft_new VALUES ('{"c1": 3, "c2": "24656565323"}');
--Testcase 316:
INSERT INTO ft_new VALUES ('{"c1": 4, "c2": "-1209012"}');
--Testcase 317:
INSERT INTO ft_new VALUES ('{"c1": 5, "c2": "a"}');

--Testcase 318:
SELECT * FROM ft_new;

--Testcase 319:
UPDATE ft_new SET v = '{"c2": "oneonwe"}' WHERE (v->>'c1')::int > 3;
--Testcase 320:
SELECT * FROM ft_new;

--Testcase 321:
DELETE FROM ft_new WHERE (v->>'c1')::int > 4;
--Testcase 322:
SELECT * FROM ft_new;

-- clean up
--Testcase 323:
DELETE FROM ft_new;
--Testcase 324:
DROP FOREIGN TABLE ft_new;

-- created new file pointed by insert_file_selector option
\set new_file :PATH_FILENAME'/data/test-modify/parquet_modify_5/new_file.parquet'
--Testcase 325:
CREATE FUNCTION selector(v jsonb, dirname text)
RETURNS TEXT AS
$$
    SELECT dirname || '/new_file.parquet';
$$
LANGUAGE SQL;
--Testcase 326:
CREATE FOREIGN TABLE ft_new (
  v jsonb
) SERVER parquet_s3_srv OPTIONS (insert_file_selector 'selector(v, dirname)', dirname :'var', key_columns 'c1', schemaless 'true');

-- new file was not created
--Testcase 327:
CREATE FOREIGN TABLE new_file (
    v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'new_file', key_columns 'c1', schemaless 'true'); -- should fail

--Testcase 328:
INSERT INTO ft_new VALUES ('{"c1": 1, "c2": "b"}');
--Testcase 329:
INSERT INTO ft_new VALUES ('{"c1": 2, "c2": "_@#AJFEkaef"}');
--Testcase 330:
INSERT INTO ft_new VALUES ('{"c1": 3, "c2": "2_!(#)"}');
--Testcase 331:
INSERT INTO ft_new VALUES ('{"c1": 4, "c2": "anu"}');
--Testcase 332:
INSERT INTO ft_new VALUES ('{"c1": 5, "c2": "swrr"}');
--Testcase 333:
SELECT * FROM ft_new ORDER BY (v->>'c1')::int;

--Testcase 334:
UPDATE ft_new SET v = '{"c2": "oneonwe"}' WHERE (v->>'c1')::int > 3;
--Testcase 335:
SELECT * FROM ft_new ORDER BY (v->>'c1')::int;

--Testcase 336:
DELETE FROM ft_new WHERE (v->>'c1')::int > 4;
--Testcase 337:
SELECT * FROM ft_new ORDER BY (v->>'c1')::int;

-- new file was created
--Testcase 338:
CREATE FOREIGN TABLE new_file (
    v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'new_file', key_columns 'c1', schemaless 'true'); -- OK

--Testcase 339:
SELECT * FROM new_file;

--Testcase 340:
DELETE FROM ft_new;
--Testcase 341:
DROP FOREIGN TABLE new_file;
--Testcase 342:
DROP FOREIGN TABLE ft_new;
--Testcase 343:
DROP FUNCTION selector;

-- Raise error when not specify dirname option, and no schema match
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_5/new_file.parquet'
--Testcase 344:
CREATE FOREIGN TABLE ft_new (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c1', schemaless 'true');

--Testcase 345:
INSERT INTO ft_new VALUES ('{"c1": 1, "c2": 11}');
--Testcase 346:
INSERT INTO ft_new VALUES ('{"c1": 2, "c2": 12}');
--Testcase 347:
SELECT * FROM ft_new;
--Testcase 348:
INSERT INTO ft_new VALUES (jsonb_build_object('c1', 3, 'c2', 13, 'c3', date('2001-02-02'))); -- should fail
--Testcase 349:
UPDATE ft_new SET v = '{"c3": "2001-02-05"}'; -- should fail
--Testcase 350:
SELECT * FROM ft_new;

--Testcase 351:
DELETE FROM ft_new;
--Testcase 352:
DROP FOREIGN TABLE ft_new;

--
-- Test INSERT/UPDATE value with 'sorted' option
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_int.parquet'
--Testcase 353:
CREATE FOREIGN TABLE ft_sorted_int (
  v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'var', sorted 'c1', key_columns 'c1', schemaless 'true');

--Testcase 354:
INSERT INTO ft_sorted_int VALUES ('{"c1": 1, "c2": "one"}');
--Testcase 355:
INSERT INTO ft_sorted_int VALUES ('{"c1": 2, "c2": "two"}');
--Testcase 356:
INSERT INTO ft_sorted_int VALUES ('{"c1": 10, "c2": "ten"}');
--Testcase 357:
INSERT INTO ft_sorted_int VALUES ('{"c1": 100, "c2": "one hundred"}');
--Testcase 358:
INSERT INTO ft_sorted_int VALUES ('{"c1": 20, "c2": "twenty"}');
--Testcase 359:
SELECT * FROM ft_sorted_int;

--Testcase 360:
UPDATE ft_sorted_int SET v = '{"c1": 1000}' WHERE v->>'c2' = 'one';
--Testcase 361:
SELECT * FROM ft_sorted_int;

--Testcase 362:
DELETE FROM ft_sorted_int WHERE (v->>'c1')::int = 10;
--Testcase 363:
SELECT * FROM ft_sorted_int;

-- clean up
--Testcase 364:
DELETE FROM ft_sorted_int;
--Testcase 365:
DROP FOREIGN TABLE ft_sorted_int;

-- test with un-support sorted column type
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_text.parquet'
--Testcase 366:
CREATE FOREIGN TABLE ft_sorted_text (
  v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'var', key_columns 'c1', schemaless 'true');

--Testcase 367:
INSERT INTO ft_sorted_text VALUES ('{"c1": 1, "c2": "one"}');
--Testcase 368:
INSERT INTO ft_sorted_text VALUES ('{"c1": 2, "c2": "two"}');
--Testcase 369:
INSERT INTO ft_sorted_text VALUES ('{"c1": 10, "c2": "ten"}');
--Testcase 370:
ALTER FOREIGN TABLE ft_sorted_text OPTIONS (sorted 'c2');
--Testcase 371:
INSERT INTO ft_sorted_text VALUES ('{"c1": 100, "c2": "one hundred"}'); -- should fail
--Testcase 372:
INSERT INTO ft_sorted_text VALUES ('{"c1": 20, "c2": "twenty"}'); -- should fail
--Testcase 373:
SELECT * FROM ft_sorted_text;

--Testcase 374:
UPDATE ft_sorted_text SET v = '{"c1": 1000}' WHERE v->>'c2' = 'one'; -- should fail
--Testcase 375:
SELECT * FROM ft_sorted_text;

--Testcase 376:
DELETE FROM ft_sorted_text WHERE (v->>'c1')::int = 10; -- OK
--Testcase 377:
SELECT * FROM ft_sorted_text;

-- clean up
--Testcase 378:
DELETE FROM ft_sorted_text;
--Testcase 379:
DROP FOREIGN TABLE ft_sorted_text;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_date.parquet'
--Testcase 380:
CREATE FOREIGN TABLE ft_sorted_date (v jsonb)
SERVER parquet_s3_srv OPTIONS (filename :'var', sorted 'c2', key_columns 'c1', schemaless 'true');

-- clean-up first
--Testcase 381:
DELETE FROM ft_sorted_date;

--Testcase 382:
INSERT INTO ft_sorted_date VALUES (jsonb_build_object('c1', 1, 'c2', date('2020-01-01')));
--Testcase 383:
INSERT INTO ft_sorted_date VALUES (jsonb_build_object('c1', 2, 'c2', date('2000-01-01')));
--Testcase 384:
INSERT INTO ft_sorted_date VALUES (jsonb_build_object('c1', 3, 'c2', date('2021-09-01')));
--Testcase 385:
INSERT INTO ft_sorted_date VALUES (jsonb_build_object('c1', 4, 'c2', date('1990-11-11')));
--Testcase 386:
INSERT INTO ft_sorted_date VALUES (jsonb_build_object('c1', 5, 'c2', date('2022-07-07')));
--Testcase 387:
SELECT * FROM ft_sorted_date;

--Testcase 388:
UPDATE ft_sorted_date SET v = '{"c2": "2022-01-01"}' WHERE (v->>'c2')::date = '2000-01-01';
--Testcase 389:
SELECT * FROM ft_sorted_date;

--Testcase 390:
DELETE FROM ft_sorted_date WHERE (v->>'c1')::int = 1;
--Testcase 391:
SELECT * FROM ft_sorted_date;

-- clean up
--Testcase 392:
DELETE FROM ft_sorted_date;
--Testcase 393:
DROP FOREIGN TABLE ft_sorted_date;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_time.parquet'
--Testcase 394:
CREATE FOREIGN TABLE ft_sorted_time (
  v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'var', sorted 'c2', key_columns 'c1', schemaless 'true');

-- clean-up first
--Testcase 395:
DELETE FROM ft_sorted_time;
--Testcase 396:
INSERT INTO ft_sorted_time VALUES (jsonb_build_object('c1', 1, 'c2', timestamp '2020-01-01 00:00:00'));
--Testcase 397:
INSERT INTO ft_sorted_time VALUES (jsonb_build_object('c1', 2, 'c2', timestamp '2000-01-01 10:00:00'));
--Testcase 398:
INSERT INTO ft_sorted_time VALUES (jsonb_build_object('c1', 3, 'c2', timestamp '2021-09-01 20:00:00'));
--Testcase 399:
INSERT INTO ft_sorted_time VALUES (jsonb_build_object('c1', 4, 'c2', timestamp '1990-11-11 08:00:00'));
--Testcase 400:
INSERT INTO ft_sorted_time VALUES (jsonb_build_object('c1', 5, 'c2', timestamp '2022-07-07 07:00:00'));
--Testcase 401:
SELECT * FROM ft_sorted_time;

--Testcase 402:
UPDATE ft_sorted_time SET v = '{"c1": 10}' WHERE (v->>'c2')::timestamp = '2020-01-01T00:00:00';
--Testcase 403:
SELECT * FROM ft_sorted_time;

--Testcase 404:
DELETE FROM ft_sorted_time WHERE (v->>'c1')::int = 1;
--Testcase 405:
SELECT * FROM ft_sorted_time;

-- clean up
--Testcase 406:
DELETE FROM ft_sorted_time;
--Testcase 407:
DROP FOREIGN TABLE ft_sorted_time;

--
-- un-supported column data type, multiple sorted columns
--
\set var :PATH_FILENAME'/data/test-modify/parquet_modify_7/'
--Testcase 408:
CREATE FOREIGN TABLE ft_sorted_types (v jsonb)
SERVER parquet_s3_srv OPTIONS (dirname :'var', sorted 'c2', key_columns 'c1', schemaless 'true');
--Testcase 409:
INSERT INTO ft_sorted_types VALUES ('{"c1": 1, "c2": "foo"}'); -- should fail
--Testcase 410:
DROP FOREIGN TABLE ft_sorted_types;

--Testcase 411:
CREATE FOREIGN TABLE ft_sorted_types (v jsonb)
SERVER parquet_s3_srv OPTIONS (dirname :'var', sorted 'c4', key_columns 'c1', schemaless 'true');
--Testcase 412:
INSERT INTO ft_sorted_types VALUES ('{"c1": 1, "c4": {"a": 1}}'); -- should fail
--Testcase 413:
DROP FOREIGN TABLE ft_sorted_types;

--Testcase 414:
CREATE FOREIGN TABLE ft_sorted_types (v jsonb)
SERVER parquet_s3_srv OPTIONS (dirname :'var', sorted 'c5', key_columns 'c1', schemaless 'true');
--Testcase 415:
INSERT INTO ft_sorted_types VALUES ('{"c1": 1, "c5": [1, 2]}'); -- should fail
--Testcase 416:
DROP FOREIGN TABLE ft_sorted_types;

--Testcase 417:
CREATE FUNCTION selector(v JSONB, dirname text)
RETURNS TEXT AS
$$
    SELECT CASE
           WHEN (v->>'c1')::int % 2 = 0 THEN dirname || 'ft_sorted_1.parquet'
           ELSE dirname || 'ft_sorted_2.parquet'
           END;
$$
LANGUAGE SQL;

--Testcase 418:
CREATE FOREIGN TABLE ft_sorted_types (v jsonb)
SERVER parquet_s3_srv OPTIONS (dirname :'var', sorted 'c1 c3', key_columns 'c1', schemaless 'true', insert_file_selector 'selector(v , dirname)');
--Testcase 419:
INSERT INTO ft_sorted_types VALUES ('{"c1": 1, "c2": "foo", "c3": 1.0, "c4": {"a": 1}, "c5": [1, 2]}');
--Testcase 420:
INSERT INTO ft_sorted_types VALUES ('{"c1": 3, "c2": "foo", "c3": 20.0, "c4": {"a": 2}, "c5": [12, 22]}');
--Testcase 421:
INSERT INTO ft_sorted_types VALUES ('{"c1": 5, "c2": "foo", "c3": 40.0, "c4": {"a": 4}, "c5": [14, 24]}');
--Testcase 422:
INSERT INTO ft_sorted_types VALUES ('{"c1": 5, "c2": "foo", "c3": 30.0, "c4": {"a": 5}, "c5": [15, 25]}');
--Testcase 423:
INSERT INTO ft_sorted_types VALUES ('{"c1": 7, "c2": "foo", "c3": 55.5, "c4": {"a": 6}, "c5": [16, 26]}');

-- should failed, can not find position
--Testcase 424:
INSERT INTO ft_sorted_types VALUES ('{"c1": 11, "c2": "foo", "c3": 10.5, "c4": {"a": 6}, "c5": [16, 26]}');

--Testcase 425:
INSERT INTO ft_sorted_types VALUES ('{"c1": 2, "c2": "foo", "c3": 10.0, "c4": {"a": 10}, "c5": [13, 23]}');
--Testcase 426:
INSERT INTO ft_sorted_types VALUES ('{"c1": 40, "c2": "foo", "c3": 100.2, "c4": {"a": 11}, "c5": [21, 32]}');
--Testcase 427:
INSERT INTO ft_sorted_types VALUES ('{"c1": 6, "c2": "foo", "c3": 60.2, "c4": {"a": 12}, "c5": [1, 32]}');
--Testcase 428:
INSERT INTO ft_sorted_types VALUES ('{"c1": 6, "c2": "foo", "c3": 14.2, "c4": {"a": 13}, "c5": [2, 32]}');
--Testcase 429:
INSERT INTO ft_sorted_types VALUES ('{"c1": 14, "c2": "foo", "c3": 72.2, "c4": {"a": 14}, "c5": [21, 2]}');

-- should failed, can not find position
--Testcase 430:
INSERT INTO ft_sorted_types VALUES ('{"c1": 18, "c2": "foo", "c3": 130.2, "c4": {"a": 15}, "c5": [1, 3]}');

--Testcase 431:
SELECT * FROM ft_sorted_types ORDER BY (v->>'c1')::float8;

--Testcase 432:
EXPLAIN VERBOSE
SELECT * FROM ft_sorted_types ORDER BY (v->>'c1')::float8, (v->>'c3')::float8;
--Testcase 433:
SELECT * FROM ft_sorted_types ORDER BY (v->>'c1')::float8, (v->>'c3')::float8;

--Testcase 434:
EXPLAIN VERBOSE
SELECT * FROM ft_sorted_types ORDER BY (v->>'c1')::float8;
--Testcase 435:
SELECT * FROM ft_sorted_types ORDER BY (v->>'c1')::float8;

-- clean up
--Testcase 436:
DELETE FROM ft_sorted_types;
--Testcase 437:
DROP FOREIGN TABLE ft_sorted_types;
--Testcase 438:
DROP FUNCTION selector;

--
--INSERT with user defined function
--
--Testcase 439:
CREATE FUNCTION selector(v jsonb, dirname text)
RETURNS TEXT AS
$$
    SELECT CASE
           WHEN (v->>'c1')::int % 2 = 0 THEN dirname || 'ft_sorted_int.parquet'
           ELSE dirname || 'ft_sorted_text.parquet'
           END;
$$
LANGUAGE SQL;

\set var :PATH_FILENAME'/data/test-modify/parquet_modify_6/'
\set file1 :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_int.parquet'
\set file2 :PATH_FILENAME'/data/test-modify/parquet_modify_6/ft_sorted_text.parquet'
--Testcase 440:
CREATE FOREIGN TABLE ft_user_defined (v jsonb)
SERVER parquet_s3_srv OPTIONS (insert_file_selector 'selector(v , dirname)', dirname :'var', sorted 'c1', key_columns 'c1', schemaless 'true');

--Testcase 441:
CREATE FOREIGN TABLE ft_user_defined_1 (
    v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'file1', sorted 'c1', key_columns 'c1', schemaless 'true');

--Testcase 442:
CREATE FOREIGN TABLE ft_user_defined_2 (
    v jsonb
) SERVER parquet_s3_srv OPTIONS (filename :'file2', sorted 'c1', key_columns 'c1', schemaless 'true');

--Testcase 443:
INSERT INTO ft_user_defined VALUES ('{"c1": 9, "c2": "aMC"}');
--Testcase 444:
INSERT INTO ft_user_defined VALUES ('{"c1": 11, "c2": "!_KWRN@QIEPAE"}');
--Testcase 445:
INSERT INTO ft_user_defined VALUES ('{"c1": 12, "c2": "JAVA"}');
--Testcase 446:
INSERT INTO ft_user_defined VALUES ('{"c1": 13, "c2": "awefrq3"}');

--Testcase 447:
SELECT * FROM ft_user_defined ORDER BY (v->>'c1')::float8;
--Testcase 448:
SELECT * FROM ft_user_defined_1 ORDER BY (v->>'c1')::float8;
--Testcase 449:
SELECT * FROM ft_user_defined_2 ORDER BY (v->>'c1')::float8;

-- clean up
--Testcase 450:
DELETE FROM ft_user_defined WHERE (v->>'c1')::int >= 9;
--Testcase 451:
DROP FOREIGN TABLE ft_user_defined;
--Testcase 452:
DROP FOREIGN TABLE ft_user_defined_1;
--Testcase 453:
DROP FOREIGN TABLE ft_user_defined_2;
--Testcase 454:
DROP FUNCTION selector;

--
-- test for both key and key_columns defined
--

\set var :PATH_FILENAME'/data/test-modify/parquet_modify_f'
--Testcase 455:
CREATE FOREIGN TABLE tmp_table (
    v JSONB OPTIONS (key 'true')
) SERVER parquet_s3_srv OPTIONS (dirname :'var', key_columns 'a', schemaless 'true');

--Testcase 456:
SELECT * FROM tmp_table;

-- in schemaless mode key option has no meaning
-- NULL check only for a column
--Testcase 457:
INSERT INTO tmp_table VALUES ('{"id" :null, "a": "c"}'); -- OK
-- should fail: key column must not be null
--Testcase 458:
INSERT INTO tmp_table VALUES ('{"id" :5, "a": null}');
--Testcase 459:
SELECT * FROM tmp_table;

-- clean up
--Testcase 460:
DELETE FROM tmp_table WHERE v->>'a' = 'c';
--Testcase 461:
DROP FOREIGN TABLE tmp_table;

-- IMPORT FOREIGN SCHEMA with key_columns option
\set var '\"':PATH_FILENAME'/data/test-modify/parquet_modify/"'
--Testcase 462:
CREATE SCHEMA tmp_schema;
IMPORT FOREIGN SCHEMA :var FROM SERVER parquet_s3_srv INTO tmp_schema OPTIONS (sorted 'c1', schemaless 'true', key_columns 'id c1 c2');
--Testcase 463:
\det+ tmp_schema.*
--Testcase 464:
\d tmp_schema.*
--Testcase 465:
DROP SCHEMA tmp_schema CASCADE;

--CLEAN
--Testcase 466:
DROP USER MAPPING FOR CURRENT_USER SERVER parquet_s3_srv;
--Testcase 467:
DROP SERVER parquet_s3_srv CASCADE;
--Testcase 468:
DROP EXTENSION parquet_s3_fdw CASCADE;

-- revert data
\! rm -rf /tmp/data_local/data/test-modify || true
\! cp -a data/ /tmp/data_local/
