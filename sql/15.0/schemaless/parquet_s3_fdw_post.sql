-- ===================================================================
-- create FDW objects
-- ===================================================================

--Testcase 1:
CREATE EXTENSION parquet_s3_fdw;

--Testcase 2:
CREATE SERVER parquet_s3_srv FOREIGN DATA WRAPPER parquet_s3_fdw
      :USE_MINIO;
--Testcase 3:
CREATE SERVER parquet_s3_srv_2 FOREIGN DATA WRAPPER parquet_s3_fdw
      :USE_MINIO;
--Testcase 4:
CREATE SERVER parquet_s3_srv_3 FOREIGN DATA WRAPPER parquet_s3_fdw
      :USE_MINIO;

--Testcase 5:
CREATE USER MAPPING FOR CURRENT_USER SERVER parquet_s3_srv :USER_PASSWORD;
--Testcase 6:
CREATE USER MAPPING FOR CURRENT_USER SERVER parquet_s3_srv_2 :USER_PASSWORD;
--Testcase 7:
CREATE USER MAPPING FOR public SERVER parquet_s3_srv_3 :USER_PASSWORD;


-- ===================================================================
-- create objects used through FDW loopback server
-- ===================================================================
--Testcase 8:
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');

--Testcase 9:
CREATE SCHEMA "S 1";
\set var '\"':PATH_FILENAME'\/ported_postgres\"'
IMPORT FOREIGN SCHEMA :var FROM SERVER parquet_s3_srv INTO "S 1" OPTIONS (sorted 'c1', schemaless 'true');

-- -- Disable autovacuum for these tables to avoid unexpected effects of that
-- ALTER TABLE "S 1"."T1" SET (autovacuum_enabled = 'false');
-- ALTER TABLE "S 1"."T2" SET (autovacuum_enabled = 'false');
-- ALTER TABLE "S 1"."T3" SET (autovacuum_enabled = 'false');
-- ALTER TABLE "S 1"."T4" SET (autovacuum_enabled = 'false');

-- ANALYZE "S 1"."T1";
-- ANALYZE "S 1"."T2";
-- ANALYZE "S 1"."T3";
-- ANALYZE "S 1"."T4";

-- ===================================================================
-- create foreign tables
-- ===================================================================
\set var :PATH_FILENAME'/ported_postgres/ft1.parquet'
--Testcase 10:
CREATE FOREIGN TABLE ft1 (
	c0 int,
  v jsonb
) SERVER parquet_s3_srv
OPTIONS (filename :'var', key_columns 'c1', sorted 'c1', schemaless 'true');
--Testcase 446:
ALTER FOREIGN TABLE ft1 DROP COLUMN c0;

\set var :PATH_FILENAME'/ported_postgres/ft1.parquet'
--Testcase 11:
CREATE FOREIGN TABLE ft2 (
  v jsonb,
	cx int
) SERVER parquet_s3_srv
OPTIONS (filename :'var', key_columns 'c1', sorted 'c1', schemaless 'true');
--Testcase 447:
ALTER FOREIGN TABLE ft2 DROP COLUMN cx;

\set var :PATH_FILENAME'/ported_postgres/T3.parquet'
--Testcase 12:
CREATE FOREIGN TABLE ft4 (
  v jsonb
) SERVER parquet_s3_srv
OPTIONS (filename :'var', key_columns 'c1', sorted 'c1', schemaless 'true');

\set var :PATH_FILENAME'/ported_postgres/T4.parquet'
--Testcase 13:
CREATE FOREIGN TABLE ft5 (
	v jsonb
) SERVER parquet_s3_srv
OPTIONS (filename :'var', key_columns 'c1', sorted 'c1', schemaless 'true');

\set var :PATH_FILENAME'/ported_postgres/T4.parquet'
--Testcase 14:
CREATE FOREIGN TABLE ft6 (
	v jsonb
) SERVER parquet_s3_srv_2
OPTIONS (filename :'var', key_columns 'c1', sorted 'c1', schemaless 'true');

\set var :PATH_FILENAME'/ported_postgres/T4.parquet'
--Testcase 15:
CREATE FOREIGN TABLE ft7 (
	v jsonb
) SERVER parquet_s3_srv_3
OPTIONS (filename :'var', key_columns 'c1', sorted 'c1', schemaless 'true');
-- -- ===================================================================
-- -- tests for validator
-- -- ===================================================================
-- -- requiressl and some other parameters are omitted because
-- -- valid values for them depend on configure options
-- ALTER SERVER testserver1 OPTIONS (
-- 	use_remote_estimate 'false',
-- 	updatable 'true',
-- 	fdw_startup_cost '123.456',
-- 	fdw_tuple_cost '0.123',
-- 	service 'value',
-- 	connect_timeout 'value',
-- 	dbname 'value',
-- 	host 'value',
-- 	hostaddr 'value',
-- 	port 'value',
-- 	--client_encoding 'value',
-- 	application_name 'value',
-- 	--fallback_application_name 'value',
-- 	keepalives 'value',
-- 	keepalives_idle 'value',
-- 	keepalives_interval 'value',
-- 	tcp_user_timeout 'value',
-- 	-- requiressl 'value',
-- 	sslcompression 'value',
-- 	sslmode 'value',
-- 	sslcert 'value',
-- 	sslkey 'value',
-- 	sslrootcert 'value',
-- 	sslcrl 'value',
-- 	--requirepeer 'value',
-- 	krbsrvname 'value',
-- 	gsslib 'value'
-- 	--replication 'value'
-- );

-- -- Error, invalid list syntax
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo; bar');

-- -- OK but gets a warning
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo, bar');
-- ALTER SERVER testserver1 OPTIONS (DROP extensions);

-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (DROP user, DROP password);

-- -- Attempt to add a valid option that's not allowed in a user mapping
-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (ADD sslmode 'require');

-- -- But we can add valid ones fine
-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (ADD sslpassword 'dummy');

-- -- Ensure valid options we haven't used in a user mapping yet are
-- -- permitted to check validation.
-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (ADD sslkey 'value', ADD sslcert 'value');

-- ALTER FOREIGN TABLE ft1 OPTIONS (schema_name 'S 1', table_name 'T 1');
-- ALTER FOREIGN TABLE ft2 OPTIONS (schema_name 'S 1', table_name 'T 1');
-- ALTER FOREIGN TABLE ft1 ALTER COLUMN v->>'c1' OPTIONS (column_name 'C 1');
-- ALTER FOREIGN TABLE ft2 ALTER COLUMN v->>'c1' OPTIONS (column_name 'C 1');
-- \det+

-- Test that alteration of server options causes reconnection
-- Remote's errors might be non-English, so hide them to ensure stable results
\set VERBOSITY terse
--Testcase 16:
SELECT v->>'c3' as c3, v->>'c5' as c5 FROM ft1 ORDER BY v->>'c3', (v->>'c1')::int8 LIMIT 1;  -- should work
--Testcase 448:
ALTER SERVER parquet_s3_srv OPTIONS (SET use_minio 'false');
--Testcase 17:
SELECT v->>'c3' as c3, v->>'c5' as c5 FROM ft1 ORDER BY v->>'c3', (v->>'c1')::int8 LIMIT 1;  -- should fail if only when we use minio/s3. With local file, option use_minio is useless.
DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER parquet_s3_srv
            OPTIONS (SET use_minio 'true')$$;
    END;
$d$;
--Testcase 18:
SELECT v->>'c3' as c3, v->>'c5' as c5 FROM ft1 ORDER BY v->>'c3', (v->>'c1')::int8 LIMIT 1;  -- should work again

-- Test that alteration of user mapping options causes reconnection
--Testcase 449:
ALTER USER MAPPING FOR CURRENT_USER SERVER parquet_s3_srv
  OPTIONS (SET user 'no such user');
--Testcase 19:
SELECT v->>'c3' as c3, v->>'c5' as c5 FROM ft1 ORDER BY v->>'c3', (v->>'c1')::int8 LIMIT 1;  -- should fail if only when we use minio/s3. With local file, option user is useless.
--Testcase 450:
ALTER USER MAPPING FOR CURRENT_USER SERVER parquet_s3_srv
  OPTIONS (SET user 'minioadmin');
--Testcase 20:
SELECT v->>'c3' as c3, v->>'c5' as c5 FROM ft1 ORDER BY v->>'c3', (v->>'c1')::int8 LIMIT 1;  -- should work again
\set VERBOSITY default

-- Now we should be able to run ANALYZE.
-- To exercise multiple code paths, we use local stats on ft1
-- and remote-estimate mode on ft2.
-- ANALYZE ft1;
-- ALTER FOREIGN TABLE ft2 OPTIONS (use_remote_estimate 'true');

-- ===================================================================
-- test error case for create publication on foreign table
-- ===================================================================
--Testcase 446:
CREATE PUBLICATION testpub_ftbl FOR TABLE ft1;  -- should fail

-- ===================================================================
-- simple queries
-- ===================================================================
-- single table without alias
--Testcase 21:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY v->>'c3', (v->>'c1')::int8 OFFSET 100 LIMIT 10;
--Testcase 22:
SELECT * FROM ft1 ORDER BY v->>'c3', (v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- single table with alias - also test that tableoid sort is not pushed to remote side
--Testcase 23:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8, t1.tableoid OFFSET 100 LIMIT 10;
--Testcase 24:
SELECT * FROM ft1 t1 ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8, t1.tableoid OFFSET 100 LIMIT 10;
-- whole-row reference
--Testcase 25:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1 FROM ft1 t1 ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- parquet_s3_fdw only fill slot attributes if column was referred in targetlist or clauses. In other cases mark attribute as NULL.
--Testcase 26:
SELECT t1 FROM ft1 t1 ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- empty result
--Testcase 27:
SELECT * FROM ft1 WHERE false;
-- with WHERE clause
--Testcase 28:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = 101 AND (t1.v->>'c6')::int = '1' AND (t1.v->>'c7')::int >= '1';
--Testcase 29:
SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = 101 AND (t1.v->>'c6')::int = '1' AND (t1.v->>'c7')::int >= '1';
-- with FOR UPDATE/SHARE
--Testcase 30:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (v->>'c1')::int8 = 101 FOR UPDATE;
--Testcase 31:
SELECT * FROM ft1 t1 WHERE (v->>'c1')::int8 = 101 FOR UPDATE;
--Testcase 32:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (v->>'c1')::int8 = 102 FOR SHARE;
--Testcase 33:
SELECT * FROM ft1 t1 WHERE (v->>'c1')::int8 = 102 FOR SHARE;
-- aggregate
--Testcase 34:
SELECT COUNT(*) FROM ft1 t1;
-- subquery
--Testcase 35:
SELECT * FROM ft1 t1 WHERE t1.v->>'c3' IN (SELECT v->>'c3' FROM ft2 t2 WHERE (v->>'c1')::int8 <= 10) ORDER BY (v->>'c1')::int8;
-- subquery+MAX
--Testcase 36:
SELECT * FROM ft1 t1 WHERE t1.v->>'c3' = (SELECT MAX(v->>'c3') FROM ft2 t2) ORDER BY (v->>'c1')::int8;
-- used in CTE
--Testcase 37:
WITH t1 AS (SELECT * FROM ft1 WHERE (v->>'c1')::int8 <= 10) SELECT (t2.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, t2.v->>'c3' as c3, t2.v->>'c5' as c5 FROM t1, ft2 t2 WHERE (t1.v->>'c1')::int8 = (t2.v->>'c1')::int8 ORDER BY (t1.v->>'c1')::int8;
-- fixed values
--Testcase 38:
SELECT 'fixed', NULL FROM ft1 t1 WHERE (v->>'c1')::int8 = 1;
-- Test forcing the remote server to produce sorted data for a merge join.
--Testcase 451:
SET enable_hashjoin TO false;
--Testcase 452:
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 39:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft2 t1 JOIN "S 1"."T1" t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) OFFSET 100 LIMIT 10;
--Testcase 40:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft2 t1 JOIN "S 1"."T1" t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) OFFSET 100 LIMIT 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 41:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft2 t1 LEFT JOIN "S 1"."T1" t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) OFFSET 100 LIMIT 10;
--Testcase 42:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft2 t1 LEFT JOIN "S 1"."T1" t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) OFFSET 100 LIMIT 10;
-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.
--Testcase 43:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT (t1.v->>'c1')::int8 as c1 FROM "S 1"."T1" t1 left join ft1 t2 join ft2 t3 on ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) on ((t3.v->>'c1')::int8 = (t1.v->>'c1')::int8) OFFSET 100 LIMIT 10;
--Testcase 44:
SELECT (t1.v->>'c1')::int8 as c1 FROM "S 1"."T1" t1 left join ft1 t2 join ft2 t3 on ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) on ((t3.v->>'c1')::int8 = (t1.v->>'c1')::int8) OFFSET 100 LIMIT 10;
-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.
--Testcase 45:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1, (t3.v->>'c1')::int8 as c1 FROM "S 1"."T1" t1 left join ft1 t2 full join ft2 t3 on ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) on ((t3.v->>'c1')::int8 = (t1.v->>'c1')::int8) OFFSET 100 LIMIT 10;
--Testcase 46:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1, (t3.v->>'c1')::int8 as c1 FROM "S 1"."T1" t1 left join ft1 t2 full join ft2 t3 on ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) on ((t3.v->>'c1')::int8 = (t1.v->>'c1')::int8) OFFSET 100 LIMIT 10;
-- Test similar to above with all full outer joins
--Testcase 47:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1, (t3.v->>'c1')::int8 as c1 FROM "S 1"."T1" t1 full join ft1 t2 full join ft2 t3 on ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) on ((t3.v->>'c1')::int8 = (t1.v->>'c1')::int8) OFFSET 100 LIMIT 10;
--Testcase 48:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1, (t3.v->>'c1')::int8 as c1 FROM "S 1"."T1" t1 full join ft1 t2 full join ft2 t3 on ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) on ((t3.v->>'c1')::int8 = (t1.v->>'c1')::int8) OFFSET 100 LIMIT 10;
--Testcase 453:
RESET enable_hashjoin;
--Testcase 454:
RESET enable_nestloop;

-- Test executing assertion in estimate_path_cost_size() that makes sure that
-- retrieved_rows for foreign rel re-used to cost pre-sorted foreign paths is
-- a sensible value even when the rel has tuples=0
\set var :PATH_FILENAME'/ported_postgres/loct_empty.parquet'
--Testcase 49:
CREATE FOREIGN TABLE ft_empty (v jsonb)
  SERVER parquet_s3_srv OPTIONS (filename :'var', sorted 'c1', schemaless 'true');
-- ANALYZE ft_empty;
--Testcase 50:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_empty ORDER BY (v->>'c1')::int8;

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================
--Testcase 51:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = 1;         -- Var, OpExpr(b), Const
--Testcase 52:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = 100 AND (t1.v->>'c2')::int = 0; -- BoolExpr
--Testcase 53:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (v->>'c1')::int8 IS NULL;        -- NullTest
--Testcase 54:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (v->>'c1')::int8 IS NOT NULL;    -- NullTest
--Testcase 55:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs((v->>'c1')::int8), 0) = 1; -- FuncExpr
--Testcase 56:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (v->>'c1')::int8 = -(v->>'c1')::int8;          -- OpExpr(l)
--Testcase 57:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE ((v->>'c1')::int8 IS NOT NULL) IS DISTINCT FROM ((v->>'c1')::int8 IS NOT NULL); -- DistinctExpr
--Testcase 58:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (v->>'c1')::int8 = ANY(ARRAY[(v->>'c2')::int, 1, (v->>'c1')::int8 + 0]); -- ScalarArrayOpExpr
--Testcase 59:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (v->>'c1')::int8 = (ARRAY[(v->>'c1')::int8,(v->>'c2')::int,3])[1]; -- SubscriptingRef
--Testcase 60:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE v->>'c6' = E'foo''s\\bar';  -- check special chars
--Testcase 61:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE v->>'c8' = 'foo';  -- can't be sent to remote
-- parameterized remote path for foreign table
--Testcase 62:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM "S 1"."T1" a, ft2 b WHERE (a.v->>'c1')::int8 = 47 AND (b.v->>'c1')::int8 = (a.v->>'c2')::int8;
--Testcase 63:
SELECT * FROM ft2 a, ft2 b WHERE (a.v->>'c1')::int8 = 47 AND (b.v->>'c1')::int8 = (a.v->>'c2')::int8;
-- check both safe and unsafe join conditions
--Testcase 64:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft2 a, ft2 b
  WHERE (a.v->>'c2')::int = 6 AND (b.v->>'c1')::int8 = (a.v->>'c1')::int8 AND a.v->>'c8' = 'foo' AND b.v->>'c7' = upper(a.v->>'c7');
--Testcase 65:s
SELECT * FROM ft2 a, ft2 b
WHERE (a.v->>'c2')::int = 6 AND (b.v->>'c1')::int8 = (a.v->>'c1')::int8 AND a.v->>'c8' = 'foo' AND b.v->>'c7' = upper(a.v->>'c7');
-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
--Testcase 66:
SELECT * FROM ft1 WHERE (v->>'c1')::int8 = ANY (ARRAY(SELECT (v->>'c1')::int8 FROM ft2 WHERE (v->>'c1')::int8 < 5));
--Testcase 67:
SELECT * FROM ft2 WHERE (v->>'c1')::int8 = ANY (ARRAY(SELECT (v->>'c1')::int8 FROM ft1 WHERE (v->>'c1')::int8 < 5));
-- we should not push order by clause with volatile expressions or unsafe
-- collations
--Testcase 68:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY (ft2.v->>'c1')::int8, random();
--Testcase 69:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY (ft2.v->>'c1')::int8, ft2.v->>'c3' collate "C";

-- user-defined operator/function
--Testcase 70:
CREATE FUNCTION parquet_s3_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
--Testcase 71:
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);

-- built-in operators and functions can be shipped for remote execution
--Testcase 72:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = abs((t1.v->>'c2')::int);
--Testcase 73:
SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = abs((t1.v->>'c2')::int);
--Testcase 74:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = (t1.v->>'c2')::int;
--Testcase 75:
SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = (t1.v->>'c2')::int;

-- by default, user-defined ones cannot
--Testcase 76:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = parquet_s3_fdw_abs((t1.v->>'c2')::int);
--Testcase 77:
SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = parquet_s3_fdw_abs((t1.v->>'c2')::int);
--Testcase 78:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int === (t1.v->>'c2')::int;
--Testcase 79:
SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int === (t1.v->>'c2')::int;

-- ORDER BY can be shipped, though
--Testcase 80:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int === (t1.v->>'c2')::int order by (t1.v->>'c2')::int limit 1;
--Testcase 81:
SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int === (t1.v->>'c2')::int order by (t1.v->>'c2')::int limit 1;

-- but let's put them in an extension ...
--Testcase 455:
ALTER EXTENSION parquet_s3_fdw ADD FUNCTION parquet_s3_fdw_abs(int);
--Testcase 456:
ALTER EXTENSION parquet_s3_fdw ADD OPERATOR === (int, int);
--Testcase 457:
ALTER SERVER parquet_s3_srv OPTIONS (ADD extensions 'parquet_s3_fdw');

-- ... now they can be shipped
--Testcase 82:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = parquet_s3_fdw_abs((t1.v->>'c2')::int);
--Testcase 83:
SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = parquet_s3_fdw_abs((t1.v->>'c2')::int);
--Testcase 84:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int === (t1.v->>'c2')::int;
--Testcase 85:
SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int === (t1.v->>'c2')::int;

-- and both ORDER BY and LIMIT can be shipped
--Testcase 86:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int === (t1.v->>'c2')::int order by (t1.v->>'c2')::int limit 1;
--Testcase 87:
SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int === (t1.v->>'c2')::int order by (t1.v->>'c2')::int limit 1;

-- Test CASE pushdown
-- Parquet_s3_fdw not support CASE pushdown
--Testcase 458:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT v->>'c1', v->>'c2',v->>'c3' FROM ft2 WHERE CASE WHEN (v->>'c1')::int > 990 THEN (v->>'c1')::int END < 1000 ORDER BY (v->>'c1')::int;
--Testcase 459:
SELECT v->>'c1', v->>'c2',v->>'c3' FROM ft2 WHERE CASE WHEN (v->>'c1')::int > 990 THEN (v->>'c1')::int END < 1000 ORDER BY (v->>'c1')::int;

-- Nested CASE
--Testcase 460:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT v->>'c1', v->>'c2',v->>'c3' FROM ft2 WHERE CASE CASE WHEN (v->>'c2')::int > 0 THEN (v->>'c2')::int END WHEN 100 THEN 601 WHEN (v->>'c2')::int THEN (v->>'c2')::int ELSE 0 END > 600 ORDER BY (v->>'c1')::int;
--Testcase 461:
SELECT v->>'c1', v->>'c2',v->>'c3' FROM ft2 WHERE CASE CASE WHEN (v->>'c2')::int > 0 THEN (v->>'c2')::int END WHEN 100 THEN 601 WHEN (v->>'c2')::int THEN (v->>'c2')::int ELSE 0 END > 600 ORDER BY (v->>'c1')::int;

-- CASE arg WHEN
--Testcase 462:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE (v->>'c1')::int > (CASE mod((v->>'c1')::int, 4) WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- CASE cannot be pushed down because of unshippable arg clause
--Testcase 463:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE (v->>'c1')::int > (CASE random()::integer WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- these are shippable
--Testcase 464:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE (v->>'c6')::text WHEN 'foo' THEN true ELSE (v->>'c3')::text < 'bar' END;
--Testcase 465:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE (v->>'c3')::text WHEN (v->>'c6')::text THEN true ELSE (v->>'c3')::text < 'bar' END;

-- but this is not because of collation
--Testcase 466:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE (v->>'c3')::text COLLATE "C" WHEN (v->>'c6')::text THEN true ELSE (v->>'c3')::text < 'bar' END;
-- check schema-qualification of regconfig constant
--Testcase 711:
CREATE TEXT SEARCH CONFIGURATION public.custom_search
  (COPY = pg_catalog.english);
--Testcase 712:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (v->>'c1')::int, to_tsvector('custom_search'::regconfig, (v->>'c3')::text) FROM ft1
WHERE (v->>'c1')::int = 642 AND length(to_tsvector('custom_search'::regconfig, (v->>'c3')::text)) > 0;
--Testcase 713:
SELECT (v->>'c1')::int, to_tsvector('custom_search'::regconfig, (v->>'c3')::text) FROM ft1
WHERE (v->>'c1')::int = 642 AND length(to_tsvector('custom_search'::regconfig, (v->>'c3')::text)) > 0;
--Testcase 714:
DROP TEXT SEARCH CONFIGURATION public.custom_search;
-- ===================================================================
-- JOIN queries
-- ===================================================================
-- Analyze ft4 and ft5 so that we have better statistics. These tables do not
-- have use_remote_estimate set.
-- ANALYZE ft4;
-- ANALYZE ft5;

-- join two tables
--Testcase 88:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
--Testcase 89:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- join three tables
--Testcase 90:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) JOIN ft4 t3 ON ((t3.v->>'c1')::int8 = (t1.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 10 LIMIT 10; 
--Testcase 91:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) JOIN ft4 t3 ON ((t3.v->>'c1')::int8 = (t1.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 10 LIMIT 10;
-- left outer join
--Testcase 92:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;
--Testcase 93:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;
-- left outer join three tables
--Testcase 94:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) LEFT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 95:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) LEFT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side
--Testcase 96:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t1.v->>'c2')::int8 as c2, (t2.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE (v->>'c1')::int8 < 10) t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) WHERE (t1.v->>'c1')::int8 < 10;
--Testcase 97:
SELECT (t1.v->>'c1')::int8 as c1, (t1.v->>'c2')::int8 as c2, (t2.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE (v->>'c1')::int8 < 10) t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) WHERE (t1.v->>'c1')::int8 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
--Testcase 98:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t1.v->>'c2')::int8 as c2, (t2.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE (v->>'c1')::int8 < 10) t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8)
			WHERE ((t2.v->>'c1')::int8 < 10 OR (t2.v->>'c1')::int8 IS NULL) AND (t1.v->>'c1')::int8 < 10;
--Testcase 99:
SELECT (t1.v->>'c1')::int8 as c1, (t1.v->>'c2')::int8 as c2, (t2.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE (v->>'c1')::int8 < 10) t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8)
			WHERE ((t2.v->>'c1')::int8 < 10 OR (t2.v->>'c1')::int8 IS NULL) AND (t1.v->>'c1')::int8 < 10;
-- right outer join
--Testcase 100:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t2.v->>'c1')::int8, (t1.v->>'c1')::int8 OFFSET 10 LIMIT 10;
--Testcase 101:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t2.v->>'c1')::int8, (t1.v->>'c1')::int8 OFFSET 10 LIMIT 10;
-- right outer join three tables
--Testcase 102:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) RIGHT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 103:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) RIGHT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
-- full outer join
--Testcase 104:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft4 t1 FULL JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 45 LIMIT 10;
--Testcase 105:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft4 t1 FULL JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 45 LIMIT 10;
-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
--Testcase 106:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT (v->>'c1')::int8 as c1 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t1 FULL JOIN (SELECT (v->>'c1')::int8 as c1 FROM ft5 WHERE (v->>'c1')::int8 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 107:
SELECT t1.c1, t2.c1 FROM (SELECT (v->>'c1')::int8 as c1 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t1 FULL JOIN (SELECT (v->>'c1')::int8 as c1 FROM ft5 WHERE (v->>'c1')::int8 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 108:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1 FROM (SELECT (v->>'c1')::int8 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t1 FULL JOIN (SELECT (v->>'c1')::int8 FROM ft5 WHERE (v->>'c1')::int8 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
--Testcase 109:
SELECT 1 FROM (SELECT (v->>'c1')::int8 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t1 FULL JOIN (SELECT (v->>'c1')::int8 FROM ft5 WHERE (v->>'c1')::int8 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
-- b. one of the joining relations is a base relation and the other is a join
-- relation
--Testcase 110:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT (v->>'c1')::int8 as c1 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t1 FULL JOIN (SELECT (t2.v->>'c1')::int8 as c1, (t3.v->>'c1')::int8 as c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) WHERE ((t2.v->>'c1')::int8 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 111:
SELECT t1.c1, ss.a, ss.b FROM (SELECT (v->>'c1')::int8 as c1 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t1 FULL JOIN (SELECT (t2.v->>'c1')::int8 as c1, (t3.v->>'c1')::int8 as c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) WHERE ((t2.v->>'c1')::int8 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- c. test deparsing the remote query as nested subqueries
--Testcase 112:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT (v->>'c1')::int8 as c1 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT (v->>'c1')::int8 as c1 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t2 FULL JOIN (SELECT (v->>'c1')::int8 as c1 FROM ft5 WHERE (v->>'c1')::int8 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 113:
SELECT t1.c1, ss.a, ss.b FROM (SELECT (v->>'c1')::int8 as c1 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT (v->>'c1')::int8 as c1 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t2 FULL JOIN (SELECT (v->>'c1')::int8 as c1 FROM ft5 WHERE (v->>'c1')::int8 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- d. test deparsing rowmarked relations as subqueries
--Testcase 114:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT (v->>'c1')::int8 as c1 FROM "S 1"."T3" WHERE (v->>'c1')::int8 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT (v->>'c1')::int8 as c1 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t2 FULL JOIN (SELECT (v->>'c1')::int8 as c1 FROM ft5 WHERE (v->>'c1')::int8 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
--Testcase 115:
SELECT t1.c1, ss.a, ss.b FROM (SELECT (v->>'c1')::int8 as c1 FROM "S 1"."T3" WHERE (v->>'c1')::int8 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT (v->>'c1')::int8 as c1 FROM ft4 WHERE (v->>'c1')::int8 between 50 and 60) t2 FULL JOIN (SELECT (v->>'c1')::int8 as c1 FROM ft5 WHERE (v->>'c1')::int8 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
-- full outer join + inner join
--Testcase 116:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1, (t3.v->>'c1')::int8 as c1 FROM ft4 t1 INNER JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8 + 1 and (t1.v->>'c1')::int8 between 50 and 60) FULL JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8, (t3.v->>'c1')::int8 LIMIT 10;
--Testcase 117:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1, (t3.v->>'c1')::int8 as c1 FROM ft4 t1 INNER JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8 + 1 and (t1.v->>'c1')::int8 between 50 and 60) FULL JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8, (t3.v->>'c1')::int8 LIMIT 10;
-- full outer join three tables
--Testcase 118:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 FULL JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) FULL JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 119:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 FULL JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) FULL JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
-- full outer join + right outer join
--Testcase 120:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 FULL JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) RIGHT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 121:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 FULL JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) RIGHT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
-- right outer join + full outer join
--Testcase 122:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) FULL JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 123:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) FULL JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
-- full outer join + left outer join
--Testcase 124:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 FULL JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) LEFT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 125:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 FULL JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) LEFT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
-- left outer join + full outer join
--Testcase 126:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) FULL JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 127:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) FULL JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 458:
SET enable_memoize TO off;
-- right outer join + left outer join
--Testcase 128:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) LEFT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 129:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) LEFT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 459:
RESET enable_memoize;
-- left outer join + right outer join
--Testcase 130:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) RIGHT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
--Testcase 131:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2, (t3.v->>'c3') as c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) RIGHT JOIN ft4 t3 ON ((t2.v->>'c1')::int8 = (t3.v->>'c1')::int8) OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause, only matched rows
--Testcase 132:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft4 t1 FULL JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) WHERE ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8 OR (t1.v->>'c1')::int8 IS NULL) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;
--Testcase 133:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft4 t1 FULL JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) WHERE ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8 OR (t1.v->>'c1')::int8 IS NULL) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause with shippable extensions set
--Testcase 134:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8, t2.v->>'c2', t1.v->>'c3' FROM ft1 t1 FULL JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) WHERE parquet_s3_fdw_abs((t1.v->>'c1')::int) > 0 OFFSET 10 LIMIT 10;
--Testcase 460:
ALTER SERVER parquet_s3_srv OPTIONS (DROP extensions);
-- full outer join + WHERE clause with shippable extensions not set
--Testcase 135:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8, t2.v->>'c2', t1.v->>'c3' FROM ft1 t1 FULL JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) WHERE parquet_s3_fdw_abs((t1.v->>'c1')::int) > 0 OFFSET 10 LIMIT 10;
--Testcase 461:
ALTER SERVER parquet_s3_srv OPTIONS (ADD extensions 'parquet_s3_fdw');
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
--Testcase 136:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 137:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 138:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10 FOR UPDATE;
--Testcase 139:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10 FOR UPDATE;
-- join two tables with FOR SHARE clause
--Testcase 140:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 141:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 142:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10 FOR SHARE;
--Testcase 143:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10 FOR SHARE;
-- join in CTE
--Testcase 144:
EXPLAIN (VERBOSE, COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT (t1.v->>'c1')::int8, t1.v->>'c3', (t2.v->>'c1')::int8 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
--Testcase 145:
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT (t1.v->>'c1')::int8, t1.v->>'c3', (t2.v->>'c1')::int8 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
-- ctid with whole-row reference
--Testcase 146:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.ctid, t1, t2, (t1.v->>'c1')::int8 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- SEMI JOIN, not pushed down
--Testcase 147:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 AS c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE (t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
--Testcase 148:
SELECT (t1.v->>'c1')::int8 AS c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE (t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- ANTI JOIN, not pushed down
--Testcase 149:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 AS c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE (t1.v->>'c1')::int8 = (t2.v->>'c2')::int8) ORDER BY (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
--Testcase 150:
SELECT (t1.v->>'c1')::int8 AS c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE (t1.v->>'c1')::int8 = (t2.v->>'c2')::int8) ORDER BY (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- CROSS JOIN can be pushed down
--Testcase 151:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 100 LIMIT 10;
--Testcase 152:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- different server, not pushed down. No result expected.
--Testcase 153:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft5 t1 JOIN ft6 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 100 LIMIT 10;
--Testcase 154:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft5 t1 JOIN ft6 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.
--Testcase 155:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.v->>'c8' = t2.v->>'c8') ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 100 LIMIT 10;
--Testcase 156:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.v->>'c8' = t2.v->>'c8') ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.
--Testcase 157:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) WHERE t1.v->>'c8' = 'foo' ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
--Testcase 158:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) WHERE t1.v->>'c8' = 'foo' ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
--Testcase 159:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) WHERE t1.v->>'c8' = t2.v->>'c8' ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
--Testcase 160:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) WHERE t1.v->>'c8' = t2.v->>'c8' ORDER BY t1.v->>'c3', (t1.v->>'c1')::int8 OFFSET 100 LIMIT 10;
-- Aggregate after UNION, for testing setrefs
--Testcase 161:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) UNION SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
--Testcase 162:
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) UNION SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c1')::int8 as c1 FROM ft1 t1 JOIN ft2 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
-- join with lateral reference
--Testcase 163:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1 FROM "S 1"."T1" t1, LATERAL (SELECT DISTINCT (t2.v->>'c1')::int8 as c1, (t3.v->>'c1')::int8 as c1 FROM ft1 t2, ft2 t3 WHERE (t2.v->>'c1')::int8 = (t3.v->>'c1')::int8 AND (t2.v->>'c2')::int = (t1.v->>'c2')::int) q ORDER BY (t1.v->>'c1')::int8 OFFSET 10 LIMIT 10;
--Testcase 164:
SELECT (t1.v->>'c1')::int8 as c1 FROM "S 1"."T1" t1, LATERAL (SELECT DISTINCT (t2.v->>'c1')::int8 as c1, (t3.v->>'c1')::int8 as c1 FROM ft1 t2, ft2 t3 WHERE (t2.v->>'c1')::int8 = (t3.v->>'c1')::int8 AND (t2.v->>'c2')::int = (t1.v->>'c2')::int) q ORDER BY (t1.v->>'c1')::int8 OFFSET 10 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}
--Testcase 165:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, (ft2.v->>'c1')::int8 as c1 FROM (SELECT 13 FROM ft1 WHERE (v->>'c1')::int8 = 13) q(a) RIGHT JOIN ft2 ON (q.a = (ft2.v->>'c1')::int8) WHERE (ft2.v->>'c1')::int8 BETWEEN 10 AND 15;
--Testcase 166:
SELECT q.a, (ft2.v->>'c1')::int8 as c1 FROM (SELECT 13 FROM ft1 WHERE (v->>'c1')::int8 = 13) q(a) RIGHT JOIN ft2 ON (q.a = (ft2.v->>'c1')::int8) WHERE (ft2.v->>'c1')::int8 BETWEEN 10 AND 15;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}
--Testcase 167:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (ft4.v->>'c1')::int8 as c1, q.* FROM ft4 LEFT JOIN (SELECT 13, (ft1.v->>'c1')::int8, (ft2.v->>'c1')::int8 FROM ft1 RIGHT JOIN ft2 ON ((ft1.v->>'c1')::int8 = (ft2.v->>'c1')::int8) WHERE (ft1.v->>'c1')::int8 = 12) q(a, b, c) ON ((ft4.v->>'c1')::int8 = q.b) WHERE (ft4.v->>'c1')::int8 BETWEEN 10 AND 15;
--Testcase 168:
SELECT (ft4.v->>'c1')::int8 as c1, q.* FROM ft4 LEFT JOIN (SELECT 13, (ft1.v->>'c1')::int8, (ft2.v->>'c1')::int8 FROM ft1 RIGHT JOIN ft2 ON ((ft1.v->>'c1')::int8 = (ft2.v->>'c1')::int8) WHERE (ft1.v->>'c1')::int8 = 12) q(a, b, c) ON ((ft4.v->>'c1')::int8 = q.b) WHERE (ft4.v->>'c1')::int8 BETWEEN 10 AND 15;

-- join with nullable side with some columns with null values
--Testcase 462:
UPDATE ft5 SET v = json_build_object('c3', null) where (v->>'c1')::int % 9 = 0;
--Testcase 463:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft5, ft5.v->>'c1', ft5.v->>'c2', ft5.v->>'c3', ft4.v->>'c1', ft4.v->>'c2' FROM ft5 left join ft4 on (ft5.v->>'c1')::int = (ft4.v->>'c1')::int WHERE (ft4.v->>'c1')::int BETWEEN 10 and 30 ORDER BY (ft5.v->>'c1')::int, (ft4.v->>'c1')::int;
--Testcase 464:
SELECT ft5, ft5.v->>'c1', ft5.v->>'c2', ft5.v->>'c3', ft4.v->>'c1', ft4.v->>'c2' FROM ft5 left join ft4 on (ft5.v->>'c1')::int = (ft4.v->>'c1')::int WHERE (ft4.v->>'c1')::int BETWEEN 10 and 30 ORDER BY (ft5.v->>'c1')::int, (ft4.v->>'c1')::int;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)
\set var :PATH_FILENAME'/ported_postgres/local_tbl.parquet'
--Testcase 169:
CREATE FOREIGN TABLE local_tbl (v jsonb)
SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'c1', schemaless 'true');
-- ANALYZE local_tbl;
--Testcase 465:
SET enable_nestloop TO false;
--Testcase 466:
SET enable_hashjoin TO false;
--Testcase 170:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE (ft1.v->>'c1')::int8 = (ft2.v->>'c1')::int8 AND (ft1.v->>'c2')::int = (ft4.v->>'c1')::int8
    AND (ft1.v->>'c2')::int = (ft5.v->>'c1')::int8 AND (ft1.v->>'c2')::int = (local_tbl.v->>'c1')::int8 AND (ft1.v->>'c1')::int8 < 100 AND (ft2.v->>'c1')::int8 < 100 ORDER BY (ft1.v->>'c1')::int8 FOR UPDATE;
--Testcase 171:
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE (ft1.v->>'c1')::int8 = (ft2.v->>'c1')::int8 AND (ft1.v->>'c2')::int = (ft4.v->>'c1')::int8
    AND (ft1.v->>'c2')::int = (ft5.v->>'c1')::int8 AND (ft1.v->>'c2')::int = (local_tbl.v->>'c1')::int8 AND (ft1.v->>'c1')::int8 < 100 AND (ft2.v->>'c1')::int8 < 100 ORDER BY (ft1.v->>'c1')::int8 FOR UPDATE;
--Testcase 467:
RESET enable_nestloop;
--Testcase 468:
RESET enable_hashjoin;

-- These test not supported on parquet_s3_fdw
-- test that add_paths_with_pathkeys_for_rel() arranges for the epq_path to
-- return columns needed by the parent ForeignScan node
--Testcase 715:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.*, COALESCE((ft1.v->>'c3')::text || (ft2.v->>'c3')::text, 'foobar') FROM ft1 INNER JOIN ft2 ON ((ft1.v->>'c1')::int8 = (ft2.v->>'c1')::int8 AND (ft1.v->>'c1')::int8 < 100)) ss ON ((local_tbl.v->>'c1')::int8 = (ss.v->'c1')::int8) ORDER BY (local_tbl.v->>'c1')::int8 FOR UPDATE OF local_tbl;
--Testcase 716:
ALTER SERVER parquet_s3_srv OPTIONS (DROP extensions);
--Testcase 717:
ALTER SERVER parquet_s3_srv OPTIONS (ADD fdw_startup_cost '10000.0');
--Testcase 718:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.* FROM ft1 INNER JOIN ft2 ON ((ft1.v->>'c1')::int8 = (ft2.v->>'c1')::int8 AND (ft1.v->>'c1')::int8 < 100 AND (ft1.v->>'c1')::int8 = parquet_s3_fdw_abs((ft2.v->>'c2')::int))) ss ON ((local_tbl.v->>'c3')::int8 = (ss.v->'c3')::int8) ORDER BY (local_tbl.v->>'c1')::int8 FOR UPDATE OF local_tbl;
--Testcase 719:
ALTER SERVER parquet_s3_srv OPTIONS (DROP fdw_startup_cost);
--Testcase 720:
ALTER SERVER parquet_s3_srv OPTIONS (ADD extensions 'parquet_s3_fdw');

--Testcase 172:
DROP FOREIGN TABLE local_tbl;

-- check join pushdown in situations where multiple userids are involved
--Testcase 173:
CREATE ROLE regress_view_owner SUPERUSER;
--Testcase 174:
CREATE USER MAPPING FOR regress_view_owner SERVER parquet_s3_srv;
GRANT SELECT ON ft4 TO regress_view_owner;
GRANT SELECT ON ft5 TO regress_view_owner;

--Testcase 175:
CREATE VIEW v4 AS SELECT * FROM ft4;
--Testcase 176:
CREATE VIEW v5 AS SELECT * FROM ft5;
--Testcase 469:
ALTER VIEW v5 OWNER TO regress_view_owner;
--Testcase 177:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM v4 t1 LEFT JOIN v5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
--Testcase 178:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM v4 t1 LEFT JOIN v5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
--Testcase 470:
ALTER VIEW v4 OWNER TO regress_view_owner;
--Testcase 179:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM v4 t1 LEFT JOIN v5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 180:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM v4 t1 LEFT JOIN v5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;  -- can be pushed down

--Testcase 181:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM v4 t1 LEFT JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user
--Testcase 182:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM v4 t1 LEFT JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;
--Testcase 471:
ALTER VIEW v4 OWNER TO CURRENT_USER;
--Testcase 183:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM v4 t1 LEFT JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 184:
SELECT (t1.v->>'c1')::int8 as c1, (t2.v->>'c2')::int8 as c2 FROM v4 t1 LEFT JOIN ft5 t2 ON ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) ORDER BY (t1.v->>'c1')::int8, (t2.v->>'c1')::int8 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 472:
ALTER VIEW v4 OWNER TO regress_view_owner;

-- cleanup
--Testcase 185:
DROP OWNED BY regress_view_owner;
--Testcase 186:
DROP ROLE regress_view_owner;


-- ===================================================================
-- Aggregate and grouping queries
-- ===================================================================

-- Simple aggregates
--Testcase 187:
explain (verbose, costs off)
select count(v->>'c6'), sum((v->>'c1')::int8), avg((v->>'c1')::int8), min((v->>'c2')::int), max((v->>'c1')::int8), stddev((v->>'c2')::int), sum((v->>'c1')::int8) * (random() <= 1)::int as sum2 from ft1 where (v->>'c2')::int < 5 group by (v->>'c2')::int order by 1, 2;
--Testcase 188:
select count(v->>'c6'), sum((v->>'c1')::int8), avg((v->>'c1')::int8), min((v->>'c2')::int), max((v->>'c1')::int8), stddev((v->>'c2')::int), sum((v->>'c1')::int8) * (random() <= 1)::int as sum2 from ft1 where (v->>'c2')::int < 5 group by (v->>'c2')::int order by 1, 2;

--Testcase 189:
explain (verbose, costs off)
select count(v->>'c6'), sum((v->>'c1')::int8), avg((v->>'c1')::int8), min((v->>'c2')::int), max((v->>'c1')::int8), stddev((v->>'c2')::int), sum((v->>'c1')::int8) * (random() <= 1)::int as sum2 from ft1 where (v->>'c2')::int < 5 group by (v->>'c2')::int order by 1, 2 limit 1;
--Testcase 190:
select count(v->>'c6'), sum((v->>'c1')::int8), avg((v->>'c1')::int8), min((v->>'c2')::int), max((v->>'c1')::int8), stddev((v->>'c2')::int), sum((v->>'c1')::int8) * (random() <= 1)::int as sum2 from ft1 where (v->>'c2')::int < 5 group by (v->>'c2')::int order by 1, 2 limit 1;

-- Aggregate is not pushed down as aggregation contains random()
--Testcase 191:
explain (verbose, costs off)
select sum((v->>'c1')::int8 * (random() <= 1)::int) as sum, avg((v->>'c1')::int8) from ft1;

-- Aggregate over join query
--Testcase 192:
explain (verbose, costs off)
select count(*), sum((t1.v->>'c1')::int8), avg((t2.v->>'c1')::int8) from ft1 t1 inner join ft1 t2 on ((t1.v->>'c2')::int = (t2.v->>'c2')::int) where (t1.v->>'c2')::int = 6;
--Testcase 193:
select count(*), sum((t1.v->>'c1')::int8), avg((t2.v->>'c1')::int8) from ft1 t1 inner join ft1 t2 on ((t1.v->>'c2')::int = (t2.v->>'c2')::int) where (t1.v->>'c2')::int = 6;

-- Not pushed down due to local conditions present in underneath input rel
--Testcase 194:
explain (verbose, costs off)
select sum((t1.v->>'c1')::int8), count((t2.v->>'c1')::int8) from ft1 t1 inner join ft2 t2 on ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) where (((t1.v->>'c1')::int8 * (t2.v->>'c1')::int8)/((t1.v->>'c1')::int8 * (t2.v->>'c1')::int8)) * random() <= 1;

-- GROUP BY clause having expressions
--Testcase 195:
explain (verbose, costs off)
select (v->>'c2')::int/2, sum((v->>'c2')::int) * ((v->>'c2')::int/2) from ft1 group by (v->>'c2')::int/2 order by (v->>'c2')::int/2;
--Testcase 196:
select (v->>'c2')::int/2, sum((v->>'c2')::int) * ((v->>'c2')::int/2) from ft1 group by (v->>'c2')::int/2 order by (v->>'c2')::int/2;

-- Aggregates in subquery are pushed down.
--Testcase 197:
explain (verbose, costs off)
select count(x.a), sum(x.a) from (select (v->>'c2')::int a, sum((v->>'c1')::int8) b from ft1 group by (v->>'c2')::int, sqrt((v->>'c1')::int8) order by 1, 2) x;
--Testcase 198:
select count(x.a), sum(x.a) from (select (v->>'c2')::int a, sum((v->>'c1')::int8) b from ft1 group by (v->>'c2')::int, sqrt((v->>'c1')::int8) order by 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out
--Testcase 199:
explain (verbose, costs off)
select (v->>'c2')::int * (random() <= 1)::int as sum1, sum((v->>'c1')::int8) * (v->>'c2')::int as sum2 from ft1 group by (v->>'c2')::int order by 1, 2;
--Testcase 200:
select (v->>'c2')::int * (random() <= 1)::int as sum1, sum((v->>'c1')::int8) * (v->>'c2')::int as sum2 from ft1 group by (v->>'c2')::int order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
--Testcase 201:
explain (verbose, costs off)
select (v->>'c2')::int * (random() <= 1)::int as c2 from ft2 group by (v->>'c2')::int * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
--Testcase 202:
explain (verbose, costs off)
select count((v->>'c2')::int) w, (v->>'c2')::int x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;
--Testcase 203:
select count((v->>'c2')::int) w, (v->>'c2')::int x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
--Testcase 204:
explain (verbose, costs off)
select (v->>'c2')::int as c2, (v->>'c2')::int as c2 from ft1 where (v->>'c2')::int > 6 group by 1, 2 order by sum((v->>'c1')::int8);
--Testcase 205:
select (v->>'c2')::int as c2, (v->>'c2')::int as c2 from ft1 where (v->>'c2')::int > 6 group by 1, 2 order by sum((v->>'c1')::int8);

-- Testing HAVING clause shippability
--Testcase 206:
explain (verbose, costs off)
select (v->>'c2')::int as c2, sum((v->>'c1')::int8) from ft2 group by (v->>'c2')::int having avg((v->>'c1')::int8) < 500 and sum((v->>'c1')::int8) < 49800 order by (v->>'c2')::int;
--Testcase 207:
select (v->>'c2')::int as c2, sum((v->>'c1')::int8) from ft2 group by (v->>'c2')::int having avg((v->>'c1')::int8) < 500 and sum((v->>'c1')::int8) < 49800 order by (v->>'c2')::int;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
--Testcase 208:
explain (verbose, costs off)
select count(*) from (select  v->>'c5', count((v->>'c1')::int8) from ft1 group by  v->>'c5', sqrt((v->>'c2')::int) having (avg((v->>'c1')::int8) / avg((v->>'c1')::int8)) * random() <= 1 and avg((v->>'c1')::int8) < 500) x;
--Testcase 209:
select count(*) from (select  v->>'c5', count((v->>'c1')::int8) from ft1 group by  v->>'c5', sqrt((v->>'c2')::int) having (avg((v->>'c1')::int8) / avg((v->>'c1')::int8)) * random() <= 1 and avg((v->>'c1')::int8) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
--Testcase 210:
explain (verbose, costs off)
select sum((v->>'c1')::int8) from ft1 group by (v->>'c2')::int having avg((v->>'c1')::int8 * (random() <= 1)::int) > 100 order by 1;

-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781
--Testcase 211:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum((v->>'c1')::int8) from ft1;
--Testcase 212:
select exists(select 1 from pg_enum), sum((v->>'c1')::int8) from ft1;

--Testcase 213:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum((v->>'c1')::int8) from ft1 group by 1;
--Testcase 214:
select exists(select 1 from pg_enum), sum((v->>'c1')::int8) from ft1 group by 1;


-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates

-- ORDER BY within aggregate, same column used to order
--Testcase 215:
explain (verbose, costs off)
select array_agg((v->>'c1')::int8 order by (v->>'c1')::int8) from ft1 where (v->>'c1')::int8 < 100 group by (v->>'c2')::int order by 1;
--Testcase 216:
select array_agg((v->>'c1')::int8 order by (v->>'c1')::int8) from ft1 where (v->>'c1')::int8 < 100 group by (v->>'c2')::int order by 1;

-- ORDER BY within aggregate, different column used to order also using DESC
--Testcase 217:
explain (verbose, costs off)
select array_agg((v->>'c5')::timestamp order by (v->>'c1')::int8 desc) from ft2 where (v->>'c2')::int = 6 and (v->>'c1')::int8 < 50;
--Testcase 218:
select array_agg((v->>'c5')::timestamp order by (v->>'c1')::int8 desc) from ft2 where (v->>'c2')::int = 6 and (v->>'c1')::int8 < 50;

-- DISTINCT within aggregate
--Testcase 219:
explain (verbose, costs off)
select array_agg(distinct ((t1.v->>'c1')::int8)%5) from ft4 t1 full join ft5 t2 on ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) where (t1.v->>'c1')::int8 < 20 or ((t1.v->>'c1')::int8 is null and (t2.v->>'c1')::int8 < 5) group by ((t2.v->>'c1')::int8)%3 order by 1;
--Testcase 220:
select array_agg(distinct ((t1.v->>'c1')::int8)%5) from ft4 t1 full join ft5 t2 on ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) where (t1.v->>'c1')::int8 < 20 or ((t1.v->>'c1')::int8 is null and (t2.v->>'c1')::int8 < 5) group by ((t2.v->>'c1')::int8)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate
--Testcase 221:
explain (verbose, costs off)
select array_agg(distinct ((t1.v->>'c1')::int8)%5 order by ((t1.v->>'c1')::int8)%5) from ft4 t1 full join ft5 t2 on ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) where (t1.v->>'c1')::int8 < 20 or ((t1.v->>'c1')::int8 is null and (t2.v->>'c1')::int8 < 5) group by ((t2.v->>'c1')::int8)%3 order by 1;
--Testcase 222:
select array_agg(distinct ((t1.v->>'c1')::int8)%5 order by ((t1.v->>'c1')::int8)%5) from ft4 t1 full join ft5 t2 on ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) where (t1.v->>'c1')::int8 < 20 or ((t1.v->>'c1')::int8 is null and (t2.v->>'c1')::int8 < 5) group by ((t2.v->>'c1')::int8)%3 order by 1;

--Testcase 223:
explain (verbose, costs off)
select array_agg(distinct ((t1.v->>'c1')::int8)%5 order by ((t1.v->>'c1')::int8)%5 desc nulls last) from ft4 t1 full join ft5 t2 on ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) where (t1.v->>'c1')::int8 < 20 or ((t1.v->>'c1')::int8 is null and (t2.v->>'c1')::int8 < 5) group by ((t2.v->>'c1')::int8)%3 order by 1;
--Testcase 224:
select array_agg(distinct ((t1.v->>'c1')::int8)%5 order by ((t1.v->>'c1')::int8)%5 desc nulls last) from ft4 t1 full join ft5 t2 on ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) where (t1.v->>'c1')::int8 < 20 or ((t1.v->>'c1')::int8 is null and (t2.v->>'c1')::int8 < 5) group by ((t2.v->>'c1')::int8)%3 order by 1;

-- FILTER within aggregate
--Testcase 225:
explain (verbose, costs off)
select sum((v->>'c1')::int8) filter (where (v->>'c1')::int8 < 100 and (v->>'c2')::int > 5) from ft1 group by (v->>'c2')::int order by 1 nulls last;
--Testcase 226:
select sum((v->>'c1')::int8) filter (where (v->>'c1')::int8 < 100 and (v->>'c2')::int > 5) from ft1 group by (v->>'c2')::int order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
--Testcase 227:
explain (verbose, costs off)
select sum((v->>'c1')::int8 % 3), sum(distinct (v->>'c1')::int8 %3 order by (v->>'c1')::int8 % 3) filter (where (v->>'c1')::int8 % 3 < 2), (v->>'c2')::int as c2 from ft1 where (v->>'c2')::int = 6 group by (v->>'c2')::int;
--Testcase 228:
select sum((v->>'c1')::int8 % 3), sum(distinct (v->>'c1')::int8 %3 order by (v->>'c1')::int8 % 3) filter (where (v->>'c1')::int8 % 3 < 2), (v->>'c2')::int as c2 from ft1 where (v->>'c2')::int = 6 group by (v->>'c2')::int;

-- Outer query is aggregation query
--Testcase 229:
explain (verbose, costs off)
select distinct (select count(*) filter (where (t2.v->>'c2')::int = 6 and (t2.v->>'c1')::int8 < 10) from ft1 t1 where (t1.v->>'c1')::int8 = 6) from ft2 t2 where (t2.v->>'c2')::int % 6 = 0 order by 1;
--Testcase 230:
select distinct (select count(*) filter (where (t2.v->>'c2')::int = 6 and (t2.v->>'c1')::int8 < 10) from ft1 t1 where (t1.v->>'c1')::int8 = 6) from ft2 t2 where (t2.v->>'c2')::int % 6 = 0 order by 1;
-- Inner query is aggregation query
--Testcase 231:
explain (verbose, costs off)
select distinct (select count((t1.v->>'c1')::int8) filter (where (t2.v->>'c2')::int = 6 and (t2.v->>'c1')::int8 < 10) from ft1 t1 where (t1.v->>'c1')::int8 = 6) from ft2 t2 where (t2.v->>'c2')::int % 6 = 0 order by 1;
--Testcase 232:
select distinct (select count((t1.v->>'c1')::int8) filter (where (t2.v->>'c2')::int = 6 and (t2.v->>'c1')::int8 < 10) from ft1 t1 where (t1.v->>'c1')::int8 = 6) from ft2 t2 where (t2.v->>'c2')::int % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable
--Testcase 233:
explain (verbose, costs off)
select sum((v->>'c1')::int8) filter (where ((v->>'c1')::int8 / (v->>'c1')::int8) * random() <= 1) from ft1 group by (v->>'c2')::int order by 1;
--Testcase 234:
explain (verbose, costs off)
select sum((v->>'c2')::int) filter (where (v->>'c2')::int in (select (v->>'c2')::int from ft1 where (v->>'c2')::int < 5)) from ft1;

-- Ordered-sets within aggregate
--Testcase 235:
explain (verbose, costs off)
select (v->>'c2')::int as c2, rank('10'::varchar) within group (order by v->>'c6'), percentile_cont((v->>'c2')::int/10::numeric) within group (order by (v->>'c1')::int8) from ft1 where (v->>'c2')::int < 10 group by (v->>'c2')::int having percentile_cont((v->>'c2')::int/10::numeric) within group (order by (v->>'c1')::int8) < 500 order by (v->>'c2')::int;
--Testcase 236:
select (v->>'c2')::int as c2, rank('10'::varchar) within group (order by v->>'c6'), percentile_cont((v->>'c2')::int/10::numeric) within group (order by (v->>'c1')::int8) from ft1 where (v->>'c2')::int < 10 group by (v->>'c2')::int having percentile_cont((v->>'c2')::int/10::numeric) within group (order by (v->>'c1')::int8) < 500 order by (v->>'c2')::int;

-- Using multiple arguments within aggregates
--Testcase 237:
explain (verbose, costs off)
select (v->>'c1')::int8 as c1, rank((v->>'c1')::int8, (v->>'c2')::int) within group (order by (v->>'c1')::int8, (v->>'c2')::int) from ft1 group by (v->>'c1')::int8, (v->>'c2')::int, v having (v->>'c1')::int8 = 6 order by 1;
--Testcase 238:
select (v->>'c1')::int8 as c1, rank((v->>'c1')::int8, (v->>'c2')::int) within group (order by (v->>'c1')::int8, (v->>'c2')::int) from ft1 group by (v->>'c1')::int8, (v->>'c2')::int, v having (v->>'c1')::int8 = 6 order by 1;

-- User defined function for user defined aggregate, VARIADIC
--Testcase 239:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
--Testcase 240:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

-- Disable hash aggregation for plan stability.
--Testcase 473:
set enable_hashagg to false;

-- Not pushed down due to user defined aggregate
--Testcase 241:
explain (verbose, costs off)
select (v->>'c2')::int as c2, least_agg((v->>'c1')::int8) from ft1 group by (v->>'c2')::int order by (v->>'c2')::int;

-- Add function and aggregate into extension
--Testcase 474:
alter extension parquet_s3_fdw add function least_accum(anyelement, variadic anyarray);
--Testcase 475:
alter extension parquet_s3_fdw add aggregate least_agg(variadic items anyarray);
--Testcase 476:
alter server parquet_s3_srv options (set extensions 'parquet_s3_fdw');

-- Now aggregate will be pushed.  Aggregate will display VARIADIC argument.
--Testcase 242:
explain (verbose, costs off)
select (v->>'c2')::int as c2, least_agg((v->>'c1')::int8) from ft1 where (v->>'c2')::int < 100 group by (v->>'c2')::int order by (v->>'c2')::int;
--Testcase 243:
select (v->>'c2')::int as c2, least_agg((v->>'c1')::int8) from ft1 where (v->>'c2')::int < 100 group by (v->>'c2')::int order by (v->>'c2')::int;

-- Remove function and aggregate from extension
--Testcase 477:
alter extension parquet_s3_fdw drop function least_accum(anyelement, variadic anyarray);
--Testcase 478:
alter extension parquet_s3_fdw drop aggregate least_agg(variadic items anyarray);
--Testcase 479:
alter server parquet_s3_srv options (set extensions 'parquet_s3_fdw');

-- Not pushed down as we have dropped objects from extension.
--Testcase 244:
explain (verbose, costs off)
select (v->>'c2')::int as c2, least_agg((v->>'c1')::int8) from ft1 group by (v->>'c2')::int order by (v->>'c2')::int;

-- Cleanup
--Testcase 480:
reset enable_hashagg;
--Testcase 245:
drop aggregate least_agg(variadic items anyarray);
--Testcase 246:
drop function least_accum(anyelement, variadic anyarray);


-- Testing USING OPERATOR() in ORDER BY within aggregate.
-- For this, we need user defined operators along with operator family and
-- operator class.  Create those and then add them in extension.  Note that
-- user defined objects are considered unshippable unless they are part of
-- the extension.
--Testcase 247:
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);

--Testcase 248:
create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);

--Testcase 249:
create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);

--Testcase 250:
create operator family my_op_family using btree;

--Testcase 251:
create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;

--Testcase 252:
create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);

-- This will not be pushed as user defined sort operator is not part of the
-- extension yet.
--Testcase 253:
explain (verbose, costs off)
select array_agg((v->>'c1')::int8 order by (v->>'c1')::int using operator(public.<^)) from ft2 where (v->>'c2')::int = 6 and (v->>'c1')::int8 < 100 group by (v->>'c2')::int;

-- This should not be pushed either.
--Testcase 447:
explain (verbose, costs off)
select * from ft2 order by (v->>'c1')::int using operator(public.<^);

-- Update local stats on ft2
-- ANALYZE ft2;

-- Add into extension
--Testcase 481:
alter extension parquet_s3_fdw add operator class my_op_class using btree;
--Testcase 482:
alter extension parquet_s3_fdw add function my_op_cmp(a int, b int);
--Testcase 483:
alter extension parquet_s3_fdw add operator family my_op_family using btree;
--Testcase 484:
alter extension parquet_s3_fdw add operator public.<^(int, int);
--Testcase 485:
alter extension parquet_s3_fdw add operator public.=^(int, int);
--Testcase 486:
alter extension parquet_s3_fdw add operator public.>^(int, int);
--Testcase 487:
alter server parquet_s3_srv options (set extensions 'parquet_s3_fdw');

-- Now this will be pushed as sort operator is part of the extension.
--Testcase 254:
explain (verbose, costs off)
select array_agg((v->>'c1')::int8 order by (v->>'c1')::int using operator(public.<^)) from ft2 where (v->>'c2')::int = 6 and (v->>'c1')::int8 < 100 group by (v->>'c2')::int;
--Testcase 255:
select array_agg((v->>'c1')::int8 order by (v->>'c1')::int using operator(public.<^)) from ft2 where (v->>'c2')::int = 6 and (v->>'c1')::int8 < 100 group by (v->>'c2')::int;

-- This should be pushed too.
-- Parquet_s3_fdw not support pushdown user-defined operator
--Testcase 448:
explain (verbose, costs off)
select * from ft2 order by (v->>'c1')::int using operator(public.<^);

-- Remove from extension
--Testcase 488:
alter extension parquet_s3_fdw drop operator class my_op_class using btree;
--Testcase 489:
alter extension parquet_s3_fdw drop function my_op_cmp(a int, b int);
--Testcase 490:
alter extension parquet_s3_fdw drop operator family my_op_family using btree;
--Testcase 491:
alter extension parquet_s3_fdw drop operator public.<^(int, int);
--Testcase 492:
alter extension parquet_s3_fdw drop operator public.=^(int, int);
--Testcase 493:
alter extension parquet_s3_fdw drop operator public.>^(int, int);
--Testcase 494:
alter server parquet_s3_srv options (set extensions 'parquet_s3_fdw');

-- This will not be pushed as sort operator is now removed from the extension.
--Testcase 256:
explain (verbose, costs off)
select array_agg((v->>'c1')::int8 order by (v->>'c1')::int using operator(public.<^)) from ft2 where (v->>'c2')::int = 6 and (v->>'c1')::int8 < 100 group by (v->>'c2')::int;

-- Cleanup
--Testcase 257:
drop operator class my_op_class using btree;
--Testcase 258:
drop function my_op_cmp(a int, b int);
--Testcase 259:
drop operator family my_op_family using btree;
--Testcase 260:
drop operator public.>^(int, int);
--Testcase 261:
drop operator public.=^(int, int);
--Testcase 262:
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.
--Testcase 263:
explain (verbose, costs off)
select count(t1.v->>'c3') from ft2 t1 left join ft2 t2 on ((t1.v->>'c1')::int8 = random() * (t2.v->>'c2')::int);

-- Subquery in FROM clause having aggregate
--Testcase 264:
explain (verbose, costs off)
select count(*), x.b from ft1, (select (v->>'c2')::int a, sum((v->>'c1')::int8) b from ft1 group by (v->>'c2')::int) x where (ft1.v->>'c2')::int = x.a group by x.b order by 1, 2;
--Testcase 265:
select count(*), x.b from ft1, (select (v->>'c2')::int a, sum((v->>'c1')::int8) b from ft1 group by (v->>'c2')::int) x where (ft1.v->>'c2')::int = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING
--Testcase 266:
explain (verbose, costs off)
select avg((t1.v->>'c1')::int8), sum((t2.v->>'c1')::int8) from ft4 t1 full join ft5 t2 on ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) group by (t2.v->>'c1')::int8 having (avg((t1.v->>'c1')::int8) is null and sum((t2.v->>'c1')::int8) < 10) or sum((t2.v->>'c1')::int8) is null order by 1 nulls last, 2;
--Testcase 267:
select avg((t1.v->>'c1')::int8), sum((t2.v->>'c1')::int8) from ft4 t1 full join ft5 t2 on ((t1.v->>'c1')::int8 = (t2.v->>'c1')::int8) group by (t2.v->>'c1')::int8 having (avg((t1.v->>'c1')::int8) is null and sum((t2.v->>'c1')::int8) < 10) or sum((t2.v->>'c1')::int8) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.
--Testcase 268:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select (v->>'c1')::int8 as c1 from ft4 where (v->>'c1')::int8 between 50 and 60) t1 full join (select (v->>'c1')::int8 as c1 from ft5 where (v->>'c1')::int8 between 50 and 60) t2 on (t1.c1 = t2.c1);
--Testcase 269:
select count(*), sum(t1.c1), avg(t2.c1) from (select (v->>'c1')::int8 as c1 from ft4 where (v->>'c1')::int8 between 50 and 60) t1 full join (select (v->>'c1')::int8 as c1 from ft5 where (v->>'c1')::int8 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
--Testcase 270:
explain (verbose, costs off)
select sum((v->>'c2')::int) * (random() <= 1)::int as sum from ft1 order by 1;
--Testcase 271:
select sum((v->>'c2')::int) * (random() <= 1)::int as sum from ft1 order by 1;

-- LATERAL join, with parameterization
--Testcase 495:
set enable_hashagg to false;
--Testcase 272:
explain (verbose, costs off)
select (v->>'c2')::int as c2, sum from "S 1"."T1" t1, lateral (select sum((t2.v->>'c1')::int8 + (t1.v->>'c1')::int8) sum from ft2 t2 group by (t2.v->>'c1')::int8) qry where (t1.v->>'c2')::int * 2 = qry.sum and (t1.v->>'c2')::int < 3 and (t1.v->>'c1')::int8 < 100 order by 1;
--Testcase 273:
select (v->>'c2')::int as c2, sum from "S 1"."T1" t1, lateral (select sum((t2.v->>'c1')::int8 + (t1.v->>'c1')::int8) sum from ft2 t2 group by (t2.v->>'c1')::int8) qry where (t1.v->>'c2')::int * 2 = qry.sum and (t1.v->>'c2')::int < 3 and (t1.v->>'c1')::int8 < 100 order by 1;
--Testcase 496:
reset enable_hashagg;

-- bug #15613: bad plan for foreign table scan with lateral reference
--Testcase 274:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (ref_0.v->>'c2')::int8 AS c2, subq_1.*
FROM
    "S 1"."T1" AS ref_0,
    LATERAL (
        SELECT (ref_0.v->>'c1')::int8 c1, subq_0.*
        FROM (SELECT (ref_0.v->>'c2')::int as c2, (ref_1.v->>'c3') as c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = (ref_3.v->>'c3'))
    ) AS subq_1
WHERE (ref_0.v->>'c1')::int8 < 10 AND subq_1.c3 = '00001'
ORDER BY (ref_0.v->>'c1')::int8;
--Testcase 275:
SELECT (ref_0.v->>'c2')::int8 AS c2, subq_1.*
FROM
    "S 1"."T1" AS ref_0,
    LATERAL (
        SELECT (ref_0.v->>'c1')::int8 c1, subq_0.*
        FROM (SELECT (ref_0.v->>'c2')::int as c2, (ref_1.v->>'c3') as c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = (ref_3.v->>'c3'))
    ) AS subq_1
WHERE (ref_0.v->>'c1')::int8 < 10 AND subq_1.c3 = '00001'
ORDER BY (ref_0.v->>'c1')::int8;

-- Check with placeHolderVars
--Testcase 276:
explain (verbose, costs off)
select sum(q.a), count(q.b) from ft4 left join (select 13, avg((ft1.v->>'c1')::int8), sum((ft2.v->>'c1')::int8) from ft1 right join ft2 on ((ft1.v->>'c1')::int8 = (ft2.v->>'c1')::int8)) q(a, b, c) on ((ft4.v->>'c1')::int8 <= q.b);
--Testcase 277:
select sum(q.a), count(q.b) from ft4 left join (select 13, avg((ft1.v->>'c1')::int8), sum((ft2.v->>'c1')::int8) from ft1 right join ft2 on ((ft1.v->>'c1')::int8 = (ft2.v->>'c1')::int8)) q(a, b, c) on ((ft4.v->>'c1')::int8 <= q.b);

-- Not supported cases
-- Grouping sets
--Testcase 278:
explain (verbose, costs off)
select (v->>'c2')::int as c2, sum((v->>'c1')::int8) from ft1 where (v->>'c2')::int < 3 group by rollup((v->>'c2')::int) order by 1 nulls last;
--Testcase 279:
select (v->>'c2')::int as c2, sum((v->>'c1')::int8) from ft1 where (v->>'c2')::int < 3 group by rollup((v->>'c2')::int) order by 1 nulls last;
--Testcase 280:
explain (verbose, costs off)
select (v->>'c2')::int as c2, sum((v->>'c1')::int8) from ft1 where (v->>'c2')::int < 3 group by cube((v->>'c2')::int) order by 1 nulls last;
--Testcase 281:
select (v->>'c2')::int as c2, sum((v->>'c1')::int8) from ft1 where (v->>'c2')::int < 3 group by cube((v->>'c2')::int) order by 1 nulls last;
--Testcase 282:
explain (verbose, costs off)
select (v->>'c2')::int as c2, (v->>'c6') as c6, sum((v->>'c1')::int8) from ft1 where (v->>'c2')::int < 3 group by grouping sets((v->>'c2')::int, (v->>'c6')) order by 1 nulls last, 2 nulls last;
--Testcase 283:
select (v->>'c2')::int as c2, (v->>'c6') as c6, sum((v->>'c1')::int8) from ft1 where (v->>'c2')::int < 3 group by grouping sets((v->>'c2')::int, (v->>'c6')) order by 1 nulls last, 2 nulls last;
--Testcase 284:
explain (verbose, costs off)
select (v->>'c2')::int as c2, sum((v->>'c1')::int8), grouping((v->>'c2')::int) from ft1 where (v->>'c2')::int < 3 group by (v->>'c2')::int order by 1 nulls last;
--Testcase 285:
select (v->>'c2')::int as c2, sum((v->>'c1')::int8), grouping((v->>'c2')::int) from ft1 where (v->>'c2')::int < 3 group by (v->>'c2')::int order by 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
--Testcase 286:
explain (verbose, costs off)
select distinct sum((v->>'c1')::int4)/1000 s from ft2 where (v->>'c2')::int < 6 group by (v->>'c2')::int order by 1;
--Testcase 287:
select distinct sum((v->>'c1')::int4)/1000 s from ft2 where (v->>'c2')::int < 6 group by (v->>'c2')::int order by 1;

-- WindowAgg
--Testcase 288:
explain (verbose, costs off)
select (v->>'c2')::int as c2, sum((v->>'c2')::int), count((v->>'c2')::int) over (partition by (v->>'c2')::int%2) from ft2 where (v->>'c2')::int < 10 group by (v->>'c2')::int order by 1;
--Testcase 289:
select (v->>'c2')::int as c2, sum((v->>'c2')::int), count((v->>'c2')::int) over (partition by (v->>'c2')::int%2) from ft2 where (v->>'c2')::int < 10 group by (v->>'c2')::int order by 1;
--Testcase 290:
explain (verbose, costs off)
select (v->>'c2')::int as c2, array_agg((v->>'c2')::int) over (partition by (v->>'c2')::int%2 order by (v->>'c2')::int desc) from ft1 where (v->>'c2')::int < 10 group by (v->>'c2')::int order by 1;
--Testcase 291:
select (v->>'c2')::int as c2, array_agg((v->>'c2')::int) over (partition by (v->>'c2')::int%2 order by (v->>'c2')::int desc) from ft1 where (v->>'c2')::int < 10 group by (v->>'c2')::int order by 1;
--Testcase 292:
explain (verbose, costs off)
select (v->>'c2')::int as c2, array_agg((v->>'c2')::int) over (partition by (v->>'c2')::int%2 order by (v->>'c2')::int range between current row and unbounded following) from ft1 where (v->>'c2')::int < 10 group by (v->>'c2')::int order by 1;
--Testcase 293:
select (v->>'c2')::int as c2, array_agg((v->>'c2')::int) over (partition by (v->>'c2')::int%2 order by (v->>'c2')::int range between current row and unbounded following) from ft1 where (v->>'c2')::int < 10 group by (v->>'c2')::int order by 1;


-- ===================================================================
-- parameterized queries
-- ===================================================================
-- simple join
--Testcase 294:
PREPARE st1(int, int) AS SELECT (t1.v->>'c3') as c3, (t2.v->>'c3') as c3 FROM ft1 t1, ft2 t2 WHERE (t1.v->>'c1')::int8 = $1 AND (t2.v->>'c1')::int8 = $2;
--Testcase 295:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(1, 2);
--Testcase 296:
EXECUTE st1(1, 1);
--Testcase 297:
EXECUTE st1(101, 101);
-- subquery using stable function (can't be sent to remote)
--Testcase 298:
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int8 < $2 AND (t1.v->>'c3')::int IN (SELECT (v->>'c3')::int as c3 FROM ft2 t2 WHERE (v->>'c1')::int8 > $1 AND date((v->>'c5')::timestamp) = '1970-01-17'::date) ORDER BY (v->>'c1')::int8;
--Testcase 299:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st2(10, 20);
--Testcase 300:
EXECUTE st2(10, 20);
--Testcase 301:
EXECUTE st2(101, 121);
-- subquery using immutable function (can be sent to remote)
--Testcase 302:
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int8 < $2 AND (t1.v->>'c3')::int IN (SELECT (v->>'c3')::int as c3 FROM ft2 t2 WHERE (v->>'c1')::int8 > $1 AND date((v->>'c5')::timestamp) = '1970-01-17'::date) ORDER BY (v->>'c1')::int8;
--Testcase 303:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st3(10, 20);
--Testcase 304:
EXECUTE st3(10, 20);
--Testcase 305:
EXECUTE st3(20, 30);
-- custom plan should be chosen initially
--Testcase 306:
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = $1;
--Testcase 307:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 308:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 309:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 310:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 311:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan
--Testcase 312:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote
--Testcase 313:
PREPARE st5(user_enum,int) AS SELECT * FROM ft1 t1 WHERE v->>'c8' = $1::text and (v->>'c1')::int8 = $2;
--Testcase 314:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 315:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 316:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 317:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 318:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 319:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 320:
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning
--Testcase 321:
PREPARE st6 AS SELECT * FROM ft1 t1 WHERE (t1.v->>'c1')::int8 = (t1.v->>'c2')::int;
--Testcase 322:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 497:
PREPARE st7 AS INSERT INTO ft1 VALUES (json_build_object('c1', 1001, 'c2', 101, 'c3', 'foo'));
--Testcase 498:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;

-- ALTER TABLE "S 1"."T1" RENAME TO "T0";
\set var :PATH_FILENAME'/ported_postgres/T0.parquet'
--Testcase 499:
ALTER FOREIGN TABLE ft1 OPTIONS (SET filename :'var');
--Testcase 323:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 324:
EXECUTE st6;
-- EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
-- ALTER TABLE "S 1"."T0" RENAME TO "T1";
\set var :PATH_FILENAME'/ported_postgres/T1.parquet'
--Testcase 500:
ALTER FOREIGN TABLE ft1 OPTIONS (SET filename :'var');

--Testcase 325:
PREPARE st8 AS SELECT count(v->>'c3') FROM ft1 t1 WHERE (t1.v->>'c1')::int === (t1.v->>'c2')::int;
--Testcase 326:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
--Testcase 501:
ALTER SERVER parquet_s3_srv OPTIONS (DROP extensions);
--Testcase 327:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
--Testcase 328:
EXECUTE st8;
--Testcase 502:
ALTER SERVER parquet_s3_srv OPTIONS (ADD extensions 'parquet_s3_fdw');

-- cleanup
DEALLOCATE st1;
DEALLOCATE st2;
DEALLOCATE st3;
DEALLOCATE st4;
DEALLOCATE st5;
DEALLOCATE st6;
-- DEALLOCATE st7;
DEALLOCATE st8;

-- System columns, except ctid and oid, should not be sent to remote
--Testcase 329:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1;
--Testcase 330:
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1'::regclass LIMIT 1;
--Testcase 331:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 332:
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 333:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 334:
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 335:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM ft1 t1 LIMIT 1;
--Testcase 336:
SELECT ctid, * FROM ft1 t1 LIMIT 1;

-- ===================================================================
-- used in PL/pgSQL function
-- ===================================================================
--Testcase 337:
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN
--Testcase 338:
    SELECT (v->>'c1')::int8 INTO v_c1 FROM ft1 WHERE (v->>'c1')::int8 = p_c1 LIMIT 1;
    PERFORM (v->>'c1')::int8 FROM ft1 WHERE (v->>'c1')::int8 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
--Testcase 339:
SELECT f_test(100);
--Testcase 340:
DROP FUNCTION f_test(int);

-- ===================================================================
-- REINDEX
-- ===================================================================
-- remote table is not created here
-- raise error when creating foreign table with local file
-- do not raise error with minio/s3 servers but raise error when selecting table
\set var :PATH_FILENAME'/ported_postgres/reindex_local.parquet'
--Testcase 341:
CREATE FOREIGN TABLE reindex_foreign (v jsonb)
  SERVER parquet_s3_srv_2 OPTIONS (filename :'var', sorted 'c1', schemaless 'true');
REINDEX TABLE reindex_foreign; -- error
REINDEX TABLE CONCURRENTLY reindex_foreign; -- error
--Testcase 342:
DROP FOREIGN TABLE reindex_foreign;
-- partitions and foreign tables
-- CREATE TABLE reind_fdw_parent (c1 int) PARTITION BY RANGE ((v->>'c1')::int8);
-- CREATE TABLE reind_fdw_0_10 PARTITION OF reind_fdw_parent
--   FOR VALUES FROM (0) TO (10);
-- CREATE FOREIGN TABLE reind_fdw_10_20 PARTITION OF reind_fdw_parent
--   FOR VALUES FROM (10) TO (20)
--   SERVER loopback OPTIONS (table_name 'reind_local_10_20');
-- REINDEX TABLE reind_fdw_parent; -- ok
-- REINDEX TABLE CONCURRENTLY reind_fdw_parent; -- ok
-- DROP TABLE reind_fdw_parent;

-- ===================================================================
-- conversion error
-- ===================================================================
-- ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE int;
-- SELECT * FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8) WHERE x1 = 1;  -- ERROR
-- SELECT ftx.x1, ft2.v->>'c2', ftx.x8 FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8), ft2
--   WHERE ftx.x1 = ft2.v->>'c1' AND ftx.x1 = 1; -- ERROR
-- SELECT ftx.x1, ft2.v->>'c2', ftx FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8), ft2
--   WHERE ftx.x1 = ft2.v->>'c1' AND ftx.x1 = 1; -- ERROR
-- SELECT sum((v->>'c2')::int), array_agg(c8) FROM ft1 GROUP BY c8; -- ERROR
-- ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE text;

-- ===================================================================
-- local type can be different from remote type in some cases,
-- in particular if similarly-named operators do equivalent things
-- ===================================================================
--Testcase 452:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE (v->>'c8')::text = 'foo' LIMIT 1;
--Testcase 453:
SELECT * FROM ft1 WHERE (v->>'c8')::text = 'foo' LIMIT 1;
--Testcase 454:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE 'foo' = (v->>'c8')::text LIMIT 1;
--Testcase 455:
SELECT * FROM ft1 WHERE 'foo' = (v->>'c8')::text LIMIT 1;
-- we declared c8 to be text locally, but it's still the same type on
-- the remote which will balk if we try to do anything incompatible
-- with that remote type
-- Parquet_S3_FDW is type of file fdw. Not support user define type
--Testcase 456:
SELECT * FROM ft1 WHERE (v->>'c8')::text LIKE 'foo' LIMIT 1; -- ERROR
--Testcase 457:
SELECT * FROM ft1 WHERE ((v->>'c8')::text)::text LIKE 'foo' LIMIT 1; -- ERROR; cast not pushed down

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
-- BEGIN;
-- DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY v->>'c1';
-- FETCH c;
-- SAVEPOINT s;
-- ERROR OUT;          -- ERROR
-- ROLLBACK TO s;
-- FETCH c;
-- SAVEPOINT s;
-- SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
-- ROLLBACK TO s;
-- FETCH c;
-- SELECT * FROM ft1 ORDER BY v->>'c1' LIMIT 1;
-- COMMIT;

-- ===================================================================
-- test handling of collations
-- schemaless foreign table does not have text column, this test is skipped
-- ===================================================================
-- \set var :PATH_FILENAME'/ported_postgres/loct3.parquet'
-- --Testcase 343:
-- create foreign table loct3 (f1 text collate "C", f2 text, f3 varchar(10))
--   server parquet_s3_srv options (filename :'var');
-- --Testcase 344:
-- create foreign table ft3 (f1 text collate "C", f2 text, f3 varchar(10))
--   server parquet_s3_srv options (filename :'var');

-- -- can be sent to remote
-- --Testcase 345:
-- explain (verbose, costs off) select * from ft3 where f1 = 'foo';
-- --Testcase 346:
-- explain (verbose, costs off) select * from ft3 where f1 COLLATE "C" = 'foo';
-- --Testcase 347:
-- explain (verbose, costs off) select * from ft3 where f2 = 'foo';
-- --Testcase 348:
-- explain (verbose, costs off) select * from ft3 where f3 = 'foo';
-- --Testcase 349:
-- explain (verbose, costs off) select * from ft3 f, loct3 l
--   where f.f3 = l.f3 and l.f1 = 'foo';
-- -- can't be sent to remote
-- --Testcase 350:
-- explain (verbose, costs off) select * from ft3 where f1 COLLATE "POSIX" = 'foo';
-- --Testcase 351:
-- explain (verbose, costs off) select * from ft3 where f1 = 'foo' COLLATE "C";
-- --Testcase 352:
-- explain (verbose, costs off) select * from ft3 where f2 COLLATE "C" = 'foo';
-- --Testcase 353:
-- explain (verbose, costs off) select * from ft3 where f2 = 'foo' COLLATE "C";
-- --Testcase 354:
-- explain (verbose, costs off) select * from ft3 f, loct3 l
--   where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';

-- ===================================================================
-- test writable foreign table stuff
-- ===================================================================
--Testcase 503:
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (v) SELECT json_build_object('c1', (v->>'c1')::int8+1000, 'c2', (v->>'c2')::int8 + 100, 'c3', (v->>'c3') || (v->>'c3')) FROM ft2 LIMIT 20;
--Testcase 504:
INSERT INTO ft2 (v) SELECT json_build_object('c1', (v->>'c1')::int8+1000, 'c2', (v->>'c2')::int8 + 100, 'c3', (v->>'c3') || (v->>'c3')) FROM ft2 LIMIT 20;
--Testcase 505:
INSERT INTO ft2 (v)
  VALUES (json_build_object('c1', 1101, 'c2', 201, 'c3', 'aaa')), (json_build_object('c1', 1102,'c2', 202,'c3', 'bbb')), (json_build_object('c1', 1103,'c2', 203,'c3', 'ccc'));
--Testcase 506:
INSERT INTO ft2 (v) VALUES (json_build_object('c1', 1104, 'c2', 204, 'c3', 'ddd')), (json_build_object('c1', 1105, 'c2', 205, 'c3', 'eee'));
--Testcase 507:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET v = json_build_object('c2', (v->>'c2')::int8 + 300, 'c3' ,v->>'c3' || '_update3') WHERE (v->>'c1')::int8 % 10 = 3;
--Testcase 508:
UPDATE ft2 SET v = json_build_object('c2', (v->>'c2')::int8 + 300, 'c3' ,v->>'c3' || '_update3') WHERE (v->>'c1')::int8 % 10 = 3;
--Testcase 509:
SELECT * FROM ft2 WHERE (v->>'c1')::int8 % 10 = 3;
--Testcase 510:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET v = json_build_object('c2', (v->>'c2')::int8 + 400, 'c3', v->>'c3' || '_update7') WHERE (v->>'c1')::int8 % 10 = 7;
--Testcase 511:
UPDATE ft2 SET v = json_build_object('c2', (v->>'c2')::int8 + 400, 'c3', v->>'c3' || '_update7') WHERE (v->>'c1')::int8 % 10 = 7;
--Testcase 512:
SELECT * FROM ft2 WHERE (v->>'c1')::int8 % 10 = 7;
--Testcase 513:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET v = json_build_object('c2', (ft2.v->>'c2')::int8 + 500, 'c3', ft2.v->>'c3' || '_update9', 'c7', 'ft2')
  FROM ft1 WHERE (ft1.v->>'c1')::int8 = (ft2.v->>'c2')::int8 AND (ft1.v->>'c1')::int8 % 10 = 9;
--Testcase 514:
UPDATE ft2 SET v = json_build_object('c2', (ft2.v->>'c2')::int8 + 500, 'c3', ft2.v->>'c3' || '_update9', 'c7', 'ft2')
  FROM ft1 WHERE (ft1.v->>'c1')::int8 = (ft2.v->>'c2')::int8 AND (ft1.v->>'c1')::int8 % 10 = 9;
--Testcase 515:
EXPLAIN (verbose, costs off)
  DELETE FROM ft2 WHERE (v->>'c1')::int8 % 10 = 5;
--Testcase 516:
DELETE FROM ft2 WHERE (v->>'c1')::int8 % 10 = 5;
--Testcase 517:
EXPLAIN (verbose, costs off)
DELETE FROM ft2 USING ft1 WHERE (ft1.v->>'c1')::int8 = (ft2.v->>'c2')::int8 AND (ft1.v->>'c1')::int8 % 10 = 2;
--Testcase 518:
DELETE FROM ft2 USING ft1 WHERE (ft1.v->>'c1')::int8 = (ft2.v->>'c2')::int8 AND (ft1.v->>'c1')::int8 % 10 = 2;
--Testcase 519:
SELECT (v->>'c1')::int8 AS c1,(v->>'c2')::int AS c2,v->>'c3' AS c3,v->>'c5' AS c5 FROM ft2 ORDER BY (v->>'c1')::int8;
--Testcase 520:
EXPLAIN (verbose, costs off)
INSERT INTO ft2 VALUES (json_build_object('c1', 1200, 'c2', 999, 'c3', 'foo'));
--Testcase 521:
INSERT INTO ft2 VALUES (json_build_object('c1', 1200, 'c2', 999, 'c3', 'foo'));
--Testcase 522:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET v = json_build_object('c3', 'bar') WHERE (v->>'c1')::int8 = 1200;
--Testcase 523:
UPDATE ft2 SET v = json_build_object('c3', 'bar') WHERE (v->>'c1')::int8 = 1200;
--Testcase 524:
EXPLAIN (verbose, costs off)
DELETE FROM ft2 WHERE (v->>'c1')::int8 = 1200;
--Testcase 525:
DELETE FROM ft2 WHERE (v->>'c1')::int8 = 1200;

-- Test UPDATE/DELETE with RETURNING on a three-table join
--Testcase 526:
INSERT INTO ft2
  SELECT json_build_object('c1', id, 'c2', id - 1200, 'c3', to_char(id, 'FM00000')) FROM generate_series(1201, 1300) id;
--Testcase 527:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET v = json_build_object('c3', 'foo')
  FROM ft4 INNER JOIN ft5 ON ((ft4.v->>'c1')::int = (ft5.v->>'c1')::int)
  WHERE (ft2.v->>'c1')::int8 > 1200 AND (ft2.v->>'c2')::int = (ft4.v->>'c1')::int;
--Testcase 528:
UPDATE ft2 SET v = json_build_object('c3', 'foo')
  FROM ft4 INNER JOIN ft5 ON ((ft4.v->>'c1')::int = (ft5.v->>'c1')::int)
  WHERE (ft2.v->>'c1')::int8 > 1200 AND (ft2.v->>'c2')::int = (ft4.v->>'c1')::int;
--Testcase 529:
SELECT ft2, ft2.*, ft4, ft4.*
  FROM ft2 INNER JOIN ft4 ON ((ft2.v->>'c1')::int8 > 1200 AND (ft2.v->>'c2')::int = (ft4.v->>'c1')::int)
  INNER JOIN ft5 ON ((ft4.v->>'c1')::int = (ft5.v->>'c1')::int);
--Testcase 530:
EXPLAIN (verbose, costs off)
DELETE FROM ft2
  USING ft4 LEFT JOIN ft5 ON ((ft4.v->>'c1')::int = (ft5.v->>'c1')::int)
  WHERE (ft2.v->>'c1')::int8 > 1200 AND (ft2.v->>'c1')::int8 % 10 = 0 AND (ft2.v->>'c2')::int = (ft4.v->>'c1')::int;
--Testcase 531:
DELETE FROM ft2
  USING ft4 LEFT JOIN ft5 ON ((ft4.v->>'c1')::int = (ft5.v->>'c1')::int)
  WHERE (ft2.v->>'c1')::int8 > 1200 AND (ft2.v->>'c1')::int8 % 10 = 0 AND (ft2.v->>'c2')::int = (ft4.v->>'c1')::int;
--Testcase 532:
DELETE FROM ft2 WHERE (ft2.v->>'c1')::int8 > 1200;

-- Test UPDATE with a MULTIEXPR sub-select
-- (maybe someday this'll be remotely executable, but not today)
--Testcase 533:
EXPLAIN (verbose, costs off)
UPDATE ft2 AS target SET (v) = (
    SELECT json_build_object('c2', (v->>'c2')::int * 10, 'c7', v->>'c7')
        FROM ft2 AS src
        WHERE (target.v->>'c1')::int8 = (src.v->>'c1')::int8
) WHERE (v->>'c1')::int > 1100;

--Testcase 534:
UPDATE ft2 AS target SET (v) = (
    SELECT json_build_object('c2', (v->>'c2')::int * 10, 'c7', v->>'c7')
        FROM ft2 AS src
        WHERE (target.v->>'c1')::int8 = (src.v->>'c1')::int8
) WHERE (v->>'c1')::int > 1100;

--Testcase 535:
UPDATE ft2 AS target SET (v) = (
    SELECT json_build_object('c2', (v->>'c2')::int / 10)
        FROM ft2 AS src
        WHERE (target.v->>'c1')::int8 = (src.v->>'c1')::int8
) WHERE (v->>'c1')::int > 1100;

-- Test UPDATE involving a join that can be pushed down,
-- but a SET clause that can't be
--Testcase 536:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE ft2 d SET v = json_build_object('c2', CASE WHEN random() >= 0 THEN (d.v->'c2')::int ELSE 0 END)
  FROM ft2 AS t WHERE (d.v->'c1')::int8 = (t.v->'c1')::int8 AND (d.v->'c1')::int8 > 1000;
--Testcase 537:
UPDATE ft2 d SET v = json_build_object('c2', CASE WHEN random() >= 0 THEN (d.v->'c2')::int ELSE 0 END)
  FROM ft2 AS t WHERE (d.v->'c1')::int8 = (t.v->'c1')::int8 AND (d.v->'c1')::int8 > 1000;

-- Test UPDATE/DELETE with WHERE or JOIN/ON conditions containing
-- user-defined operators/functions
-- ALTER SERVER loopback OPTIONS (DROP extensions);
--Testcase 538:
INSERT INTO ft2
  SELECT json_build_object('c1', id, 'c2', id % 10, 'c3', to_char(id, 'FM00000')) FROM generate_series(2001, 2010) id;
--Testcase 539:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET v = json_build_object('c3', 'bar') WHERE parquet_s3_fdw_abs((v->>'c1')::int) > 2000;
--Testcase 540:
UPDATE ft2 SET v = json_build_object('c3', 'bar') WHERE parquet_s3_fdw_abs((v->>'c1')::int) > 2000;
--Testcase 541:
SELECT * FROM ft2 WHERE parquet_s3_fdw_abs((v->>'c1')::int) > 2000;
--Testcase 542:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET v = json_build_object('c3', 'baz')
  FROM ft4 INNER JOIN ft5 ON ((ft4.v->>'c1')::int = (ft5.v->>'c1')::int)
  WHERE (ft2.v->>'c1')::int > 2000 AND (ft2.v->>'c2')::int === (ft4.v->>'c1')::int;
--Testcase 543:
UPDATE ft2 SET v = json_build_object('c3', 'baz')
  FROM ft4 INNER JOIN ft5 ON ((ft4.v->>'c1')::int = (ft5.v->>'c1')::int)
  WHERE (ft2.v->>'c1')::int > 2000 AND (ft2.v->>'c2')::int === (ft4.v->>'c1')::int;
--Testcase 544:
SELECT ft2.*, ft4.*, ft5.*
  FROM ft2, ft4 INNER JOIN ft5 ON ((ft4.v->>'c1')::int = (ft5.v->>'c1')::int)
  WHERE (ft2.v->>'c1')::int > 2000 AND (ft2.v->>'c2')::int === (ft4.v->>'c1')::int;
--Testcase 545:
EXPLAIN (verbose, costs off)
DELETE FROM ft2
  USING ft4 INNER JOIN ft5 ON ((ft4.v->>'c1')::int === (ft5.v->>'c1')::int)
  WHERE (ft2.v->>'c1')::int > 2000 AND (ft2.v->>'c2')::int = (ft4.v->>'c1')::int;
--Testcase 546:
DELETE FROM ft2
  USING ft4 INNER JOIN ft5 ON ((ft4.v->>'c1')::int === (ft5.v->>'c1')::int)
  WHERE (ft2.v->>'c1')::int > 2000 AND (ft2.v->>'c2')::int = (ft4.v->>'c1')::int;
--Testcase 547:
DELETE FROM ft2 WHERE (ft2.v->>'c1')::int8 > 2000;
-- ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

-- Test that trigger on remote table works as expected
--Testcase 548:
CREATE OR REPLACE FUNCTION "S 1".F_BRTRIG() RETURNS trigger AS $$
BEGIN
    NEW.v =  jsonb_set(NEW.v, '{c3}', to_jsonb(NEW.v->>'c3' || '_trig_update'), false);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--Testcase 549:
CREATE TRIGGER t1_br_insert BEFORE INSERT OR UPDATE
    ON ft2 FOR EACH ROW EXECUTE PROCEDURE "S 1".F_BRTRIG();

--Testcase 550:
INSERT INTO ft2 VALUES (json_build_object('c1', 1208, 'c2', 818, 'c3', 'fff'));
--Testcase 551:
SELECT * FROM ft2 WHERE (v->>'c1')::int8 = 1208;
--Testcase 552:
INSERT INTO ft2 VALUES (json_build_object('c1', 1218, 'c2', 818, 'c3', 'ggg', 'c6', '(--;'));
--Testcase 553:
SELECT * FROM ft2 WHERE (v->>'c1')::int8 = 1218;
--Testcase 554:
UPDATE ft2 SET v = json_build_object('c2', (ft2.v->>'c2')::int + 600, 'c3', v->>'c3') WHERE (v->>'c1')::int8 % 10 = 8 AND (v->>'c1')::int8 < 1200;
--Testcase 555:
SELECT * FROM ft2 WHERE (v->>'c1')::int8 % 10 = 8 AND (v->>'c1')::int8 < 1200;

-- -- Test errors thrown on remote side during update
-- ALTER TABLE "S 1"."T 1" ADD CONSTRAINT c2positive CHECK (c2 >= 0);
-- parquet storage can not check duplicate key / conflict
-- INSERT INTO ft1(c1, c2) VALUES(11, 12);  -- duplicate key
-- INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT DO NOTHING; -- works
-- INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO NOTHING; -- unsupported
-- INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO UPDATE SET c3 = 'ffg'; -- unsupported
-- INSERT INTO ft1(c1, c2) VALUES(1111, -2);  -- c2positive
-- UPDATE ft1 SET c2 = -c2 WHERE v->>'c1' = 1;  -- c2positive

-- Test savepoint/rollback behavior
-- parquet_s3_fdw does not support transaction, the expected will not be same as postgres_fdw
--Testcase 355:
select (v->>'c2')::int as c2, count(*) from ft2 where (v->>'c2')::int < 500 group by 1 order by 1;
--Testcase 356:
select (v->>'c2')::int as c2, count(*) from "S 1"."T1" where (v->>'c2')::int < 500 group by 1 order by 1;
begin;
--Testcase 556:
update ft2 set v = json_build_object('c2', 42, 'c3', v->>'c3') where (v->>'c2')::int = 0;
--Testcase 357:
select (v->>'c2')::int as c2, count(*) from ft2 where (v->>'c2')::int < 500 group by 1 order by 1;
savepoint s1;
--Testcase 557:
update ft2 set v = json_build_object('c2', 44, 'c3', v->>'c3') where (v->>'c2')::int = 4;
--Testcase 358:
select (v->>'c2')::int as c2, count(*) from ft2 where (v->>'c2')::int < 500 group by 1 order by 1;
release savepoint s1;
--Testcase 359:
select (v->>'c2')::int as c2, count(*) from ft2 where (v->>'c2')::int < 500 group by 1 order by 1;
savepoint s2;
--Testcase 558:
update ft2 set  v = json_build_object('c2', 46, 'c3', v->>'c3') where (v->>'c2')::int = 6;
--Testcase 360:
select (v->>'c2')::int as c2, count(*) from ft2 where (v->>'c2')::int < 500 group by 1 order by 1;
rollback to savepoint s2;
--Testcase 361:
select (v->>'c2')::int as c2, count(*) from ft2 where (v->>'c2')::int < 500 group by 1 order by 1;
release savepoint s2;
--Testcase 362:
select (v->>'c2')::int as c2, count(*) from ft2 where (v->>'c2')::int < 500 group by 1 order by 1;
savepoint s3;
-- update ft2 set c2 = -2 where c2 = 42 and v->>'c1' = 10; -- fail on remote side
rollback to savepoint s3;
--Testcase 363:
select (v->>'c2')::int as c2, count(*) from ft2 where (v->>'c2')::int < 500 group by 1 order by 1;
release savepoint s3;
--Testcase 364:
select (v->>'c2')::int as c2, count(*) from ft2 where (v->>'c2')::int < 500 group by 1 order by 1;
-- none of the above is committed yet remotely
--Testcase 365:
select (v->>'c2')::int as c2, count(*) from "S 1"."T1" where (v->>'c2')::int < 500 group by 1 order by 1;
commit;
--Testcase 366:
select (v->>'c2')::int as c2, count(*) from ft2 where (v->>'c2')::int < 500 group by 1 order by 1;
--Testcase 367:
select (v->>'c2')::int as c2, count(*) from "S 1"."T1" where (v->>'c2')::int < 500 group by 1 order by 1;

-- VACUUM ANALYZE "S 1"."T1";

-- Above DMLs add data with v->>'c6' as NULL in ft1, so test ORDER BY NULLS LAST and NULLs
-- FIRST behavior here.
-- ORDER BY DESC NULLS LAST options
\set var :PATH_FILENAME'/ported_postgres/ft1_null.parquet'
--Testcase 368:
CREATE FOREIGN TABLE ft1_null (
	v jsonb
) SERVER parquet_s3_srv
OPTIONS (filename :'var', sorted 'c1', schemaless 'true');

--Testcase 369:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1_null ORDER BY v->>'c6' DESC NULLS LAST, (v->>'c1')::int8 OFFSET 795 LIMIT 10;
--Testcase 370:
SELECT * FROM ft1_null ORDER BY v->>'c6' DESC NULLS LAST, (v->>'c1')::int8 OFFSET 795  LIMIT 10;
-- ORDER BY DESC NULLS FIRST options
--Testcase 371:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1_null ORDER BY v->>'c6' DESC NULLS FIRST, (v->>'c1')::int8 OFFSET 15 LIMIT 10;
--Testcase 372:
SELECT * FROM ft1_null ORDER BY v->>'c6' DESC NULLS FIRST, (v->>'c1')::int8 OFFSET 15 LIMIT 10;
-- ORDER BY ASC NULLS FIRST options
--Testcase 373:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1_null ORDER BY v->>'c6' ASC NULLS FIRST, (v->>'c1')::int8 OFFSET 15 LIMIT 10;
--Testcase 374:
SELECT * FROM ft1_null ORDER BY v->>'c6' ASC NULLS FIRST, (v->>'c1')::int8 OFFSET 15 LIMIT 10;

-- ===================================================================
-- test check constraints
-- ===================================================================

-- Consistent check constraints provide consistent results
--Testcase 559:
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c2positive CHECK ((v->>'c2')::int >= 0);
--Testcase 375:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE (v->>'c2')::int < 0;
--Testcase 376:
SELECT count(*) FROM ft1 WHERE (v->>'c2')::int < 0;
--Testcase 560:
SET constraint_exclusion = 'on';
--Testcase 377:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE (v->>'c2')::int < 0;
--Testcase 378:
SELECT count(*) FROM ft1 WHERE (v->>'c2')::int < 0;
--Testcase 561:
RESET constraint_exclusion;
-- check constraint is enforced on the remote side, not locally
-- INSERT INTO ft1(c1, c2) VALUES(1111, -2);  -- c2positive
-- UPDATE ft1 SET c2 = -c2 WHERE v->>'c1' = 1;  -- c2positive
--Testcase 562:
ALTER FOREIGN TABLE ft1 DROP CONSTRAINT ft1_c2positive;

-- But inconsistent check constraints provide inconsistent results
--Testcase 563:
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c2negative CHECK ((v->>'c2')::int < 0);
--Testcase 379:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE (v->>'c2')::int >= 0;
--Testcase 380:
SELECT count(*) FROM ft1 WHERE (v->>'c2')::int >= 0;
--Testcase 564:
SET constraint_exclusion = 'on';
--Testcase 381:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE (v->>'c2')::int >= 0;
--Testcase 382:
SELECT count(*) FROM ft1 WHERE (v->>'c2')::int >= 0;
--Testcase 565:
RESET constraint_exclusion;
-- local check constraint is not actually enforced
-- INSERT INTO ft1(c1, c2) VALUES(1111, 2);
-- UPDATE ft1 SET c2 = c2 + 1 WHERE v->>'c1' = 1;
--Testcase 566:
ALTER FOREIGN TABLE ft1 DROP CONSTRAINT ft1_c2negative;

-- ===================================================================
-- test WITH CHECK OPTION constraints
-- ===================================================================

--Testcase 383:
CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.v := jsonb_set(NEW.v, '{a}', to_jsonb((NEW.v->>'a')::int + 10), false); RETURN NEW; END$$ LANGUAGE plpgsql;

\set var :PATH_FILENAME'/ported_postgres/base_tbl.parquet'
--Testcase 384:
CREATE FOREIGN TABLE base_tbl (v jsonb)
  SERVER parquet_s3_srv OPTIONS (filename :'var', schemaless 'true', key_columns 'a');
--Testcase 385:
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON base_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();
--Testcase 386:
CREATE FOREIGN TABLE foreign_tbl (v jsonb)
  SERVER parquet_s3_srv OPTIONS (filename :'var', schemaless 'true', key_columns 'a');
--Testcase 387:
CREATE VIEW rw_view AS SELECT * FROM foreign_tbl
  WHERE (v->>'a')::int < (v->>'b')::int WITH CHECK OPTION;
--Testcase 388:
\d+ rw_view

--Testcase 567:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (json_build_object('a', 0, 'b', 5));
--Testcase 568:
INSERT INTO rw_view VALUES (json_build_object('a', 0, 'b', 5)); -- should fail
--Testcase 569:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (json_build_object('a', 0, 'b', 15));
--Testcase 570:
INSERT INTO rw_view VALUES (json_build_object('a', 0, 'b', 15)); -- ok
--Testcase 571:
SELECT * FROM foreign_tbl;

--Testcase 572:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET v = json_build_object('b', (v->>'b')::int + 5);
--Testcase 573:
UPDATE rw_view SET v = json_build_object('b', (v->>'b')::int + 5); -- should fail
--Testcase 574:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET v = json_build_object('b', (v->>'b')::int + 15);
--Testcase 575:
UPDATE rw_view SET v = json_build_object('b', (v->>'b')::int + 15); -- ok
--Testcase 576:
SELECT * FROM foreign_tbl;

--Testcase 389:
DROP FOREIGN TABLE foreign_tbl CASCADE;
--Testcase 390:
DROP TRIGGER row_before_insupd_trigger ON base_tbl;
--Testcase 391:
DROP FOREIGN TABLE base_tbl;

-- test WCO for partitions
-- Postgres does not support create partition by range of jsonb elemnent value.
-- The test below is skipped.
-- \set var :PATH_FILENAME'/ported_postgres/child_tbl.parquet'
-- --Testcase 392:
-- CREATE FOREIGN TABLE child_tbl (v jsonb)
--   SERVER parquet_s3_srv OPTIONS (filename :'var', schemaless 'true');
-- --Testcase 393:
-- CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON child_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();
-- --Testcase 394:
-- CREATE FOREIGN TABLE foreign_tbl (v jsonb)
--   SERVER parquet_s3_srv OPTIONS (filename :'var', schemaless 'true');

-- --Testcase 395:
-- CREATE TABLE parent_tbl (v jsonb) PARTITION BY RANGE((v->>'a')::int);
-- ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);
-- Detach and re-attach once, to stress the concurrent detach case.
--Testcase 449:
-- ALTER TABLE parent_tbl DETACH PARTITION foreign_tbl CONCURRENTLY;
--Testcase 450:
-- ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);

-- --Testcase 396:
-- CREATE VIEW rw_view AS SELECT * FROM parent_tbl
--   WHERE (v->>'a')::int < (v->>'b')::int WITH CHECK OPTION;
-- --Testcase 397:
-- \d+ rw_view

-- -- EXPLAIN (VERBOSE, COSTS OFF)
-- -- INSERT INTO rw_view VALUES (0, 5);
-- -- INSERT INTO rw_view VALUES (0, 5); -- should fail
-- -- EXPLAIN (VERBOSE, COSTS OFF)
-- -- INSERT INTO rw_view VALUES (0, 15);
-- -- INSERT INTO rw_view VALUES (0, 15); -- ok
-- -- SELECT * FROM foreign_tbl;

-- -- EXPLAIN (VERBOSE, COSTS OFF)
-- -- UPDATE rw_view SET b = b + 5;
-- -- UPDATE rw_view SET b = b + 5; -- should fail
-- -- EXPLAIN (VERBOSE, COSTS OFF)
-- -- UPDATE rw_view SET b = b + 15;
-- -- UPDATE rw_view SET b = b + 15; -- ok
-- -- SELECT * FROM foreign_tbl;

-- Batch_size not supported on parquet_s3_fdw
-- We don't allow batch insert when there are any WCO constraints
-- ALTER SERVER loopback OPTIONS (ADD batch_size '10');
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 15), (0, 5);
-- INSERT INTO rw_view VALUES (0, 15), (0, 5); -- should fail
-- SELECT * FROM foreign_tbl;
-- ALTER SERVER loopback OPTIONS (DROP batch_size);

-- --Testcase 398:
-- DROP FOREIGN TABLE foreign_tbl CASCADE;
-- --Testcase 399:
-- DROP TRIGGER row_before_insupd_trigger ON child_tbl;
-- --Testcase 400:
-- DROP TABLE parent_tbl CASCADE;

--Testcase 401:
DROP FUNCTION row_before_insupd_trigfunc;


-- ===================================================================
-- test serial columns (ie, sequence-based defaults)
-- schemaless-mode does not has serial column
-- ===================================================================
\set var :PATH_FILENAME'/ported_postgres/loc1.parquet'
--Testcase 577:
create foreign table loc1 (v jsonb)
  server parquet_s3_srv options(filename :'var', schemaless 'true', key_columns 'f1');

--Testcase 578:
create foreign table rem1 (v jsonb)
  server parquet_s3_srv options(filename :'var', schemaless 'true', key_columns 'f1');
-- select pg_catalog.setval('rem1_f1_seq', 10, false);
--Testcase 579:
insert into loc1 values (json_build_object('f1', 1, 'f2', 'hi'));
--Testcase 580:
insert into rem1 values (json_build_object('f1', 10, 'f2', 'hi remote'));
--Testcase 581:
insert into loc1 values (json_build_object('f1', 2, 'f2', 'bye'));
--Testcase 582:
insert into rem1 values (json_build_object('f1', 11, 'f2', 'bye remote'));
--Testcase 583:
select * from rem1;

-- ===================================================================
-- test generated columns
-- schemaless-mode does not has generated column
-- ===================================================================
\set var :PATH_FILENAME'/ported_postgres/gloc1.parquet'
--Testcase 584:
create foreign table gloc1 (
  v JSONB)
  server parquet_s3_srv options(filename :'var', schemaless 'true', key_columns 'a');
--Testcase 585:
create foreign table grem1 (
  v JSONB)
  server parquet_s3_srv options(filename :'var', schemaless 'true', key_columns 'a');
-- explain (verbose, costs off)
-- insert into grem1 (a) values (1), (2);
-- insert into grem1 (a) values (1), (2);
-- explain (verbose, costs off)
-- update grem1 set a = 22 where a = 2;
-- update grem1 set a = 22 where a = 2;
-- select * from gloc1;
-- select * from grem1;
--Testcase 586:
delete from grem1;

-- parquet_s3_fdw does not support copy from
-- -- test copy from
-- copy grem1 from stdin;
-- 1
-- 2
-- \.
-- select * from gloc1;
-- select * from grem1;
-- delete from grem1;

-- parquet_s3_fdw does not support batch insert
-- -- test batch insert
-- alter server loopback options (add batch_size '10');
-- explain (verbose, costs off)
-- insert into grem1 (a) values (1), (2);
-- insert into grem1 (a) values (1), (2);
-- select * from gloc1;
-- select * from grem1;
-- delete from grem1;
-- alter server loopback options (drop batch_size);

-- ===================================================================
-- test local triggers
-- ===================================================================

-- Trigger functions "borrowed" from triggers regress test.
--Testcase 587:
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

--Testcase 588:
CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 589:
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 590:
CREATE OR REPLACE FUNCTION trigger_data()  RETURNS trigger
LANGUAGE plpgsql AS $$

declare
	oldnew text[];
	relid text;
    argstr text;
begin

	relid := TG_relid::regclass;
	argstr := '';
	for i in 0 .. TG_nargs - 1 loop
		if i > 0 then
			argstr := argstr || ', ';
		end if;
		argstr := argstr || TG_argv[i];
	end loop;

    RAISE NOTICE '%(%) % % % ON %',
		tg_name, argstr, TG_when, TG_level, TG_OP, relid;
    oldnew := '{}'::text[];
	if TG_OP != 'INSERT' then
		oldnew := array_append(oldnew, format('OLD: %s', OLD));
	end if;

	if TG_OP != 'DELETE' then
		oldnew := array_append(oldnew, format('NEW: %s', NEW));
	end if;

    RAISE NOTICE '%', array_to_string(oldnew, ',');

	if TG_OP = 'DELETE' then
		return OLD;
	else
		return NEW;
	end if;
end;
$$;

-- Test basic functionality
--Testcase 591:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 592:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 593:
delete from rem1;
--Testcase 594:
insert into rem1 values (json_build_object('f1', 1, 'f2', 'insert'));
--Testcase 595:
update rem1 set v = json_build_object('f2', 'update') where (v->>'f1')::int = 1;
--Testcase 596:
update rem1 set v = json_build_object('f2', (v->>'f2') || (v->>'f2'));


-- cleanup
--Testcase 597:
DROP TRIGGER trig_row_before ON rem1;
--Testcase 598:
DROP TRIGGER trig_row_after ON rem1;
--Testcase 599:
DROP TRIGGER trig_stmt_before ON rem1;
--Testcase 600:
DROP TRIGGER trig_stmt_after ON rem1;

--Testcase 601:
DELETE from rem1;

-- Test multiple AFTER ROW triggers on a foreign table
--Testcase 602:
CREATE TRIGGER trig_row_after1
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 603:
CREATE TRIGGER trig_row_after2
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 604:
insert into rem1 values (json_build_object('f1', 1, 'f2', 'insert'));
--Testcase 605:
update rem1 set v = json_build_object('f2', 'update') where (v->>'f1')::int = 1;
--Testcase 606:
update rem1 set v = json_build_object('f2', (v->>'f2') || (v->>'f2'));
--Testcase 607:
delete from rem1;

-- cleanup
--Testcase 608:
DROP TRIGGER trig_row_after1 ON rem1;
--Testcase 609:
DROP TRIGGER trig_row_after2 ON rem1;

-- Test WHEN conditions

--Testcase 610:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW
WHEN (NEW.v->>'f2' like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 611:
CREATE TRIGGER trig_row_after_insupd
AFTER INSERT OR UPDATE ON rem1
FOR EACH ROW
WHEN (NEW.v->>'f2' like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Insert or update not matching: nothing happens
--Testcase 612:
INSERT INTO rem1 values (json_build_object('f1', 1, 'f2', 'insert'));
--Testcase 613:
UPDATE rem1 set v = json_build_object('f2', 'test');

-- Insert or update matching: triggers are fired
--Testcase 614:
INSERT INTO rem1 values (json_build_object('f1', 2, 'f2', 'update'));
--Testcase 615:
UPDATE rem1 set v = json_build_object('f2', 'update update') where v->>'f1' = '2';

--Testcase 616:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1
FOR EACH ROW
WHEN (OLD.v->>'f2' like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 617:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1
FOR EACH ROW
WHEN (OLD.v->>'f2' like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Trigger is fired for f1=2, not for f1=1
--Testcase 618:
DELETE FROM rem1;

-- cleanup
--Testcase 619:
DROP TRIGGER trig_row_before_insupd ON rem1;
--Testcase 620:
DROP TRIGGER trig_row_after_insupd ON rem1;
--Testcase 621:
DROP TRIGGER trig_row_before_delete ON rem1;
--Testcase 622:
DROP TRIGGER trig_row_after_delete ON rem1;

-- Test various RETURN statements in BEFORE triggers.

--Testcase 623:
CREATE FUNCTION trig_row_before_insupdate() RETURNS TRIGGER AS $$
  BEGIN
    NEW.v := jsonb_set(NEW.v, '{f2}', to_jsonb(NEW.v->>'f2' || ' triggered !'), false);
    RETURN NEW;
  END
$$ language plpgsql;

--Testcase 624:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- The new values should have 'triggered' appended
--Testcase 625:
INSERT INTO rem1 values (json_build_object('f1', 1, 'f2', 'insert'));
--Testcase 626:
SELECT * from loc1;
--Testcase 627:
INSERT INTO rem1 values (json_build_object('f1', 2, 'f2', 'insert'));
--Testcase 628:
SELECT * from loc1;
--Testcase 629:
UPDATE rem1 set v = json_build_object('f2', '');
--Testcase 630:
SELECT * from loc1;
--Testcase 631:
UPDATE rem1 set v = json_build_object('f2', 'skidoo');
--Testcase 632:
SELECT * from loc1;

--Testcase 633:
EXPLAIN (verbose, costs off)
UPDATE rem1 set v = json_build_object('f1', 10);
--Testcase 634:
UPDATE rem1 set v = json_build_object('f1', 10);
--Testcase 635:
SELECT * from loc1;

--Testcase 636:
DELETE FROM rem1;

-- Add a second trigger, to check that the changes are propagated correctly
-- from trigger to trigger
--Testcase 637:
CREATE TRIGGER trig_row_before_insupd2
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 638:
INSERT INTO rem1 values (json_build_object('f1', 1, 'f2', 'insert'));
--Testcase 639:
SELECT * from loc1;
--Testcase 640:
INSERT INTO rem1 values (json_build_object('f1', 2, 'f2', 'insert'));
--Testcase 641:
SELECT * from loc1;
--Testcase 642:
UPDATE rem1 set v = json_build_object('f2', '');
--Testcase 643:
SELECT * from loc1;
--Testcase 644:
UPDATE rem1 set v = json_build_object('f2', 'skidoo');
--Testcase 645:
SELECT * from loc1;

--Testcase 646:
DROP TRIGGER trig_row_before_insupd ON rem1;
--Testcase 647:
DROP TRIGGER trig_row_before_insupd2 ON rem1;

--Testcase 648:
DELETE from rem1;

--Testcase 649:
INSERT INTO rem1 VALUES (json_build_object('f1', 1, 'f2', 'test'));

-- Test with a trigger returning NULL
--Testcase 650:
CREATE FUNCTION trig_null() RETURNS TRIGGER AS $$
  BEGIN
    RETURN NULL;
  END
$$ language plpgsql;

--Testcase 651:
CREATE TRIGGER trig_null
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_null();

-- Nothing should have changed.
--Testcase 652:
INSERT INTO rem1 VALUES (json_build_object('f1', 2, 'f2', 'test2'));

--Testcase 653:
SELECT * from loc1;

--Testcase 654:
UPDATE rem1 SET v = json_build_object('f2', 'test2');

--Testcase 655:
SELECT * from loc1;

--Testcase 656:
DELETE from rem1;

--Testcase 657:
SELECT * from loc1;

--Testcase 658:
DROP TRIGGER trig_null ON rem1;
--Testcase 659:
DELETE from rem1;

-- Test a combination of local and remote triggers
--Testcase 660:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 661:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 662:
CREATE TRIGGER trig_local_before BEFORE INSERT OR UPDATE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 663:
INSERT INTO rem1 VALUES (json_build_object('f1', 12, 'f2', 'test'));
--Testcase 664:
UPDATE rem1 SET v = json_build_object('f2', 'testo');

-- Test returning a system attribute
--Testcase 665:
INSERT INTO rem1 VALUES (json_build_object('f1', 13, 'f2', 'test'));

-- cleanup
--Testcase 666:
DROP TRIGGER trig_row_before ON rem1;
--Testcase 667:
DROP TRIGGER trig_row_after ON rem1;
--Testcase 668:
DROP TRIGGER trig_local_before ON loc1;


-- Test direct foreign table modification functionality
--Testcase 669:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;
--Testcase 670:
EXPLAIN (verbose, costs off)
DELETE FROM rem1 WHERE false;     -- currently can't be pushed down

-- Test with statement-level triggers
--Testcase 671:
CREATE TRIGGER trig_stmt_before
	BEFORE DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 672:
EXPLAIN (verbose, costs off)
UPDATE rem1 set v = json_build_object('f2', '');
--Testcase 673:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;
--Testcase 674:
DROP TRIGGER trig_stmt_before ON rem1;

--Testcase 675:
CREATE TRIGGER trig_stmt_after
	AFTER DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 676:
EXPLAIN (verbose, costs off)
UPDATE rem1 set v = json_build_object('f2', '');
--Testcase 677:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;
--Testcase 678:
DROP TRIGGER trig_stmt_after ON rem1;

-- Test with row-level ON INSERT triggers
--Testcase 679:
CREATE TRIGGER trig_row_before_insert
BEFORE INSERT ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 680:
EXPLAIN (verbose, costs off)
UPDATE rem1 set v = json_build_object('f2', '');
--Testcase 681:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;
--Testcase 682:
DROP TRIGGER trig_row_before_insert ON rem1;

--Testcase 683:
CREATE TRIGGER trig_row_after_insert
AFTER INSERT ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 684:
EXPLAIN (verbose, costs off)
UPDATE rem1 set v = json_build_object('f2', ' ');
--Testcase 685:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;
--Testcase 686:
DROP TRIGGER trig_row_after_insert ON rem1;

-- Test with row-level ON UPDATE triggers
--Testcase 687:
CREATE TRIGGER trig_row_before_update
BEFORE UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 688:
EXPLAIN (verbose, costs off)
UPDATE rem1 set v = json_build_object('f2', '');        -- can't be pushed down
--Testcase 689:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;
--Testcase 690:
DROP TRIGGER trig_row_before_update ON rem1;

--Testcase 691:
CREATE TRIGGER trig_row_after_update
AFTER UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 692:
EXPLAIN (verbose, costs off)
UPDATE rem1 set v = json_build_object('f2', '');         -- can't be pushed down
--Testcase 693:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;
--Testcase 694:
DROP TRIGGER trig_row_after_update ON rem1;

-- Test with row-level ON DELETE triggers
--Testcase 695:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 696:
EXPLAIN (verbose, costs off)
UPDATE rem1 set v = json_build_object('f2', '');
--Testcase 697:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can't be pushed down
--Testcase 698:
DROP TRIGGER trig_row_before_delete ON rem1;

--Testcase 699:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 700:
EXPLAIN (verbose, costs off)
UPDATE rem1 set v = json_build_object('f2', '');
--Testcase 701:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can't be pushed down
--Testcase 702:
DROP TRIGGER trig_row_after_delete ON rem1;

-- ===================================================================
-- test inheritance features
-- schemaless mode has only one column ==> can not test
-- ===================================================================
-- CREATE TABLE a (aa TEXT);
-- CREATE TABLE loct (aa TEXT, bb TEXT);
-- ALTER TABLE a SET (autovacuum_enabled = 'false');
-- ALTER TABLE loct SET (autovacuum_enabled = 'false');
-- CREATE FOREIGN TABLE b (bb TEXT) INHERITS (a)
--   SERVER loopback OPTIONS (table_name 'loct');

-- INSERT INTO a(aa) VALUES('aaa');
-- INSERT INTO a(aa) VALUES('aaaa');
-- INSERT INTO a(aa) VALUES('aaaaa');

-- INSERT INTO b(aa) VALUES('bbb');
-- INSERT INTO b(aa) VALUES('bbbb');
-- INSERT INTO b(aa) VALUES('bbbbb');

-- SELECT tableoid::regclass, * FROM a;
-- SELECT tableoid::regclass, * FROM b;
-- SELECT tableoid::regclass, * FROM ONLY a;

-- UPDATE a SET aa = 'zzzzzz' WHERE aa LIKE 'aaaa%';

-- SELECT tableoid::regclass, * FROM a;
-- SELECT tableoid::regclass, * FROM b;
-- SELECT tableoid::regclass, * FROM ONLY a;

-- UPDATE b SET aa = 'new';

-- SELECT tableoid::regclass, * FROM a;
-- SELECT tableoid::regclass, * FROM b;
-- SELECT tableoid::regclass, * FROM ONLY a;

-- UPDATE a SET aa = 'newtoo';

-- SELECT tableoid::regclass, * FROM a;
-- SELECT tableoid::regclass, * FROM b;
-- SELECT tableoid::regclass, * FROM ONLY a;

-- DELETE FROM a;

-- SELECT tableoid::regclass, * FROM a;
-- SELECT tableoid::regclass, * FROM b;
-- SELECT tableoid::regclass, * FROM ONLY a;

-- DROP TABLE a CASCADE;
-- DROP TABLE loct;

-- Check SELECT FOR UPDATE/SHARE with an inherited source table
-- create table loct1 (f1 int, f2 int, f3 int);
-- create table loct2 (f1 int, f2 int, f3 int);

-- alter table loct1 set (autovacuum_enabled = 'false');
-- alter table loct2 set (autovacuum_enabled = 'false');

-- create table foo (f1 int, f2 int);
-- create foreign table foo2 (f3 int) inherits (foo)
--   server loopback options (table_name 'loct1');
-- create table bar (f1 int, f2 int);
-- create foreign table bar2 (f3 int) inherits (bar)
--   server loopback options (table_name 'loct2');

-- alter table foo set (autovacuum_enabled = 'false');
-- alter table bar set (autovacuum_enabled = 'false');

-- insert into foo values(1,1);
-- insert into foo values(3,3);
-- insert into foo2 values(2,2,2);
-- insert into foo2 values(4,4,4);
-- insert into bar values(1,11);
-- insert into bar values(2,22);
-- insert into bar values(6,66);
-- insert into bar2 values(3,33,33);
-- insert into bar2 values(4,44,44);
-- insert into bar2 values(7,77,77);

-- explain (verbose, costs off)
-- select * from bar where f1 in (select f1 from foo) for update;
-- select * from bar where f1 in (select f1 from foo) for update;

-- explain (verbose, costs off)
-- select * from bar where f1 in (select f1 from foo) for share;
-- select * from bar where f1 in (select f1 from foo) for share;

-- -- Now check SELECT FOR UPDATE/SHARE with an inherited source table,
-- -- where the parent is itself a foreign table
-- create table loct4 (f1 int, f2 int, f3 int);
-- create foreign table foo2child (f3 int) inherits (foo2)
--   server loopback options (table_name 'loct4');

-- explain (verbose, costs off)
-- select * from bar where f1 in (select f1 from foo2) for share;
-- select * from bar where f1 in (select f1 from foo2) for share;

-- drop foreign table foo2child;

-- -- And with a local child relation of the foreign table parent
-- create table foo2child (f3 int) inherits (foo2);

-- explain (verbose, costs off)
-- select * from bar where f1 in (select f1 from foo2) for share;
-- select * from bar where f1 in (select f1 from foo2) for share;

-- drop table foo2child;

-- -- Check UPDATE with inherited target and an inherited source table
-- explain (verbose, costs off)
-- update bar set f2 = f2 + 100 where f1 in (select f1 from foo);
-- update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

-- select tableoid::regclass, * from bar order by 1,2;

-- -- Check UPDATE with inherited target and an appendrel subquery
-- explain (verbose, costs off)
-- update bar set f2 = f2 + 100
-- from
--   ( select f1 from foo union all select f1+3 from foo ) ss
-- where bar.f1 = ss.f1;
-- update bar set f2 = f2 + 100
-- from
--   ( select f1 from foo union all select f1+3 from foo ) ss
-- where bar.f1 = ss.f1;

-- select tableoid::regclass, * from bar order by 1,2;

-- Test forcing the remote server to produce sorted data for a merge join,
-- but the foreign table is an inheritance child.
-- truncate table loct1;
-- truncate table only foo;
-- \set num_rows_foo 2000
-- insert into loct1 select generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2);
-- insert into foo select generate_series(1, :num_rows_foo, 2), generate_series(1, :num_rows_foo, 2);
-- SET enable_hashjoin to false;
-- SET enable_nestloop to false;
-- alter foreign table foo2 options (use_remote_estimate 'true');
-- create index i_loct1_f1 on loct1(f1);
-- create index i_foo_f1 on foo(f1);
-- analyze foo;
-- analyze loct1;
-- inner join; expressions in the clauses appear in the equivalence class list
-- explain (verbose, costs off)
-- 	select foo.f1, loct1.f1 from foo join loct1 on (foo.f1 = loct1.f1) order by foo.f2 offset 10 limit 10;
-- select foo.f1, loct1.f1 from foo join loct1 on (foo.f1 = loct1.f1) order by foo.f2 offset 10 limit 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
-- explain (verbose, costs off)
-- 	select foo.f1, loct1.f1 from foo left join loct1 on (foo.f1 = loct1.f1) order by foo.f2 offset 10 limit 10;
-- select foo.f1, loct1.f1 from foo left join loct1 on (foo.f1 = loct1.f1) order by foo.f2 offset 10 limit 10;
-- RESET enable_hashjoin;
-- RESET enable_nestloop;

-- Test that WHERE CURRENT OF is not supported
-- begin;
-- declare c cursor for select * from bar where f1 = 7;
-- fetch from c;
-- update bar set f2 = null where current of c;
-- rollback;

-- explain (verbose, costs off)
-- delete from foo where f1 < 5 returning *;
-- delete from foo where f1 < 5 returning *;
-- explain (verbose, costs off)
-- update bar set f2 = f2 + 100 returning *;
-- update bar set f2 = f2 + 100 returning *;

-- Test that UPDATE/DELETE with inherited target works with row-level triggers
-- CREATE TRIGGER trig_row_before
-- BEFORE UPDATE OR DELETE ON bar2
-- FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- CREATE TRIGGER trig_row_after
-- AFTER UPDATE OR DELETE ON bar2
-- FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- explain (verbose, costs off)
-- update bar set f2 = f2 + 100;
-- update bar set f2 = f2 + 100;

-- explain (verbose, costs off)
-- delete from bar where f2 < 400;
-- delete from bar where f2 < 400;

-- cleanup
-- drop table foo cascade;
-- drop table bar cascade;
-- drop table loct1;
-- drop table loct2;

-- Test pushing down UPDATE/DELETE joins to the remote server
-- create table parent (a int, b text);
-- create table loct1 (a int, b text);
-- create table loct2 (a int, b text);
-- create foreign table remt1 (a int, b text)
--   server loopback options (table_name 'loct1');
-- create foreign table remt2 (a int, b text)
--   server loopback options (table_name 'loct2');
-- alter foreign table remt1 inherit parent;

-- insert into remt1 values (1, 'foo');
-- insert into remt1 values (2, 'bar');
-- insert into remt2 values (1, 'foo');
-- insert into remt2 values (2, 'bar');

-- analyze remt1;
-- analyze remt2;

-- explain (verbose, costs off)
-- update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a returning *;
-- update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a returning *;
-- explain (verbose, costs off)
-- delete from parent using remt2 where parent.a = remt2.a returning parent;
-- delete from parent using remt2 where parent.a = remt2.a returning parent;

-- cleanup
-- drop foreign table remt1;
-- drop foreign table remt2;
-- drop table loct1;
-- drop table loct2;
-- drop table parent;

-- ===================================================================
-- test tuple routing for foreign-table partitions
-- ===================================================================

-- Test insert tuple routing
-- create table itrtest (a int, b text) partition by list (a);
-- create table loct1 (a int check (a in (1)), b text);
-- create foreign table remp1 (a int check (a in (1)), b text) server loopback options (table_name 'loct1');
-- create table loct2 (a int check (a in (2)), b text);
-- create foreign table remp2 (b text, a int check (a in (2))) server loopback options (table_name 'loct2');
-- alter table itrtest attach partition remp1 for values in (1);
-- alter table itrtest attach partition remp2 for values in (2);

-- insert into itrtest values (1, 'foo');
-- insert into itrtest values (1, 'bar') returning *;
-- insert into itrtest values (2, 'baz');
-- insert into itrtest values (2, 'qux') returning *;
-- insert into itrtest values (1, 'test1'), (2, 'test2') returning *;

-- select tableoid::regclass, * FROM itrtest;
-- select tableoid::regclass, * FROM remp1;
-- select tableoid::regclass, * FROM remp2;

-- delete from itrtest;

-- create unique index loct1_idx on loct1 (a);

-- DO NOTHING without an inference specification is supported
-- insert into itrtest values (1, 'foo') on conflict do nothing returning *;
-- insert into itrtest values (1, 'foo') on conflict do nothing returning *;

-- But other cases are not supported
-- insert into itrtest values (1, 'bar') on conflict (a) do nothing;
-- insert into itrtest values (1, 'bar') on conflict (a) do update set b = excluded.b;

-- select tableoid::regclass, * FROM itrtest;

-- delete from itrtest;

-- drop index loct1_idx;

-- Test that remote triggers work with insert tuple routing
-- create function br_insert_trigfunc() returns trigger as $$
-- begin
-- 	new.b := new.b || ' triggered !';
-- 	return new;
-- end
-- $$ language plpgsql;
-- create trigger loct1_br_insert_trigger before insert on loct1
-- 	for each row execute procedure br_insert_trigfunc();
-- create trigger loct2_br_insert_trigger before insert on loct2
-- 	for each row execute procedure br_insert_trigfunc();

-- The new values are concatenated with ' triggered !'
-- insert into itrtest values (1, 'foo') returning *;
-- insert into itrtest values (2, 'qux') returning *;
-- insert into itrtest values (1, 'test1'), (2, 'test2') returning *;
-- with result as (insert into itrtest values (1, 'test1'), (2, 'test2') returning *) select * from result;

-- drop trigger loct1_br_insert_trigger on loct1;
-- drop trigger loct2_br_insert_trigger on loct2;

-- drop table itrtest;
-- drop table loct1;
-- drop table loct2;

-- Test update tuple routing
-- create table utrtest (a int, b text) partition by list (a);
-- create table loct (a int check (a in (1)), b text);
-- create foreign table remp (a int check (a in (1)), b text) server loopback options (table_name 'loct');
-- create table locp (a int check (a in (2)), b text);
-- alter table utrtest attach partition remp for values in (1);
-- alter table utrtest attach partition locp for values in (2);

-- insert into utrtest values (1, 'foo');
-- insert into utrtest values (2, 'qux');

-- select tableoid::regclass, * FROM utrtest;
-- select tableoid::regclass, * FROM remp;
-- select tableoid::regclass, * FROM locp;

-- It's not allowed to move a row from a partition that is foreign to another
-- update utrtest set a = 2 where b = 'foo' returning *;

-- But the reverse is allowed
-- update utrtest set a = 1 where b = 'qux' returning *;

-- select tableoid::regclass, * FROM utrtest;
-- select tableoid::regclass, * FROM remp;
-- select tableoid::regclass, * FROM locp;

-- The executor should not let unexercised FDWs shut down
-- update utrtest set a = 1 where b = 'foo';

-- Test that remote triggers work with update tuple routing
-- create trigger loct_br_insert_trigger before insert on loct
-- 	for each row execute procedure br_insert_trigfunc();

-- delete from utrtest;
-- insert into utrtest values (2, 'qux');

-- Check case where the foreign partition is a subplan target rel
-- explain (verbose, costs off)
-- update utrtest set a = 1 where a = 1 or a = 2 returning *;
-- The new values are concatenated with ' triggered !'
-- update utrtest set a = 1 where a = 1 or a = 2 returning *;

-- delete from utrtest;
-- insert into utrtest values (2, 'qux');

-- Check case where the foreign partition isn't a subplan target rel
-- explain (verbose, costs off)
-- update utrtest set a = 1 where a = 2 returning *;
-- The new values are concatenated with ' triggered !'
-- update utrtest set a = 1 where a = 2 returning *;

-- drop trigger loct_br_insert_trigger on loct;

-- We can move rows to a foreign partition that has been updated already,
-- but can't move rows to a foreign partition that hasn't been updated yet

-- delete from utrtest;
-- insert into utrtest values (1, 'foo');
-- insert into utrtest values (2, 'qux');

-- Test the former case:
-- with a direct modification plan
-- explain (verbose, costs off)
-- update utrtest set a = 1 returning *;
-- update utrtest set a = 1 returning *;

-- delete from utrtest;
-- insert into utrtest values (1, 'foo');
-- insert into utrtest values (2, 'qux');

-- with a non-direct modification plan
-- explain (verbose, costs off)
-- update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x returning *;
-- update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x returning *;

-- Change the definition of utrtest so that the foreign partition get updated
-- after the local partition
-- delete from utrtest;
-- alter table utrtest detach partition remp;
-- drop foreign table remp;
-- alter table loct drop constraint loct_a_check;
-- alter table loct add check (a in (3));
-- create foreign table remp (a int check (a in (3)), b text) server loopback options (table_name 'loct');
-- alter table utrtest attach partition remp for values in (3);
-- insert into utrtest values (2, 'qux');
-- insert into utrtest values (3, 'xyzzy');

-- Test the latter case:
-- with a direct modification plan
-- explain (verbose, costs off)
-- update utrtest set a = 3 returning *;
-- update utrtest set a = 3 returning *; -- ERROR

-- with a non-direct modification plan
-- explain (verbose, costs off)
-- update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x returning *;
-- update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x returning *; -- ERROR

-- drop table utrtest;
-- drop table loct;

-- Test copy tuple routing
-- create table ctrtest (a int, b text) partition by list (a);
-- create table loct1 (a int check (a in (1)), b text);
-- create foreign table remp1 (a int check (a in (1)), b text) server loopback options (table_name 'loct1');
-- create table loct2 (a int check (a in (2)), b text);
-- create foreign table remp2 (b text, a int check (a in (2))) server loopback options (table_name 'loct2');
-- alter table ctrtest attach partition remp1 for values in (1);
-- alter table ctrtest attach partition remp2 for values in (2);

-- copy ctrtest from stdin;
-- 1	foo
-- 2	qux
-- \.

-- select tableoid::regclass, * FROM ctrtest;
-- select tableoid::regclass, * FROM remp1;
-- select tableoid::regclass, * FROM remp2;

-- Copying into foreign partitions directly should work as well
-- copy remp1 from stdin;
-- 1	bar
-- \.

-- select tableoid::regclass, * FROM remp1;

-- drop table ctrtest;
-- drop table loct1;
-- drop table loct2;

-- ===================================================================
-- test COPY FROM
-- ===================================================================

-- create table loc2 (f1 int, f2 text);
-- alter table loc2 set (autovacuum_enabled = 'false');
-- create foreign table rem2 (f1 int, f2 text) server loopback options(table_name 'loc2');

-- Test basic functionality
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- delete from rem2;

-- Test check constraints
-- alter table loc2 add constraint loc2_f1positive check (f1 >= 0);
-- alter foreign table rem2 add constraint rem2_f1positive check (f1 >= 0);

-- check constraint is enforced on the remote side, not locally
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- copy rem2 from stdin; -- ERROR
-- -1	xyzzy
-- \.
-- select * from rem2;

-- alter foreign table rem2 drop constraint rem2_f1positive;
-- alter table loc2 drop constraint loc2_f1positive;

-- delete from rem2;

-- Test local triggers
-- create trigger trig_stmt_before before insert on rem2
-- 	for each statement execute procedure trigger_func();
-- create trigger trig_stmt_after after insert on rem2
-- 	for each statement execute procedure trigger_func();
-- create trigger trig_row_before before insert on rem2
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- create trigger trig_row_after after insert on rem2
-- 	for each row execute procedure trigger_data(23,'skidoo');

-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_row_before on rem2;
-- drop trigger trig_row_after on rem2;
-- drop trigger trig_stmt_before on rem2;
-- drop trigger trig_stmt_after on rem2;

-- delete from rem2;

-- create trigger trig_row_before_insert before insert on rem2
-- 	for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_row_before_insert on rem2;

-- delete from rem2;

-- create trigger trig_null before insert on rem2
-- 	for each row execute procedure trig_null();

-- Nothing happens
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_null on rem2;

-- delete from rem2;

-- Test remote triggers
-- create trigger trig_row_before_insert before insert on loc2
-- 	for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_row_before_insert on loc2;

-- delete from rem2;

-- create trigger trig_null before insert on loc2
-- 	for each row execute procedure trig_null();

-- Nothing happens
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_null on loc2;

-- delete from rem2;

-- Test a combination of local and remote triggers
-- create trigger rem2_trig_row_before before insert on rem2
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- create trigger rem2_trig_row_after after insert on rem2
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- create trigger loc2_trig_row_before_insert before insert on loc2
-- 	for each row execute procedure trig_row_before_insupdate();

-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger rem2_trig_row_before on rem2;
-- drop trigger rem2_trig_row_after on rem2;
-- drop trigger loc2_trig_row_before_insert on loc2;

-- delete from rem2;

-- test COPY FROM with foreign table created in the same transaction
-- create table loc3 (f1 int, f2 text);
-- begin;
-- create foreign table rem3 (f1 int, f2 text)
-- 	server loopback options(table_name 'loc3');
-- copy rem3 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- commit;
-- select * from rem3;
-- drop foreign table rem3;
-- drop table loc3;

-- ===================================================================
-- test for TRUNCATE
-- ===================================================================
-- CREATE TABLE tru_rtable0 (id int primary key);
-- CREATE FOREIGN TABLE tru_ftable (id int)
--        SERVER loopback OPTIONS (table_name 'tru_rtable0');
-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(1,10) x);

-- CREATE TABLE tru_ptable (id int) PARTITION BY HASH(id);
-- CREATE TABLE tru_ptable__p0 PARTITION OF tru_ptable
--                             FOR VALUES WITH (MODULUS 2, REMAINDER 0);
-- CREATE TABLE tru_rtable1 (id int primary key);
-- CREATE FOREIGN TABLE tru_ftable__p1 PARTITION OF tru_ptable
--                                     FOR VALUES WITH (MODULUS 2, REMAINDER 1)
--        SERVER loopback OPTIONS (table_name 'tru_rtable1');
-- INSERT INTO tru_ptable (SELECT x FROM generate_series(11,20) x);

-- CREATE TABLE tru_pk_table(id int primary key);
-- CREATE TABLE tru_fk_table(fkey int references tru_pk_table(id));
-- INSERT INTO tru_pk_table (SELECT x FROM generate_series(1,10) x);
-- INSERT INTO tru_fk_table (SELECT x % 10 + 1 FROM generate_series(5,25) x);
-- CREATE FOREIGN TABLE tru_pk_ftable (id int)
--        SERVER loopback OPTIONS (table_name 'tru_pk_table');

-- CREATE TABLE tru_rtable_parent (id int);
-- CREATE TABLE tru_rtable_child (id int);
-- CREATE FOREIGN TABLE tru_ftable_parent (id int)
--        SERVER loopback OPTIONS (table_name 'tru_rtable_parent');
-- CREATE FOREIGN TABLE tru_ftable_child () INHERITS (tru_ftable_parent)
--        SERVER loopback OPTIONS (table_name 'tru_rtable_child');
-- INSERT INTO tru_rtable_parent (SELECT x FROM generate_series(1,8) x);
-- INSERT INTO tru_rtable_child  (SELECT x FROM generate_series(10, 18) x);

-- normal truncate
-- SELECT sum(id) FROM tru_ftable;        -- 55
-- TRUNCATE tru_ftable;
-- SELECT count(*) FROM tru_rtable0;		-- 0
-- SELECT count(*) FROM tru_ftable;		-- 0

-- 'truncatable' option
-- ALTER SERVER loopback OPTIONS (ADD truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (ADD truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER SERVER loopback OPTIONS (DROP truncatable);
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted

-- partitioned table with both local and foreign tables as partitions
-- SELECT sum(id) FROM tru_ptable;        -- 155
-- TRUNCATE tru_ptable;
-- SELECT count(*) FROM tru_ptable;		-- 0
-- SELECT count(*) FROM tru_ptable__p0;	-- 0
-- SELECT count(*) FROM tru_ftable__p1;	-- 0
-- SELECT count(*) FROM tru_rtable1;		-- 0

-- 'CASCADE' option
-- SELECT sum(id) FROM tru_pk_ftable;      -- 55
-- TRUNCATE tru_pk_ftable;	-- failed by FK reference
-- TRUNCATE tru_pk_ftable CASCADE;
-- SELECT count(*) FROM tru_pk_ftable;    -- 0
-- SELECT count(*) FROM tru_fk_table;		-- also truncated,0

-- truncate two tables at a command
-- INSERT INTO tru_ftable (SELECT x FROM generate_series(1,8) x);
-- INSERT INTO tru_pk_ftable (SELECT x FROM generate_series(3,10) x);
-- SELECT count(*) from tru_ftable; -- 8
-- SELECT count(*) from tru_pk_ftable; -- 8
-- TRUNCATE tru_ftable, tru_pk_ftable CASCADE;
-- SELECT count(*) from tru_ftable; -- 0
-- SELECT count(*) from tru_pk_ftable; -- 0

-- truncate with ONLY clause
-- Since ONLY is specified, the table tru_ftable_child that inherits
-- tru_ftable_parent locally is not truncated.
-- TRUNCATE ONLY tru_ftable_parent;
-- SELECT sum(id) FROM tru_ftable_parent;  -- 126
-- TRUNCATE tru_ftable_parent;
-- SELECT count(*) FROM tru_ftable_parent; -- 0

-- in case when remote table has inherited children
-- CREATE TABLE tru_rtable0_child () INHERITS (tru_rtable0);
-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(5,9) x);
-- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(10,14) x);
-- SELECT sum(id) FROM tru_ftable;   -- 95

-- Both parent and child tables in the foreign server are truncated
-- even though ONLY is specified because ONLY has no effect
-- when truncating a foreign table.
-- TRUNCATE ONLY tru_ftable;
-- SELECT count(*) FROM tru_ftable;   -- 0

-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(21,25) x);
-- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(26,30) x);
-- SELECT sum(id) FROM tru_ftable;		-- 255
-- TRUNCATE tru_ftable;			-- truncate both of parent and child
-- SELECT count(*) FROM tru_ftable;    -- 0

-- cleanup
-- DROP FOREIGN TABLE tru_ftable_parent, tru_ftable_child, tru_pk_ftable,tru_ftable__p1,tru_ftable;
-- DROP TABLE tru_rtable0, tru_rtable1, tru_ptable, tru_ptable__p0, tru_pk_table, tru_fk_table,
-- tru_rtable_parent,tru_rtable_child, tru_rtable0_child;

-- ===================================================================
-- test IMPORT FOREIGN SCHEMA
-- ===================================================================
\set var '\"':PATH_FILENAME'\/ported_postgres\"'
--Testcase 402:
CREATE SCHEMA import_dest1;
IMPORT FOREIGN SCHEMA :var FROM SERVER parquet_s3_srv INTO import_dest1 OPTIONS (sorted 'c1', schemaless 'true');
--Testcase 403:
\det+ import_dest1.*
--Testcase 404:
\d import_dest1.*

-- Options
-- CREATE SCHEMA import_dest2;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER loopback INTO import_dest2
--   OPTIONS (import_default 'true');
-- \det+ import_dest2.*
-- \d import_dest2.*
-- CREATE SCHEMA import_dest3;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER loopback INTO import_dest3
--   OPTIONS (import_collate 'false', import_generated 'false', import_not_null 'false');
-- \det+ import_dest3.*
-- \d import_dest3.*

-- Check LIMIT TO and EXCEPT
-- CREATE SCHEMA import_dest4;
-- IMPORT FOREIGN SCHEMA import_source LIMIT TO (t1, nonesuch, t4_part)
--   FROM SERVER loopback INTO import_dest4;
-- \det+ import_dest4.*
-- IMPORT FOREIGN SCHEMA import_source EXCEPT (t1, "x 4", nonesuch, t4_part)
--   FROM SERVER loopback INTO import_dest4;
-- \det+ import_dest4.*


-- Assorted error cases
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER loopback INTO import_dest4;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER loopback INTO import_dest4;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER loopback INTO notthere;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER nowhere INTO notthere;

-- Check case of a type present only on the remote server.
-- We can fake this by dropping the type locally in our transaction.
-- CREATE TYPE "Colors" AS ENUM ('red', 'green', 'blue');
-- CREATE TABLE import_source.t5 (c1 int, c2 text collate "C", "Col" "Colors");

-- CREATE SCHEMA import_dest5;
-- BEGIN;
-- DROP TYPE "Colors" CASCADE;
-- IMPORT FOREIGN SCHEMA import_source LIMIT TO (t5)
--   FROM SERVER loopback INTO import_dest5;  -- ERROR

-- ROLLBACK;

-- BEGIN;


-- CREATE SERVER fetch101 FOREIGN DATA WRAPPER postgres_fdw OPTIONS( fetch_size '101' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'fetch101'
-- AND srvoptions @> array['fetch_size=101'];

-- ALTER SERVER fetch101 OPTIONS( SET fetch_size '202' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'fetch101'
-- AND srvoptions @> array['fetch_size=101'];

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'fetch101'
-- AND srvoptions @> array['fetch_size=202'];

-- CREATE FOREIGN TABLE table30000 ( x int ) SERVER fetch101 OPTIONS ( fetch_size '30000' );

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30000'::regclass
-- AND ftoptions @> array['fetch_size=30000'];

-- ALTER FOREIGN TABLE table30000 OPTIONS ( SET fetch_size '60000');

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30000'::regclass
-- AND ftoptions @> array['fetch_size=30000'];

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30000'::regclass
-- AND ftoptions @> array['fetch_size=60000'];

-- ROLLBACK;

-- ===================================================================
-- test partitionwise joins
-- ===================================================================
-- SET enable_partitionwise_join=on;

-- CREATE TABLE fprt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
-- CREATE TABLE fprt1_p1 (LIKE fprt1);
-- CREATE TABLE fprt1_p2 (LIKE fprt1);
-- ALTER TABLE fprt1_p1 SET (autovacuum_enabled = 'false');
-- ALTER TABLE fprt1_p2 SET (autovacuum_enabled = 'false');
-- INSERT INTO fprt1_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 2) i;
-- INSERT INTO fprt1_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 2) i;
-- CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (0) TO (250)
-- 	SERVER loopback OPTIONS (table_name 'fprt1_p1', use_remote_estimate 'true');
-- CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (250) TO (500)
-- 	SERVER loopback OPTIONS (TABLE_NAME 'fprt1_p2');
-- ANALYZE fprt1;
-- ANALYZE fprt1_p1;
-- ANALYZE fprt1_p2;

-- CREATE TABLE fprt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
-- CREATE TABLE fprt2_p1 (LIKE fprt2);
-- CREATE TABLE fprt2_p2 (LIKE fprt2);
-- ALTER TABLE fprt2_p1 SET (autovacuum_enabled = 'false');
-- ALTER TABLE fprt2_p2 SET (autovacuum_enabled = 'false');
-- INSERT INTO fprt2_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 3) i;
-- INSERT INTO fprt2_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 3) i;
-- CREATE FOREIGN TABLE ftprt2_p1 (b int, c varchar, a int)
-- 	SERVER loopback OPTIONS (table_name 'fprt2_p1', use_remote_estimate 'true');
-- ALTER TABLE fprt2 ATTACH PARTITION ftprt2_p1 FOR VALUES FROM (0) TO (250);
-- CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (250) TO (500)
-- 	SERVER loopback OPTIONS (table_name 'fprt2_p2', use_remote_estimate 'true');
-- ANALYZE fprt2;
-- ANALYZE fprt2_p1;
-- ANALYZE fprt2_p2;

-- inner join three tables
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;
-- SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;

-- left outer join + nullable clause
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;
-- SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;

-- with whole-row reference; partitionwise join does not apply
-- EXPLAIN (COSTS OFF)
-- SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;
-- SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;

-- join with lateral reference
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;
-- SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;

-- with PHVs, partitionwise join selected but no join pushdown
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;
-- SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;

-- test FOR UPDATE; partitionwise join does not apply
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;
-- SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;

-- RESET enable_partitionwise_join;

-- ===================================================================
-- test partitionwise aggregates
-- ===================================================================

-- CREATE TABLE pagg_tab (a int, b int, c text) PARTITION BY RANGE(a);

-- CREATE TABLE pagg_tab_p1 (LIKE pagg_tab);
-- CREATE TABLE pagg_tab_p2 (LIKE pagg_tab);
-- CREATE TABLE pagg_tab_p3 (LIKE pagg_tab);

-- INSERT INTO pagg_tab_p1 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 10;
-- INSERT INTO pagg_tab_p2 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 20 and (i % 30) >= 10;
-- INSERT INTO pagg_tab_p3 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 30 and (i % 30) >= 20;

-- Create foreign partitions
-- CREATE FOREIGN TABLE fpagg_tab_p1 PARTITION OF pagg_tab FOR VALUES FROM (0) TO (10) SERVER loopback OPTIONS (table_name 'pagg_tab_p1');
-- CREATE FOREIGN TABLE fpagg_tab_p2 PARTITION OF pagg_tab FOR VALUES FROM (10) TO (20) SERVER loopback OPTIONS (table_name 'pagg_tab_p2');
-- CREATE FOREIGN TABLE fpagg_tab_p3 PARTITION OF pagg_tab FOR VALUES FROM (20) TO (30) SERVER loopback OPTIONS (table_name 'pagg_tab_p3');

-- ANALYZE pagg_tab;
-- ANALYZE fpagg_tab_p1;
-- ANALYZE fpagg_tab_p2;
-- ANALYZE fpagg_tab_p3;

-- When GROUP BY clause matches with PARTITION KEY.
-- Plan with partitionwise aggregates is disabled
-- SET enable_partitionwise_aggregate TO false;
-- EXPLAIN (COSTS OFF)
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- Plan with partitionwise aggregates is enabled
-- SET enable_partitionwise_aggregate TO true;
-- EXPLAIN (COSTS OFF)
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- Check with whole-row reference
-- Should have all the columns in the target list for the given relation
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
-- SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- When GROUP BY clause does not match with PARTITION KEY.
-- EXPLAIN (COSTS OFF)
-- SELECT b, avg(a), max(a), count(*) FROM pagg_tab GROUP BY b HAVING sum(a) < 700 ORDER BY 1;

-- ===================================================================
-- access rights and superuser
-- ===================================================================

-- Non-superuser cannot create a FDW without a password in the connstr
-- CREATE ROLE regress_nosuper NOSUPERUSER;

-- GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO regress_nosuper;

-- SET ROLE regress_nosuper;

-- SHOW is_superuser;

-- This will be OK, we can create the FDW
-- DO $d$
--     BEGIN
--         EXECUTE $$CREATE SERVER loopback_nopw FOREIGN DATA WRAPPER postgres_fdw
--             OPTIONS (dbname '$$||current_database()||$$',
--                      port '$$||current_setting('port')||$$'
--             )$$;
--     END;
-- $d$;

-- But creation of user mappings for non-superusers should fail
-- CREATE USER MAPPING FOR public SERVER loopback_nopw;
-- CREATE USER MAPPING FOR CURRENT_USER SERVER loopback_nopw;

-- CREATE FOREIGN TABLE pg_temp.ft1_nopw (
-- 	c1 int NOT NULL,
-- 	c2 int NOT NULL,
-- 	c3 text,
-- 	c4 timestamptz,
-- 	c5 timestamp,
-- 	c6 varchar(10),
-- 	c7 char(10) default 'ft1',
-- 	c8 user_enum
-- ) SERVER loopback_nopw OPTIONS (schema_name 'public', table_name 'ft1');

-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- If we add a password to the connstr it'll fail, because we don't allow passwords
-- in connstrs only in user mappings.

-- DO $d$
--     BEGIN
--         EXECUTE $$ALTER SERVER loopback_nopw OPTIONS (ADD password 'dummypw')$$;
--     END;
-- $d$;

-- If we add a password for our user mapping instead, we should get a different
-- error because the password wasn't actually *used* when we run with trust auth.
--
-- This won't work with installcheck, but neither will most of the FDW checks.

-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password 'dummypw');

-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- Unpriv user cannot make the mapping passwordless
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password_required 'false');


-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- RESET ROLE;

-- But the superuser can
-- ALTER USER MAPPING FOR regress_nosuper SERVER loopback_nopw OPTIONS (ADD password_required 'false');

-- SET ROLE regress_nosuper;

-- Should finally work now
-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- unpriv user also cannot set sslcert / sslkey on the user mapping
-- first set password_required so we see the right error messages
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (SET password_required 'true');
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD sslcert 'foo.crt');
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD sslkey 'foo.key');

-- We're done with the role named after a specific user and need to check the
-- changes to the public mapping.
-- DROP USER MAPPING FOR CURRENT_USER SERVER loopback_nopw;

-- This will fail again as it'll resolve the user mapping for public, which
-- lacks password_required=false
-- SELECT * FROM ft1_nopw LIMIT 1;

-- RESET ROLE;

-- The user mapping for public is passwordless and lacks the password_required=false
-- mapping option, but will work because the current user is a superuser.
-- SELECT * FROM ft1_nopw LIMIT 1;

-- cleanup
-- DROP USER MAPPING FOR public SERVER loopback_nopw;
-- DROP OWNED BY regress_nosuper;
-- DROP ROLE regress_nosuper;

-- Clean-up
-- RESET enable_partitionwise_aggregate;

-- Two-phase transactions are not supported.
-- BEGIN;
-- SELECT count(*) FROM ft1;
-- error here
-- PREPARE TRANSACTION 'fdw_tpc';
-- ROLLBACK;

-- ===================================================================
-- reestablish new connection
-- ===================================================================

-- Change application_name of remote connection to special one
-- so that we can easily terminate the connection later.
-- ALTER SERVER parquet_s3_srv OPTIONS (application_name 'fdw_retry_check');

-- If debug_discard_caches is active, it results in
-- dropping remote connections after every transaction, making it
-- impossible to test termination meaningfully.  So turn that off
-- for this test.
-- SET debug_discard_caches = 0;

-- Make sure we have a remote connection.
-- SELECT 1 FROM ft1 LIMIT 1;

-- Terminate the remote connection and wait for the termination to complete.
-- SELECT pg_terminate_backend(pid, 180000) FROM pg_stat_activity
-- 	WHERE application_name = 'fdw_retry_check';

-- This query should detect the broken connection when starting new remote
-- transaction, reestablish new connection, and then succeed.
-- BEGIN;
-- SELECT 1 FROM ft1 LIMIT 1;

-- If we detect the broken connection when starting a new remote
-- subtransaction, we should fail instead of establishing a new connection.
-- Terminate the remote connection and wait for the termination to complete.
-- SELECT pg_terminate_backend(pid, 180000) FROM pg_stat_activity
-- 	WHERE application_name = 'fdw_retry_check';
-- SAVEPOINT s;
-- The text of the error might vary across platforms, so only show SQLSTATE.
-- \set VERBOSITY sqlstate
-- SELECT 1 FROM ft1 LIMIT 1;    -- should fail
-- \set VERBOSITY default
-- COMMIT;

-- RESET debug_discard_caches;
-- =============================================================================
-- test connection invalidation cases and parquet_s3_fdw_get_connections function
-- with local parquet file (not on minio/s3 servers). It haven't server and connection.
-- =============================================================================
-- Let's ensure to close all the existing cached connections.
--Testcase 405:
SELECT 1 FROM parquet_s3_fdw_disconnect_all();
-- No cached connections, so no records should be output.
--Testcase 406:
SELECT server_name FROM parquet_s3_fdw_get_connections() ORDER BY 1;
-- This test case is for closing the connection in pgfdw_xact_callback
-- BEGIN;
-- Connection xact depth becomes 1 i.e. the connection is in midst of the xact.
--Testcase 407:
SELECT 1 FROM ft1 LIMIT 1;
--Testcase 408:
SELECT 1 FROM ft7 LIMIT 1;
-- List all the existing cached connections. parquet_s3_srv and parquet_s3_srv_3 should be
-- output.
--Testcase 409:
SELECT server_name FROM parquet_s3_fdw_get_connections() ORDER BY 1;  -- return 0 rows if not use minio/s3 servers.
-- Connections are not closed at the end of the alter and drop statements.
-- That's because the connections are in midst of this xact,
-- they are just marked as invalid in pgfdw_inval_callback.
--Testcase 703:
ALTER SERVER parquet_s3_srv OPTIONS (ADD use_remote_estimate 'off');
--Testcase 410:
DROP SERVER parquet_s3_srv_3 CASCADE;
-- List all the existing cached connections. parquet_s3_srv and parquet_s3_srv_3
-- should be output as invalid connections. Also the server name for
-- parquet_s3_srv_3 should be NULL because the server was dropped.
--Testcase 411:
SELECT * FROM parquet_s3_fdw_get_connections() ORDER BY 1;  -- return 0 rows if not use minio/s3 servers.
-- The invalid connections get closed in pgfdw_xact_callback during commit.
-- COMMIT;
-- All cached connections were closed while committing above xact, so no
-- records should be output.
-- SELECT server_name FROM parquet_s3_fdw_get_connections() ORDER BY 1;

-- =======================================================================
-- test parquet_s3_fdw_disconnect and parquet_s3_fdw_disconnect_all functions
-- with local parquet file (not on minio/s3 servers). It haven't server and connection.
-- =======================================================================
-- BEGIN;
-- Let's ensure to close all the existing cached connections.
--Testcase 412:
SELECT 1 FROM parquet_s3_fdw_disconnect_all();
-- Ensure to cache parquet_s3_srv connection.
--Testcase 413:
SELECT 1 FROM ft1 LIMIT 1;
-- Ensure to cache parquet_s3_srv_2 connection.
--Testcase 414:
SELECT 1 FROM ft6 LIMIT 1;
-- List all the existing cached connections. parquet_s3_srv and parquet_s3_srv_2 should be
-- output.
--Testcase 415:
SELECT server_name FROM parquet_s3_fdw_get_connections() ORDER BY 1;  -- return 0 rows if not use minio/s3 servers.
-- Issue a warning and return false as parquet_s3_srv connection is still in use and
-- can not be closed.
-- SELECT parquet_s3_fdw_disconnect('parquet_s3_srv');
-- List all the existing cached connections. parquet_s3_srv and parquet_s3_srv_2 should be
-- output.
--Testcase 416:
SELECT server_name FROM parquet_s3_fdw_get_connections() ORDER BY 1;  -- return 0 rows if not use minio/s3 servers.
-- Return false as connections are still in use, warnings are issued.
-- But disable warnings temporarily because the order of them is not stable.
-- SET client_min_messages = 'ERROR';
-- SELECT parquet_s3_fdw_disconnect_all();
-- RESET client_min_messages;
-- COMMIT;
-- Ensure that parquet_s3_srv_2 connection is closed.
--Testcase 417:
SELECT 1 FROM parquet_s3_fdw_disconnect('parquet_s3_srv_2');
--Testcase 418:
SELECT server_name FROM parquet_s3_fdw_get_connections() WHERE server_name = 'parquet_s3_srv_2';
-- Return false as parquet_s3_srv_2 connection is closed already.
--Testcase 419:
SELECT parquet_s3_fdw_disconnect('parquet_s3_srv_2');
-- Return an error as there is no foreign server with given name.
--Testcase 420:
SELECT parquet_s3_fdw_disconnect('unknownserver');
-- Let's ensure to close all the existing cached connections.
--Testcase 421:
SELECT 1 FROM parquet_s3_fdw_disconnect_all();
-- No cached connections, so no records should be output.
--Testcase 422:
SELECT server_name FROM parquet_s3_fdw_get_connections() ORDER BY 1;

-- =============================================================================
-- test case for having multiple cached connections for a foreign server
-- with local parquet file (not on minio/s3 servers). It haven't server and connection.
-- =============================================================================
--Testcase 423:
CREATE ROLE regress_multi_conn_user1 SUPERUSER;
--Testcase 424:
CREATE ROLE regress_multi_conn_user2 SUPERUSER;
--Testcase 425:
CREATE USER MAPPING FOR regress_multi_conn_user1 SERVER parquet_s3_srv :USER_PASSWORD;
--Testcase 426:
CREATE USER MAPPING FOR regress_multi_conn_user2 SERVER parquet_s3_srv :USER_PASSWORD;

-- BEGIN;
-- Will cache parquet_s3_srv connection with user mapping for regress_multi_conn_user1
--Testcase 704:
SET ROLE regress_multi_conn_user1;
--Testcase 427:
SELECT 1 FROM ft1 LIMIT 1;
--Testcase 705:
RESET ROLE;

-- Will cache parquet_s3_srv connection with user mapping for regress_multi_conn_user2
--Testcase 706:
SET ROLE regress_multi_conn_user2;
--Testcase 428:
SELECT 1 FROM ft1 LIMIT 1;
--Testcase 707:
RESET ROLE;

-- Should output two connections for parquet_s3_srv server
--Testcase 429:
SELECT server_name FROM parquet_s3_fdw_get_connections() ORDER BY 1; -- return 0 rows if not use minio/s3 servers.
-- COMMIT;
-- Let's ensure to close all the existing cached connections.
--Testcase 430:
SELECT 1 FROM parquet_s3_fdw_disconnect_all();
-- No cached connections, so no records should be output.
--Testcase 431:
SELECT server_name FROM parquet_s3_fdw_get_connections() ORDER BY 1; 

-- Clean up
--Testcase 432:
DROP USER MAPPING FOR regress_multi_conn_user1 SERVER parquet_s3_srv;
--Testcase 433:
DROP USER MAPPING FOR regress_multi_conn_user2 SERVER parquet_s3_srv;
--Testcase 434:
DROP ROLE regress_multi_conn_user1;
--Testcase 435:
DROP ROLE regress_multi_conn_user2;
-- ===================================================================
-- Test foreign server level option keep_connections
-- ===================================================================
-- By default, the connections associated with foreign server are cached i.e.
-- keep_connections option is on. Set it to off.
--Testcase 708:
ALTER SERVER parquet_s3_srv OPTIONS (keep_connections 'off');
-- connection to parquet_s3_srv server is closed at the end of xact
-- as keep_connections was set to off.
--Testcase 436:
SELECT 1 FROM ft1 LIMIT 1;
-- No cached connections, so no records should be output.
--Testcase 437:
SELECT server_name FROM parquet_s3_fdw_get_connections() ORDER BY 1;
--Testcase 709:
ALTER SERVER parquet_s3_srv OPTIONS (SET keep_connections 'on');

-- ===================================================================
-- batch insert
-- ===================================================================

-- BEGIN;

-- CREATE SERVER batch10 FOREIGN DATA WRAPPER postgres_fdw OPTIONS( batch_size '10' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=10'];

-- ALTER SERVER batch10 OPTIONS( SET batch_size '20' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=10'];

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=20'];

-- CREATE FOREIGN TABLE table30 ( x int ) SERVER batch10 OPTIONS ( batch_size '30' );

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=30'];

-- ALTER FOREIGN TABLE table30 OPTIONS ( SET batch_size '40');

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=30'];

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=40'];

-- ROLLBACK;

-- CREATE TABLE batch_table ( x int );

-- CREATE FOREIGN TABLE ftable ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table', batch_size '10' );
-- EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable SELECT * FROM generate_series(1, 10) i;
-- INSERT INTO ftable SELECT * FROM generate_series(1, 10) i;
-- INSERT INTO ftable SELECT * FROM generate_series(11, 31) i;
-- INSERT INTO ftable VALUES (32);
-- INSERT INTO ftable VALUES (33), (34);
-- SELECT COUNT(*) FROM ftable;
-- TRUNCATE batch_table;
-- DROP FOREIGN TABLE ftable;

-- -- try if large batches exceed max number of bind parameters
-- CREATE FOREIGN TABLE ftable ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table', batch_size '100000' );
-- INSERT INTO ftable SELECT * FROM generate_series(1, 70000) i;
-- SELECT COUNT(*) FROM ftable;
-- TRUNCATE batch_table;
-- DROP FOREIGN TABLE ftable;

-- Disable batch insert
-- CREATE FOREIGN TABLE ftable ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table', batch_size '1' );
-- EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable VALUES (1), (2);
-- INSERT INTO ftable VALUES (1), (2);
-- SELECT COUNT(*) FROM ftable;

-- Disable batch inserting into foreign tables with BEFORE ROW INSERT triggers
-- even if the batch_size option is enabled.
-- ALTER FOREIGN TABLE ftable OPTIONS ( SET batch_size '10' );
-- CREATE TRIGGER trig_row_before BEFORE INSERT ON ftable
-- FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
-- EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable VALUES (3), (4);
-- INSERT INTO ftable VALUES (3), (4);
-- SELECT COUNT(*) FROM ftable;

-- Clean up
-- DROP TRIGGER trig_row_before ON ftable;

-- DROP FOREIGN TABLE ftable;
-- DROP TABLE batch_table;

-- Use partitioning
-- CREATE TABLE batch_table ( x int ) PARTITION BY HASH (x);

-- CREATE TABLE batch_table_p0 (LIKE batch_table);
-- CREATE FOREIGN TABLE batch_table_p0f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 0)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_table_p0', batch_size '10');

-- CREATE TABLE batch_table_p1 (LIKE batch_table);
-- CREATE FOREIGN TABLE batch_table_p1f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 1)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_table_p1', batch_size '1');

-- CREATE TABLE batch_table_p2
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 2);

-- INSERT INTO batch_table SELECT * FROM generate_series(1, 66) i;
-- SELECT COUNT(*) FROM batch_table;

-- Check that enabling batched inserts doesn't interfere with cross-partition
-- updates
-- CREATE TABLE batch_cp_upd_test (a int) PARTITION BY LIST (a);
-- CREATE TABLE batch_cp_upd_test1 (LIKE batch_cp_upd_test);
-- CREATE FOREIGN TABLE batch_cp_upd_test1_f
-- 	PARTITION OF batch_cp_upd_test
-- 	FOR VALUES IN (1)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_cp_upd_test1', batch_size '10');
-- CREATE TABLE batch_cp_up_test1 PARTITION OF batch_cp_upd_test
-- 	FOR VALUES IN (2);
-- INSERT INTO batch_cp_upd_test VALUES (1), (2);

-- The following moves a row from the local partition to the foreign one
-- UPDATE batch_cp_upd_test t SET a = 1 FROM (VALUES (1), (2)) s(a) WHERE t.a = s.a;
-- SELECT tableoid::regclass, * FROM batch_cp_upd_test;

-- Clean up
-- DROP TABLE batch_table, batch_cp_upd_test, batch_table_p0, batch_table_p1 CASCADE;

-- -- Use partitioning
-- ALTER SERVER loopback OPTIONS (ADD batch_size '10');

-- CREATE TABLE batch_table ( x int, field1 text, field2 text) PARTITION BY HASH (x);

-- CREATE TABLE batch_table_p0 (LIKE batch_table);
-- ALTER TABLE batch_table_p0 ADD CONSTRAINT p0_pkey PRIMARY KEY (x);
-- CREATE FOREIGN TABLE batch_table_p0f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 2, REMAINDER 0)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_table_p0');

-- CREATE TABLE batch_table_p1 (LIKE batch_table);
-- ALTER TABLE batch_table_p1 ADD CONSTRAINT p1_pkey PRIMARY KEY (x);
-- CREATE FOREIGN TABLE batch_table_p1f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 2, REMAINDER 1)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_table_p1');

-- INSERT INTO batch_table SELECT i, 'test'||i, 'test'|| i FROM generate_series(1, 50) i;
-- SELECT COUNT(*) FROM batch_table;
-- SELECT * FROM batch_table ORDER BY x;

-- ALTER SERVER loopback OPTIONS (DROP batch_size);

-- ===================================================================
-- test asynchronous execution
-- ===================================================================

-- ALTER SERVER loopback OPTIONS (DROP extensions);
-- ALTER SERVER loopback OPTIONS (ADD async_capable 'true');
-- ALTER SERVER loopback2 OPTIONS (ADD async_capable 'true');

-- CREATE TABLE async_pt (a int, b int, c text) PARTITION BY RANGE (a);
-- CREATE TABLE base_tbl1 (a int, b int, c text);
-- CREATE TABLE base_tbl2 (a int, b int, c text);
-- CREATE FOREIGN TABLE async_p1 PARTITION OF async_pt FOR VALUES FROM (1000) TO (2000)
--   SERVER loopback OPTIONS (table_name 'base_tbl1');
-- CREATE FOREIGN TABLE async_p2 PARTITION OF async_pt FOR VALUES FROM (2000) TO (3000)
--   SERVER loopback2 OPTIONS (table_name 'base_tbl2');
-- INSERT INTO async_p1 SELECT 1000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- INSERT INTO async_p2 SELECT 2000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- simple queries
-- CREATE TABLE result_tbl (a int, b int, c text);

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b % 100 = 0;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b % 100 = 0;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT a, b, 'AAA' || c FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT a, b, 'AAA' || c FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- Check case where multiple partitions use the same connection
-- CREATE TABLE base_tbl3 (a int, b int, c text);
-- CREATE FOREIGN TABLE async_p3 PARTITION OF async_pt FOR VALUES FROM (3000) TO (4000)
--   SERVER loopback2 OPTIONS (table_name 'base_tbl3');
-- INSERT INTO async_p3 SELECT 3000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- DROP FOREIGN TABLE async_p3;
-- DROP TABLE base_tbl3;

-- Check case where the partitioned table has local/remote partitions
-- CREATE TABLE async_p3 PARTITION OF async_pt FOR VALUES FROM (3000) TO (4000);
-- INSERT INTO async_p3 SELECT 3000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- partitionwise joins
-- SET enable_partitionwise_join TO true;

-- CREATE TABLE join_tbl (a1 int, b1 int, v->>'c1' text, a2 int, b2 int, c2 text);

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO join_tbl SELECT * FROM async_pt t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;
-- INSERT INTO join_tbl SELECT * FROM async_pt t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO join_tbl SELECT t1.a, t1.b, 'AAA' || t1.c, t2.a, t2.b, 'AAA' || t2.c FROM async_pt t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;
-- INSERT INTO join_tbl SELECT t1.a, t1.b, 'AAA' || t1.c, t2.a, t2.b, 'AAA' || t2.c FROM async_pt t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- RESET enable_partitionwise_join;

-- Test rescan of an async Append node with do_exec_prune=false
-- SET enable_hashjoin TO false;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO join_tbl SELECT * FROM async_p1 t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;
-- INSERT INTO join_tbl SELECT * FROM async_p1 t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- RESET enable_hashjoin;

-- Test interaction of async execution with plan-time partition pruning
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt WHERE a < 3000;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt WHERE a < 2000;

-- Test interaction of async execution with run-time partition pruning
-- SET plan_cache_mode TO force_generic_plan;

-- PREPARE async_pt_query (int, int) AS
--   INSERT INTO result_tbl SELECT * FROM async_pt WHERE a < $1 AND b === $2;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- EXECUTE async_pt_query (3000, 505);
-- EXECUTE async_pt_query (3000, 505);

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- EXECUTE async_pt_query (2000, 505);
-- EXECUTE async_pt_query (2000, 505);

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- RESET plan_cache_mode;

-- CREATE TABLE local_tbl(a int, b int, c text);
-- INSERT INTO local_tbl VALUES (1505, 505, 'foo'), (2505, 505, 'bar');
-- ANALYZE local_tbl;

-- CREATE INDEX base_tbl1_idx ON base_tbl1 (a);
-- CREATE INDEX base_tbl2_idx ON base_tbl2 (a);
-- CREATE INDEX async_p3_idx ON async_p3 (a);
-- ANALYZE base_tbl1;
-- ANALYZE base_tbl2;
-- ANALYZE async_p3;

-- ALTER FOREIGN TABLE async_p1 OPTIONS (use_remote_estimate 'true');
-- ALTER FOREIGN TABLE async_p2 OPTIONS (use_remote_estimate 'true');

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM local_tbl, async_pt WHERE local_tbl.a = async_pt.a AND local_tbl.c = 'bar';
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM local_tbl, async_pt WHERE local_tbl.a = async_pt.a AND local_tbl.c = 'bar';
-- SELECT * FROM local_tbl, async_pt WHERE local_tbl.a = async_pt.a AND local_tbl.c = 'bar';

-- ALTER FOREIGN TABLE async_p1 OPTIONS (DROP use_remote_estimate);
-- ALTER FOREIGN TABLE async_p2 OPTIONS (DROP use_remote_estimate);

-- DROP TABLE local_tbl;
-- DROP INDEX base_tbl1_idx;
-- DROP INDEX base_tbl2_idx;
-- DROP INDEX async_p3_idx;

-- UNION queries
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl
-- (SELECT a, b, 'AAA' || c FROM async_p1 ORDER BY a LIMIT 10)
-- UNION
-- (SELECT a, b, 'AAA' || c FROM async_p2 WHERE b < 10);
-- INSERT INTO result_tbl
-- (SELECT a, b, 'AAA' || c FROM async_p1 ORDER BY a LIMIT 10)
-- UNION
-- (SELECT a, b, 'AAA' || c FROM async_p2 WHERE b < 10);

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl
-- (SELECT a, b, 'AAA' || c FROM async_p1 ORDER BY a LIMIT 10)
-- UNION ALL
-- (SELECT a, b, 'AAA' || c FROM async_p2 WHERE b < 10);
-- INSERT INTO result_tbl
-- (SELECT a, b, 'AAA' || c FROM async_p1 ORDER BY a LIMIT 10)
-- UNION ALL
-- (SELECT a, b, 'AAA' || c FROM async_p2 WHERE b < 10);

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- Disable async execution if we use gating Result nodes for pseudoconstant
-- quals
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt WHERE CURRENT_USER = SESSION_USER;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- (SELECT * FROM async_p1 WHERE CURRENT_USER = SESSION_USER)
-- UNION ALL
-- (SELECT * FROM async_p2 WHERE CURRENT_USER = SESSION_USER);

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM ((SELECT * FROM async_p1 WHERE b < 10) UNION ALL (SELECT * FROM async_p2 WHERE b < 10)) s WHERE CURRENT_USER = SESSION_USER;

-- Test that pending requests are processed properly
-- SET enable_mergejoin TO false;
-- SET enable_hashjoin TO false;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt t1, async_p2 t2 WHERE t1.a = t2.a AND t1.b === 505;
-- SELECT * FROM async_pt t1, async_p2 t2 WHERE t1.a = t2.a AND t1.b === 505;

-- CREATE TABLE local_tbl (a int, b int, c text);
-- INSERT INTO local_tbl VALUES (1505, 505, 'foo');
-- ANALYZE local_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM local_tbl t1 LEFT JOIN (SELECT *, (SELECT count(*) FROM async_pt WHERE a < 3000) FROM async_pt WHERE a < 3000) t2 ON t1.a = t2.a;
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM local_tbl t1 LEFT JOIN (SELECT *, (SELECT count(*) FROM async_pt WHERE a < 3000) FROM async_pt WHERE a < 3000) t2 ON t1.a = t2.a;
-- SELECT * FROM local_tbl t1 LEFT JOIN (SELECT *, (SELECT count(*) FROM async_pt WHERE a < 3000) FROM async_pt WHERE a < 3000) t2 ON t1.a = t2.a;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;

-- Check with foreign modify

-- CREATE TABLE base_tbl3 (a int, b int, c text);
-- CREATE FOREIGN TABLE remote_tbl (a int, b int, c text)
--   SERVER loopback OPTIONS (table_name 'base_tbl3');
-- INSERT INTO remote_tbl VALUES (2505, 505, 'bar');

-- CREATE TABLE base_tbl4 (a int, b int, c text);
-- CREATE FOREIGN TABLE insert_tbl (a int, b int, c text)
--   SERVER loopback OPTIONS (table_name 'base_tbl4');

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO insert_tbl (SELECT * FROM local_tbl UNION ALL SELECT * FROM remote_tbl);
-- INSERT INTO insert_tbl (SELECT * FROM local_tbl UNION ALL SELECT * FROM remote_tbl);

-- SELECT * FROM insert_tbl ORDER BY a;

-- Check with direct modify
-- EXPLAIN (VERBOSE, COSTS OFF)
-- WITH t AS (UPDATE remote_tbl SET c = c || c RETURNING *)
-- INSERT INTO join_tbl SELECT * FROM async_pt LEFT JOIN t ON (async_pt.a = t.a AND async_pt.b = t.b) WHERE async_pt.b === 505;
-- WITH t AS (UPDATE remote_tbl SET c = c || c RETURNING *)
-- INSERT INTO join_tbl SELECT * FROM async_pt LEFT JOIN t ON (async_pt.a = t.a AND async_pt.b = t.b) WHERE async_pt.b === 505;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- DROP TABLE local_tbl;
-- DROP FOREIGN TABLE remote_tbl;
-- DROP FOREIGN TABLE insert_tbl;
-- DROP TABLE base_tbl3;
-- DROP TABLE base_tbl4;

-- RESET enable_mergejoin;
-- RESET enable_hashjoin;

-- Test that UPDATE/DELETE with inherited target works with async_capable enabled
-- EXPLAIN (VERBOSE, COSTS OFF)
-- UPDATE async_pt SET c = c || c WHERE b = 0 RETURNING *;
-- UPDATE async_pt SET c = c || c WHERE b = 0 RETURNING *;
-- EXPLAIN (VERBOSE, COSTS OFF)
-- DELETE FROM async_pt WHERE b = 0 RETURNING *;
-- DELETE FROM async_pt WHERE b = 0 RETURNING *;

-- Check EXPLAIN ANALYZE for a query that scans empty partitions asynchronously
-- DELETE FROM async_p1;
-- DELETE FROM async_p2;
-- DELETE FROM async_p3;

-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM async_pt;

-- Clean up
-- DROP TABLE async_pt;
-- DROP TABLE base_tbl1;
-- DROP TABLE base_tbl2;
-- DROP TABLE result_tbl;
-- DROP TABLE join_tbl;

-- Test that an asynchronous fetch is processed before restarting the scan in
-- ReScanForeignScan
-- CREATE TABLE base_tbl (a int, b int);
-- INSERT INTO base_tbl VALUES (1, 11), (2, 22), (3, 33);
-- CREATE FOREIGN TABLE foreign_tbl (b int)
--   SERVER loopback OPTIONS (table_name 'base_tbl');
-- CREATE FOREIGN TABLE foreign_tbl2 () INHERITS (foreign_tbl)
--   SERVER loopback OPTIONS (table_name 'base_tbl');

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT a FROM base_tbl WHERE a IN (SELECT a FROM foreign_tbl);
-- SELECT a FROM base_tbl WHERE a IN (SELECT a FROM foreign_tbl);

-- Clean up
-- DROP FOREIGN TABLE foreign_tbl CASCADE;
-- DROP TABLE base_tbl;

-- ALTER SERVER loopback OPTIONS (DROP async_capable);
-- ALTER SERVER loopback2 OPTIONS (DROP async_capable);

-- ===================================================================
-- test invalid server, foreign table and foreign data wrapper options
-- ===================================================================
-- Invalid fdw_startup_cost option
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER postgres_fdw
-- 	OPTIONS(fdw_startup_cost '100$%$#$#');
-- -- Invalid fdw_tuple_cost option
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER postgres_fdw
-- 	OPTIONS(fdw_tuple_cost '100$%$#$#');
-- -- Invalid fetch_size option
-- CREATE FOREIGN TABLE inv_fsz (c1 int )
-- 	SERVER loopback OPTIONS (fetch_size '100$%$#$#');
-- -- Invalid batch_size option
-- CREATE FOREIGN TABLE inv_bsz (c1 int )
-- 	SERVER loopback OPTIONS (batch_size '100$%$#$#');

-- No option is allowed to be specified at foreign data wrapper level
--Testcase 451:
ALTER FOREIGN DATA WRAPPER parquet_s3_fdw OPTIONS (nonexistent 'fdw');

-- ===================================================================
-- test parallel commit
-- ===================================================================
-- ALTER SERVER loopback OPTIONS (ADD parallel_commit 'true');
-- ALTER SERVER loopback2 OPTIONS (ADD parallel_commit 'true');

-- CREATE TABLE ploc1 (f1 int, f2 text);
-- CREATE FOREIGN TABLE prem1 (f1 int, f2 text)
--   SERVER loopback OPTIONS (table_name 'ploc1');
-- CREATE TABLE ploc2 (f1 int, f2 text);
-- CREATE FOREIGN TABLE prem2 (f1 int, f2 text)
--   SERVER loopback2 OPTIONS (table_name 'ploc2');

-- BEGIN;
-- INSERT INTO prem1 VALUES (101, 'foo');
-- INSERT INTO prem2 VALUES (201, 'bar');
-- COMMIT;
-- SELECT * FROM prem1;
-- SELECT * FROM prem2;

-- BEGIN;
-- SAVEPOINT s;
-- INSERT INTO prem1 VALUES (102, 'foofoo');
-- INSERT INTO prem2 VALUES (202, 'barbar');
-- RELEASE SAVEPOINT s;
-- COMMIT;
-- SELECT * FROM prem1;
-- SELECT * FROM prem2;

-- This tests executing DEALLOCATE ALL against foreign servers in parallel
-- during pre-commit
-- BEGIN;
-- SAVEPOINT s;
-- INSERT INTO prem1 VALUES (103, 'baz');
-- INSERT INTO prem2 VALUES (203, 'qux');
-- ROLLBACK TO SAVEPOINT s;
-- RELEASE SAVEPOINT s;
-- INSERT INTO prem1 VALUES (104, 'bazbaz');
-- INSERT INTO prem2 VALUES (204, 'quxqux');
-- COMMIT;
-- SELECT * FROM prem1;
-- SELECT * FROM prem2;

-- ALTER SERVER loopback OPTIONS (DROP parallel_commit);
-- ALTER SERVER loopback2 OPTIONS (DROP parallel_commit);

-- Clean-up
--Testcase 710:
SET client_min_messages TO WARNING;
--Testcase 438:
DROP TYPE user_enum;
--Testcase 439:
DROP SCHEMA "S 1" CASCADE;
--Testcase 440:
DROP SCHEMA import_dest1 CASCADE;

--Testcase 441:
DROP USER MAPPING FOR CURRENT_USER SERVER parquet_s3_srv;
--Testcase 442:
DROP USER MAPPING FOR CURRENT_USER SERVER parquet_s3_srv_2;

--Testcase 443:
DROP SERVER parquet_s3_srv CASCADE;
--Testcase 444:
DROP SERVER parquet_s3_srv_2 CASCADE;
--Testcase 445:
DROP EXTENSION parquet_s3_fdw CASCADE;

-- Recover data
\! cp -a data/ported_postgres /tmp/data_local
--Testcase 721:
DROP FUNCTION trigger_func CASCADE;
--Testcase 722:
DROP FUNCTION trig_null CASCADE;
--Testcase 723:
DROP FUNCTION trig_row_before_insupdate CASCADE;
