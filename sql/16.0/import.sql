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

-- import foreign schema
\set var '\"':PATH_FILENAME'\/data\/simple\"'
IMPORT FOREIGN SCHEMA :var FROM SERVER parquet_s3_srv INTO public OPTIONS (sorted 'one');
--Testcase 6:
\d
--Testcase 7:
SELECT * FROM example2;


-- import_parquet
--Testcase 8:
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
--Testcase 9:
SELECT import_parquet_s3(
    'example_import',
    'public',
    'parquet_s3_srv',
    'list_parquet_s3_files',
    :'var',
    '{"sorted": "one"}');
--Testcase 10:
SELECT * FROM example_import ORDER BY one, three;

--Testcase 11:
SELECT import_parquet_s3_explicit(
    'example_import2',
    'public',
    'parquet_s3_srv',
    array['one', 'three', 'six'],
    array['int8', 'text', 'bool']::regtype[],
    'list_parquet_s3_files',
    :'var',
    '{"sorted": "one"}');
--Testcase 12:
SELECT * FROM example_import2;

\set var  '{"dir": "':PATH_FILENAME'/data/complex", "type": "complex"}'
--Testcase 13:
SELECT import_parquet_s3(
    'example_import3',
    'public',
    'parquet_s3_srv',
    'list_parquet_s3_files',
    :'var');
--Testcase 14:
SELECT * FROM example_import3;

--Testcase 15:
DROP FUNCTION list_parquet_s3_files;
--Testcase 16:
DROP EXTENSION parquet_s3_fdw CASCADE;
