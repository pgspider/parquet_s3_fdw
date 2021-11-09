-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION parquet_s3_fdw" to load this file. \quit

CREATE FUNCTION import_parquet_s3(
    tablename  text,
    schemaname text,
    servername text,
    func       regproc,
    arg        jsonb,
    options    jsonb default NULL)
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION import_parquet_s3_explicit(
    tablename  text,
    schemaname text,
    servername text,
    attnames   text[],
    atttypes   regtype[],
    func       regproc,
    arg        jsonb,
    options    jsonb default NULL)
RETURNS VOID
AS 'MODULE_PATHNAME', 'import_parquet_s3_with_attrs'
LANGUAGE C;

CREATE FUNCTION parquet_s3_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION parquet_s3_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION parquet_s3_fdw_version()
  RETURNS pg_catalog.int4 STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FOREIGN DATA WRAPPER parquet_s3_fdw
  HANDLER parquet_s3_fdw_handler
  VALIDATOR parquet_s3_fdw_validator;

CREATE FUNCTION parquet_s3_fdw_get_connections (OUT server_name text,
    OUT valid boolean)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION parquet_s3_fdw_disconnect (text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION parquet_s3_fdw_disconnect_all ()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;
