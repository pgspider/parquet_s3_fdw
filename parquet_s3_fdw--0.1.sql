-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION parquet_s3_fdw" to load this file. \quit

CREATE FUNCTION parquet_s3_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION parquet_s3_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER parquet_s3_fdw
  HANDLER parquet_s3_fdw_handler
  VALIDATOR parquet_s3_fdw_validator;
