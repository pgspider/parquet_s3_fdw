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

