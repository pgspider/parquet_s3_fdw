\set ECHO none
\ir sql/schemaless_conf/parameters_local.conf
\set ECHO all
show server_version \gset
\ir sql/:server_version/schemaless/parquet_s3_fdw_post.sql
