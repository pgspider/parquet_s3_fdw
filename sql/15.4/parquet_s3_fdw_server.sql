\set ECHO none
\ir sql/parameters_server.conf
\set ECHO all
show server_version \gset
\ir sql/:server_version/parquet_s3_fdw.sql