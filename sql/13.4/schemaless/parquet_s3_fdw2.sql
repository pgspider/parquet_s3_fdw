--Testcase 1:
CREATE EXTENSION parquet_s3_fdw;
--Testcase 2:
CREATE SERVER parquet_s3_srv FOREIGN DATA WRAPPER parquet_s3_fdw OPTIONS (use_minio 'true');
--Testcase 3:
CREATE USER MAPPING FOR PUBLIC SERVER parquet_s3_srv OPTIONS (user 'minioadmin', password 'minioadmin');

-- **********************************************
-- Foreign table using 'filename' option
-- **********************************************
-- File under bucket
--Testcase 4:
CREATE FOREIGN TABLE file0 (v jsonb) SERVER parquet_s3_srv OPTIONS (filename 's3://test-bucket/file0.parquet', schemaless 'true');
--Testcase 5:
SELECT * FROM file0;

-- File in directory
--Testcase 6:
CREATE FOREIGN TABLE file1 (v jsonb) SERVER parquet_s3_srv OPTIONS (filename 's3://test-bucket/dir1/file1.parquet', schemaless 'true');
--Testcase 7:
SELECT * FROM file1;

-- File in sub directory
--Testcase 8:
CREATE FOREIGN TABLE file111 (v jsonb) SERVER parquet_s3_srv OPTIONS (filename 's3://test-bucket/dir1/dir11/file111.parquet', schemaless 'true');
--Testcase 9:
SELECT * FROM file111;

-- Multiple files in the same directory
--Testcase 10:
CREATE FOREIGN TABLE file212223 (v jsonb) SERVER parquet_s3_srv OPTIONS (filename 's3://test-bucket/dir2/file21.parquet s3://test-bucket/dir2/file22.parquet s3://test-bucket/dir2/file23.parquet', schemaless 'true');
--Testcase 11:
SELECT * FROM file212223;

-- Multiple files in some directories
--Testcase 12:
CREATE FOREIGN TABLE file0121 (v jsonb) SERVER parquet_s3_srv OPTIONS (filename 's3://test-bucket/file0.parquet s3://test-bucket/dir1/dir12/file121.parquet', schemaless 'true');
--Testcase 13:
SELECT * FROM file0121;

-- **********************************************
-- Foreign table using 'dirname' option
-- **********************************************
-- Only bucket name
--Testcase 14:
CREATE FOREIGN TABLE bucket (v jsonb) SERVER parquet_s3_srv OPTIONS (dirname 's3://test-bucket', schemaless 'true');
--Testcase 15:
SELECT * FROM bucket;

-- Directory
--Testcase 16:
CREATE FOREIGN TABLE dir1 (v jsonb) SERVER parquet_s3_srv OPTIONS (dirname 's3://test-bucket/dir1', schemaless 'true');
--Testcase 17:
SELECT * FROM dir1;

-- Sub directory
--Testcase 18:
CREATE FOREIGN TABLE dir11 (v jsonb) SERVER parquet_s3_srv OPTIONS (dirname 's3://test-bucket/dir1/dir11', schemaless 'true');
--Testcase 19:
SELECT * FROM dir11;

-- **********************************************
-- Error cases
-- **********************************************
-- File does not exist
--Testcase 20:
CREATE FOREIGN TABLE dummyfile (v jsonb) SERVER parquet_s3_srv OPTIONS (filename 's3://test-bucket/dummy-file.parquet', schemaless 'true');
--Testcase 21:
SELECT * FROM dummyfile;

-- Bucket does not exist
--Testcase 22:
CREATE FOREIGN TABLE dummybucket (v jsonb) SERVER parquet_s3_srv OPTIONS (dirname 's3://dummy-bucket', schemaless 'true');
--Testcase 23:
SELECT * FROM dummybucket;

-- Directory does not exist
--Testcase 24:
CREATE FOREIGN TABLE dummydir (v jsonb) SERVER parquet_s3_srv OPTIONS (dirname 's3://test-bucket/dummy-dir', schemaless 'true');
--Testcase 25:
SELECT * FROM dummydir;

-- Use both options 'filename' and 'dirname'
--Testcase 26:
CREATE FOREIGN TABLE dummy1 (v jsonb) SERVER parquet_s3_srv OPTIONS (filename 's3://test-bucket/dir1/file1.parquet', dirname 's3://test-bucket/dir1', schemaless 'true');

-- Specify both local file and S3 file
--Testcase 27:
CREATE FOREIGN TABLE dummy2 (v jsonb) SERVER parquet_s3_srv OPTIONS (filename 's3://test-bucket/dir1/file1.parquet /tmp/file2.parquet', schemaless 'true');
-- **********************************************
-- Cleanup
-- **********************************************
--Testcase 28:
DROP FOREIGN TABLE file0;
--Testcase 29:
DROP FOREIGN TABLE file1;
--Testcase 30:
DROP FOREIGN TABLE file111;
--Testcase 31:
DROP FOREIGN TABLE file212223;
--Testcase 32:
DROP FOREIGN TABLE file0121;
--Testcase 33:
DROP FOREIGN TABLE bucket;
--Testcase 34:
DROP FOREIGN TABLE dir1;
--Testcase 35:
DROP FOREIGN TABLE dir11;
--Testcase 36:
DROP FOREIGN TABLE dummyfile;
--Testcase 37:
DROP FOREIGN TABLE dummybucket;
--Testcase 38:
DROP FOREIGN TABLE dummydir;
--Testcase 39:
DROP USER MAPPING FOR PUBLIC SERVER parquet_s3_srv;
--Testcase 40:
DROP SERVER parquet_s3_srv;
--Testcase 41:
DROP EXTENSION parquet_s3_fdw;
