/*-------------------------------------------------------------------------
 *
 * parquet_s3_fdw.h
 *		  Header file to modify for S3 access for parquet_s3_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/parquet_s3_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PARQUET_S3_FDW_H__
#define  __PARQUET_S3_FDW_H__
#include "commands/defrem.h"

#define parquetGetForeignRelSize parquetS3GetForeignRelSize
#define parquetGetForeignPaths parquetS3GetForeignPaths
#define parquetGetForeignPlan parquetS3GetForeignPlan
#define parquetIterateForeignScan parquetS3IterateForeignScan
#define parquetBeginForeignScan parquetS3BeginForeignScan
#define parquetEndForeignScan parquetS3EndForeignScan
#define parquetReScanForeignScan parquetS3ReScanForeignScan
#define parquetAcquireSampleRowsFunc parquetS3AcquireSampleRowsFunc
#define parquetAnalyzeForeignTable parquetS3AnalyzeForeignTable
#define parquetExplainForeignScan parquetS3ExplainForeignScan
#define parquetIsForeignScanParallelSafe parquetS3IsForeignScanParallelSafe
#define parquetEstimateDSMForeignScan parquetS3EstimateDSMForeignScan
#define parquetInitializeDSMForeignScan parquetS3InitializeDSMForeignScan
#define parquetReInitializeDSMForeignScan parquetS3ReInitializeDSMForeignScan
#define parquetInitializeWorkerForeignScan parquetS3InitializeWorkerForeignScan
#define parquetShutdownForeignScan parquetS3ShutdownForeignScan
#define parquetImportForeignSchema parquetS3ImportForeignSchema
#define parquet_fdw_validator_impl parquet_s3_fdw_validator_impl
#define parquet_fdw_use_threads parquet_s3_fdw_use_threads

#define SingleFileExecutionState SingleFileExecutionStateS3
#define CODE_VERSION 300

/* Structure to store option information. */
typedef struct parquet_s3_server_opt
{
	const char *		use_minio;		/* Connect to MinIO hostname instead of Amazon S3. */
	bool		keep_connections;	/* setting value of keep_connections
									 * server option */
}			parquet_s3_server_opt;

bool		parquet_s3_is_valid_server_option(DefElem *def);
parquet_s3_server_opt *parquet_s3_get_options(Oid foreignoid);
parquet_s3_server_opt *parquet_s3_get_server_options(Oid serverid);

/* Option name for CREATE FOREIGN SERVER. */
#define SERVER_OPTION_USE_MINIO "use_minio"
#define SERVER_OPTION_KEEP_CONNECTIONS "keep_connections"

#endif							/* __PARQUET_S3_FDW_H__ */
