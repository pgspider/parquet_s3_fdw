/*-------------------------------------------------------------------------
 *
 * parquet_s3_fdw_server_option.c
 *		  Server option management for parquet_s3_fdw
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/parquet_s3_fdw_server_option.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "parquet_s3_fdw.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/*
 * Check if the provided option is one of the valid options.
 */
bool
parquet_s3_is_valid_server_option(DefElem *def)
{
	if (strcmp(def->defname, SERVER_OPTION_USE_MINIO) == 0 ||
		strcmp(def->defname, SERVER_OPTION_KEEP_CONNECTIONS) == 0)
	{
		/* Check that bool value is valid */
		bool		check_bool_valid;

		if (!parse_bool(defGetString(def), &check_bool_valid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("parquet_s3_fdw: invalid value for boolean option \"%s\": %s",
							def->defname, defGetString(def))));
		return true;
	}
	if (strcmp(def->defname, SERVER_OPTION_REGION) == 0 ||
		strcmp(def->defname, SERVER_OPTION_ENDPOINT) == 0)
	{
		return true;
	}

	return false;
}

/*
 * Extract listed option information into parquet_s3_server_opt structure.
 */
static void
parquet_s3_extract_options(List *options, parquet_s3_server_opt * opt)
{
	ListCell   *lc;

	/* Loop through the options. */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, SERVER_OPTION_USE_MINIO) == 0)
			opt->use_minio = defGetBoolean(def);
		else if (strcmp(def->defname, SERVER_OPTION_KEEP_CONNECTIONS) == 0)
			opt->keep_connections = defGetBoolean(def);
		else if (strcmp(def->defname, SERVER_OPTION_REGION) == 0)
			opt->region = defGetString(def);
		else if (strcmp(def->defname, SERVER_OPTION_ENDPOINT) == 0)
			opt->endpoint = defGetString(def);
	}
}


/*
 * Fetch the options for a parquet_s3_fdw foreign table.
 */
parquet_s3_server_opt *
parquet_s3_get_options(Oid foreignoid)
{
	ForeignTable *f_table = NULL;
	ForeignServer *f_server = NULL;
	UserMapping *f_mapping;
	List	   *options;
	parquet_s3_server_opt *opt;

	opt = (parquet_s3_server_opt *) palloc(sizeof(parquet_s3_server_opt));
	memset(opt, 0, sizeof(parquet_s3_server_opt));

	/* Set default value. */
	opt->use_minio = false;
	/* By default, all the connections to any foreign servers are kept open. */
	opt->keep_connections = true;
	opt->region = "ap-northeast-1";
	opt->endpoint = "127.0.0.1:9000";

	/*
	 * Extract options from FDW objects.
	 */
	PG_TRY();
	{
		f_table = GetForeignTable(foreignoid);
		f_server = GetForeignServer(f_table->serverid);
	}
	PG_CATCH();
	{
		f_table = NULL;
		f_server = GetForeignServer(foreignoid);
	}
	PG_END_TRY();

	f_mapping = GetUserMapping(GetUserId(), f_server->serverid);

	options = NIL;
	if (f_table)
		options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);
	options = list_concat(options, f_mapping->options);

	/* Store option information into the structure. */
	parquet_s3_extract_options(options, opt);

	return opt;
}

/*
 * Fetch the options for a parquet_s3_fdw foreign server.
 */
parquet_s3_server_opt *
parquet_s3_get_server_options(Oid serverid)
{
	ForeignServer *f_server = NULL;
	List	   *options;
	parquet_s3_server_opt *opt;

	opt = (parquet_s3_server_opt *) palloc(sizeof(parquet_s3_server_opt));
	memset(opt, 0, sizeof(parquet_s3_server_opt));

	/* Set default value. */
	opt->use_minio = false;
	/* By default, all the connections to any foreign servers are kept open. */
	opt->keep_connections = true;
	opt->region = "ap-northeast-1";
	opt->endpoint = "127.0.0.1:9000";

	/* Get server options. */
	f_server = GetForeignServer(serverid);
	options = f_server->options;

	/* Store option information into the structure. */
	parquet_s3_extract_options(options, opt);

	return opt;
}
