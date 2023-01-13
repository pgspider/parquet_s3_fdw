/*-------------------------------------------------------------------------
 *
 * parquet_s3 Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2022, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        contrib/parquet_s3_fdw/src/slvars.hpp
 *
 *-------------------------------------------------------------------------
 */

#ifndef SLVARS_HPP
#define SLVARS_HPP

extern "C"
{
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
}


typedef struct schemaless_info
{
	bool		schemaless;			/* Enable schemaless check */
	Oid			col_oid;			/* Schemaless column oid */
	Oid			actual_col_fetch_oid;		/* Schemaless actual column fetch operator oid */
	Oid			jsonb_col_fetch_oid;		/* Schemaless nested jsonb column fetch operator oid */
}			schemaless_info;

extern List *parquet_s3_pull_slvars(Expr *expr, Index varno, List *columns, bool extract_raw, List *remote_exprs, schemaless_info *pslinfo);
extern void parquet_s3_get_schemaless_info(schemaless_info *slinfo, bool schemaless);
extern char *parquet_s3_get_slvar(Expr *node, schemaless_info *slinfo, Oid *type);
extern char *parquet_s3_get_nested_jsonb_col(Expr *node, schemaless_info *slinfo, Oid *type);

extern bool parquet_s3_is_select_all(RangeTblEntry *rte, List *tlist);
extern bool parquet_s3_is_slvar(Node *node, schemaless_info *pslinfo);
extern bool parquet_s3_is_nested_jsonb(Node *node, schemaless_info *pslinfo);
#endif
