/*-------------------------------------------------------------------------
 *
 * parquet_s3 Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2022, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        contrib/parquet_s3_fdw/src/slvars.cpp
 *
 *-------------------------------------------------------------------------
 */

extern "C"
{
#include "postgres.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "parquet_s3_fdw.h"
}
#include "slvars.hpp"

/*
 * Context for schemaless vars walker
 */
typedef struct pull_slvars_context
{
	Index				varno;
	schemaless_info	   *pslinfo;
	List			   *columns;
	bool				extract_raw;
	List			   *remote_exprs;
} pull_slvars_context;

/*
 * Schemaless variable expression
 */
typedef struct slvar_expr
{
	Var                *var;
	Const              *cnst;
} slvar_expr;

static bool parquet_s3_slvars_walker(Node *node, pull_slvars_context *context);

/*
 * parquet_s3_is_slvar: Check whether the node is fetch of schemaless type variable
 */
bool
parquet_s3_is_slvar(Node *node, schemaless_info *pslinfo)
{
	OpExpr *oe;
	Node *arg1;
	Node *arg2;

	/* skip outer cast */
	if (IsA(node, CoerceViaIO))
		node = (Node *) ((CoerceViaIO *) node)->arg;

	oe = (OpExpr *)node;

	if (!pslinfo->schemaless)
		return false;
	if (!IsA(node, OpExpr))
		return false;
	if (oe->opno != pslinfo->actual_col_fetch_oid)
		return false;
	if (list_length(oe->args) != 2)
		return false;

	arg1 = (Node *)linitial(oe->args);
	arg2 = (Node *)lsecond(oe->args);
	if (!IsA(arg1, Var) || !IsA(arg2, Const))
		return false;

	if (((Var *)arg1)->vartype != pslinfo->col_oid)
		return false;

	return true;
}

/*
 * parquet_s3_get_slvar: Extract remote column name
 */
char *
parquet_s3_get_slvar(Expr *node, schemaless_info *pslinfo, Oid *type)
{
	Oid		slvar_type;

	if (!pslinfo->schemaless)
		return NULL;

	if (parquet_s3_is_slvar((Node *)node, pslinfo))
	{
		OpExpr *oe;
		Const *cnst;

		if (IsA(node, CoerceViaIO))
		{
			/* slvar `node` is a CoerceViaIO ndoe */
			CoerceViaIO		*coe = (CoerceViaIO *) node;

			oe = (OpExpr *)coe->arg;
			slvar_type = coe->resulttype;
		}
		else
		{
			/* slvar `node` is an OpExpr node */
			oe = (OpExpr *)node;
			slvar_type = oe->opresulttype;
		}


		if (type)
			*type = slvar_type;

		cnst = lsecond_node(Const, oe->args);

		return TextDatumGetCString(cnst->constvalue);
	}

	return NULL;
}

/*
 * parquet_s3_is_nested_jsonb: Check whether the node is fetch of schemaless nested jsonb variable
 */
bool
parquet_s3_is_nested_jsonb(Node *node, schemaless_info *pslinfo)
{
	OpExpr *oe;
	Node *arg1;
	Node *arg2;

	oe = (OpExpr *)node;

	if (!pslinfo->schemaless)
		return false;
	if (!IsA(node, OpExpr))
		return false;
	if (oe->opno != pslinfo->jsonb_col_fetch_oid)
		return false;
	if (list_length(oe->args) != 2)
		return false;

	arg1 = (Node *)linitial(oe->args);
	arg2 = (Node *)lsecond(oe->args);
	if (!IsA(arg1, Var) || !IsA(arg2, Const))
		return false;

	if (((Var *)arg1)->vartype != pslinfo->col_oid)
		return false;

	return true;
}

/*
 * parquet_s3_get_nested_jsonb_col: Extract remote column name
 */
char *
parquet_s3_get_nested_jsonb_col(Expr *node, schemaless_info *pslinfo, Oid *type)
{
	if (!pslinfo->schemaless)
		return NULL;

	if (parquet_s3_is_nested_jsonb((Node *)node, pslinfo))
	{
		OpExpr *oe;
		Const *cnst;

		oe = (OpExpr *)node;
		if (type)
			*type = oe->opresulttype;
		cnst = lsecond_node(Const, oe->args);

		return TextDatumGetCString(cnst->constvalue);
	}

	return NULL;
}

/*
 * parquet_s3_get_schemaless_info: Get information required for schemaless processing
 */
void
parquet_s3_get_schemaless_info(schemaless_info *pslinfo, bool schemaless)
{
	pslinfo->schemaless = schemaless;
	if (schemaless)
	{
		pslinfo->col_oid = JSONBOID;
		if (pslinfo->actual_col_fetch_oid == InvalidOid)
			pslinfo->actual_col_fetch_oid = LookupOperName(NULL, list_make1(makeString((char*)"->>")),
													pslinfo->col_oid, TEXTOID, true, -1);
		if (pslinfo->jsonb_col_fetch_oid == InvalidOid)
			pslinfo->jsonb_col_fetch_oid = LookupOperName(NULL, list_make1(makeString((char*)"->")),
													pslinfo->col_oid, TEXTOID, true, -1);
	}
}

/*
 * parquet_s3_slvars_walker: Recursive function for extracting remote columns name
 */
static bool
parquet_s3_slvars_walker(Node *node, pull_slvars_context *context)
{
	if (node == NULL)
		return false;

	if (parquet_s3_is_slvar(node, context->pslinfo))
	{
		if (IsA(node, CoerceViaIO))
		{
			CoerceViaIO *cio = (CoerceViaIO *) node;

			node = (Node *)cio->arg;
		}

		if (context->extract_raw)
		{
			ListCell *temp;
			foreach (temp, context->columns)
			{
				if (equal(lfirst(temp), node))
				{
					OpExpr *oe1 = (OpExpr *)lfirst(temp);
					OpExpr *oe2 = (OpExpr *)node;
					if (oe1->location == oe2->location)
						return false;
				}
			}
			foreach (temp, context->remote_exprs)
			{
				if (equal(lfirst(temp), node))
				{
					OpExpr *oe1 = (OpExpr *)lfirst(temp);
					OpExpr *oe2 = (OpExpr *)node;
					if (oe1->location == oe2->location)
						return false;
				}
			}
			context->columns = lappend(context->columns, node);
		}
		else
		{
			OpExpr *oe = (OpExpr *)node;
			Var *var = linitial_node(Var, oe->args);
			Const *cnst = lsecond_node(Const, oe->args);

			if (var->varno == context->varno && var->varlevelsup == 0)
			{
				ListCell *temp;
				char *const_str = TextDatumGetCString(cnst->constvalue);

				foreach (temp, context->columns)
				{
					char *colname = strVal(lfirst(temp));
					Assert(colname != NULL);

					if (strcmp(colname, const_str) == 0)
					{
						return false;
					}
				}
				context->columns = lappend(context->columns, makeString(const_str));
			}
		}
	}

	/* Should not find an unplanned subquery */
	Assert(!IsA(node, Query));

	return expression_tree_walker(node, (bool (*)())parquet_s3_slvars_walker,
								  (void *) context);
}

/*
 * parquet_s3_pull_slvars: Pull remote columns name
 */
List *
parquet_s3_pull_slvars(Expr *expr, Index varno, List *columns, bool extract_raw, List *remote_exprs, schemaless_info *pslinfo)
{
	pull_slvars_context context;

	memset(&context, 0, sizeof(pull_slvars_context));

	context.varno = varno;
	context.columns = columns;
	context.pslinfo = pslinfo;
	context.extract_raw = extract_raw;
	context.remote_exprs = remote_exprs;

	(void) parquet_s3_slvars_walker((Node *)expr, &context);

	return context.columns;
}
