/*-------------------------------------------------------------------------
 *
 * parquet_impl.cpp
 *		  Parquet processing implementation for parquet_s3_fdw
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/parquet_impl.cpp
 *
 *-------------------------------------------------------------------------
 */
// basename comes from string.h on Linux,
// but from libgen.h on other POSIX systems (see man basename)
#ifndef GNU_SOURCE
#include <libgen.h>
#endif

#include <sys/stat.h>
#include <math.h>
#include <list>
#include <set>

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/array.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

#include "parquet_s3_fdw.hpp"
#include "heap.hpp"
#include "exec_state.hpp"
#include "reader.hpp"
#include "common.hpp"
#include "slvars.hpp"
#include "modify_reader.hpp"
#include "modify_state.hpp"

extern "C"
{
#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/sysattr.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/appendinfo.h"
#include "optimizer/cost.h"
#include "optimizer/inherit.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "utils/varlena.h"
#include "funcapi.h"

#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#else
#include "access/table.h"
#include "access/relation.h"
#include "optimizer/optimizer.h"
#endif

#if PG_VERSION_NUM < 110000
#include "catalog/pg_am.h"
#else
#include "catalog/pg_am_d.h"
#endif
}


/* from costsize.c */
#define LOG2(x)  (log(x) / 0.693147180559945)

#if PG_VERSION_NUM < 110000
#define PG_GETARG_JSONB_P PG_GETARG_JSONB
#endif

#define IS_KEY_COLUMN(A)        ((strcmp(def->defname, "key") == 0) && \
                                 (defGetBoolean(def) == true))

#define SPD_CMD_CREATE 0
#define SPD_CMD_DROP 1

bool enable_multifile;
bool enable_multifile_merge;


static void find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2);
static void destroy_parquet_state(void *arg);
static void parse_function_signature(char *signature, std::string &funcname, std::vector<std::string> &param_names);
static bool get_column_by_name(const char *name, TupleTableSlot *slot, Oid *type, Datum *value, bool *isnull);
static List *parse_attributes_list(char *start);
/*
 * Restriction
 */
struct RowGroupFilter
{
    AttrNumber  attnum;
    bool        is_key; /* for maps */
    Const      *value;
    int         strategy;
    char       *attname;    /* actual column name in schemales mode */
    Oid         atttype;   /* Explicit cast type in schemaless mode
                               In non-schemaless NULL is expectation  */
    bool        is_column;  /* for schemaless actual column `exist` operator */
};

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the filenames:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateFileNames));
 */
enum FdwScanPrivateIndex
{
    /* List of paths to Parquet files */
	FdwScanPrivateFileNames,
    /* List of Attributes actually used in query */
	FdwScanPrivateAttributesUsed,
    /* List of columns that Parquet files are presorted by */
    FdwScanPrivateAttributesSorted,
    /* use_mmap flag (as an integer Value node) */
    FdwScanPrivateUseMmap,
    /* use_threads flag (as an integer Value node) */
    FdwScanPrivateUse_Threads,
    /* ReaderType of Parquet files */
    FdwScanPrivateType,
    /* The limit for the number of Parquet files open simultaneously. */
    FdwScanPrivateMaxOpenFiles,
    /* List of Lists (per filename) */
    FdwScanPrivateRowGroups,
    /* Schemaless Options */
    FdwScanPrivateSchemalessOpt,
    /* Schemaless Columns */
    FdwScanPrivateSchemalessColumn,
    /* Path to directory having Parquet files to read */
    FdwScanPrivateDirName,
    /* Foreign Table Id */
    FdwScanPrivateForeignTableId
};

/*
 * Plain C struct for fdw_state
 */
struct ParquetFdwPlanState
{
    List       *filenames;
    List       *attrs_sorted;
    Bitmapset  *attrs_used;     /* attributes actually used in query */
    bool        use_mmap;
    bool        use_threads;
    int32       max_open_files;
    bool        files_in_order;
    List       *rowgroups;      /* List of Lists (per filename) */
    uint64      matched_rows;
    ReaderType  type;
    char       *dirname;
    bool        schemaless;     /* In schemaless mode or not */
    schemaless_info slinfo;     /* Schemaless information */
    List       *slcols;         /* List actual column for schemaless mode */
    Aws::S3::S3Client *s3client;
    char       *selector_function_name;
    List       *key_columns;
};

static void get_filenames_in_dir(ParquetFdwPlanState *fdw_private);
static void parquet_s3_extract_slcols(ParquetFdwPlanState *fpinfo, PlannerInfo *root, RelOptInfo *baserel, List *tlist);

static int
get_strategy(Oid type, Oid opno, Oid am)
{
        Oid opclass;
    Oid opfamily;

    opclass = GetDefaultOpClass(type, am);

    if (!OidIsValid(opclass))
        return 0;

    opfamily = get_opclass_family(opclass);

    return get_op_opfamily_strategy(opno, opfamily);
}

/*
 * schemaless_extract_rowgroup_filters
 *      Build a list of expressions we can use to filter out row groups in schemaless mode
 */
static void
schemaless_extract_rowgroup_filters(List *scan_clauses,
                                    std::list<RowGroupFilter> &filters,
                                    schemaless_info *slinfo)
{
    ListCell *lc;

    foreach (lc, scan_clauses)
    {
        Expr       *clause = (Expr *) lfirst(lc);
        OpExpr     *expr;
        Expr       *left, *right;
        int         strategy;
        bool        is_key = false;
        Const      *c;
        Oid         opno;
        char       *attname;
        Oid         atttype = InvalidOid;
        bool        is_column = false;

        if (IsA(clause, RestrictInfo))
            clause = ((RestrictInfo *) clause)->clause;

        if (IsA(clause, OpExpr))
        {
            expr = (OpExpr *) clause;

            /* Only interested in binary opexprs */
            if (list_length(expr->args) != 2)
                continue;

            left = (Expr *) linitial(expr->args);
            right = (Expr *) lsecond(expr->args);

            /*
             * Looking for expressions like "EXPR OP CONST" or "CONST OP EXPR"
             *
             * XXX In schemaless mode slvar as expression is supported. Will be
             * extended in future.
             */
            if (IsA(right, Const))
            {
                /* Check for actual column exist operator: v ? 'actual column' */
                if (IsA(left, Var) && ((Var *) left)->vartype == JSONBOID)
                {
                    /* use actual column name for attname */
                    attname = TextDatumGetCString(((Const *)right)->constvalue);
                    atttype = ((Var *) left)->vartype;
                    is_column = true;
                }
                /*
                 * Check for: - actual column exist operator:  (v->'jsonb_col' ? 'element')
                 *            - slvar::type OP CONST
                 */
                else if (!((attname = parquet_s3_get_nested_jsonb_col((Expr *) left, slinfo, &atttype)) != NULL ||
                           (attname = parquet_s3_get_slvar((Expr *) left, slinfo, &atttype)) != NULL))
                    continue;

                opno = expr->opno;
                c = (Const *) right;
            }
            else if (IsA(left, Const))
            {
                /* reverse order (CONST OP slvar) */
                if ((attname = parquet_s3_get_slvar((Expr *)right, slinfo, &atttype)) == NULL)
                    continue;
                c = (Const *) left;
                opno = get_commutator(expr->opno);
            }
            else
                continue;

            /* Not a btree family operator? */
            if ((strategy = get_strategy(atttype, opno, BTREE_AM_OID)) == 0)
            {
                /*
                 * Maybe it's a gin family operator? (We only support
                 * jsonb 'exists' operator at the moment:
                 *      - ((v->>'jsonb_col')::jsonb) ? 'element'
                 *      - (v->'jsonb_col') ? 'element'
                 *      - v ? 'actual column'
                 */
                if ((strategy = get_strategy(atttype, opno, GIN_AM_OID)) == 0
                    || strategy != JsonbExistsStrategyNumber)
                    continue;
                is_key = true;
            }
        }
        else if ((attname = parquet_s3_get_slvar((Expr *)clause, slinfo, &atttype)) != NULL && atttype == BOOLOID)
        {
            /*
             * Trivial expression containing only a single boolean Var. This
             * also covers cases "slvar::boolean = true"
             */
            strategy = BTEqualStrategyNumber;
            c = (Const *) makeBoolConst(true, false);
        }
        else if (IsA(clause, BoolExpr))
        {
            /*
             * Similar to previous case but for expressions like "!(slvar::boolean)" or
             * "slvar::boolean = false"
             */
            BoolExpr *boolExpr = (BoolExpr *) clause;

            if (boolExpr->args && list_length(boolExpr->args) != 1)
                continue;

            if ((attname = parquet_s3_get_slvar((Expr *)linitial(boolExpr->args), slinfo, &atttype)) == NULL || atttype != BOOLOID)
                continue;

            strategy = BTEqualStrategyNumber;
            c = (Const *) makeBoolConst(false, false);
        }
        else
            continue;

        RowGroupFilter f
        {
            .attnum = (AttrNumber)InvalidAttrNumber, /* does not use this in schemaless mode */
            .is_key = is_key,
            .value = c,
            .strategy = strategy,
            .attname = attname,
            .atttype = atttype,
            .is_column = is_column
        };

        /* potentially inserting elements may throw exceptions */
        try {
            filters.push_back(f);
        } catch (std::exception &e) {
            elog(ERROR, "parquet_s3_fdw: extracting row filters failed");
        }
    }
}

/*
 * extract_rowgroup_filters
 *      Build a list of expressions we can use to filter out row groups.
 */
static void
extract_rowgroup_filters(List *scan_clauses,
                         std::list<RowGroupFilter> &filters)
{
    ListCell *lc;

    foreach (lc, scan_clauses)
    {
        Expr       *clause = (Expr *) lfirst(lc);
        OpExpr     *expr;
        Expr       *left, *right;
        int         strategy;
        bool        is_key = false;
        Const      *c;
        Var        *v;
        Oid         opno;

        if (IsA(clause, RestrictInfo))
            clause = ((RestrictInfo *) clause)->clause;

        if (IsA(clause, OpExpr))
        {
            expr = (OpExpr *) clause;

            /* Only interested in binary opexprs */
            if (list_length(expr->args) != 2)
                continue;

            left = (Expr *) linitial(expr->args);
            right = (Expr *) lsecond(expr->args);

            /*
             * Looking for expressions like "EXPR OP CONST" or "CONST OP EXPR"
             *
             * XXX Currently only Var as expression is supported. Will be
             * extended in future.
             */
            if (IsA(right, Const))
            {
                if (!IsA(left, Var))
                    continue;
                v = (Var *) left;
                c = (Const *) right;
                opno = expr->opno;
            }
            else if (IsA(left, Const))
            {
                /* reverse order (CONST OP VAR) */
                if (!IsA(right, Var))
                    continue;
                v = (Var *) right;
                c = (Const *) left;
                opno = get_commutator(expr->opno);
            }
            else
                continue;

            /* Not a btree family operator? */
            if ((strategy = get_strategy(v->vartype, opno, BTREE_AM_OID)) == 0)
            {
                /*
                 * Maybe it's a gin family operator? (We only support
                 * jsonb 'exists' operator at the moment)
                 */
                if ((strategy = get_strategy(v->vartype, opno, GIN_AM_OID)) == 0
                    || strategy != JsonbExistsStrategyNumber)
                    continue;
                is_key = true;
            }
        }
        else if (IsA(clause, Var))
        {
            /*
             * Trivial expression containing only a single boolean Var. This
             * also covers cases "BOOL_VAR = true"
             */
            v = (Var *) clause;
            strategy = BTEqualStrategyNumber;
            c = (Const *) makeBoolConst(true, false);
        }
        else if (IsA(clause, BoolExpr))
        {
            /*
             * Similar to previous case but for expressions like "!BOOL_VAR" or
             * "BOOL_VAR = false"
             */
            BoolExpr *boolExpr = (BoolExpr *) clause;

            if (boolExpr->args && list_length(boolExpr->args) != 1)
                continue;

            if (!IsA(linitial(boolExpr->args), Var))
                continue;

            v = (Var *) linitial(boolExpr->args);
            strategy = BTEqualStrategyNumber;
            c = (Const *) makeBoolConst(false, false);
        }
        else
            continue;

        /*
         * System columns should not be extract to filter, since
         * we don't make any effort to ensure that local and
         * remote values match (tableoid, in particular, almost
         * certainly doesn't match).
         */
        if (v->varattno < 0)
            continue;

        RowGroupFilter f
        {
            .attnum = v->varattno,
            .is_key = is_key,
            .value = c,
            .strategy = strategy,
        };

        /* potentially inserting elements may throw exceptions */
        try {
            filters.push_back(f);
        } catch (std::exception &e) {
            elog(ERROR, "parquet_s3_fdw: extracting row filters failed");
        }
    }
}

static Const *
convert_const(Const *c, Oid dst_oid)
{
    Oid         funcid;
    CoercionPathType ct;

    ct = find_coercion_pathway(dst_oid, c->consttype,
                               COERCION_EXPLICIT, &funcid);
    switch (ct)
    {
        case COERCION_PATH_FUNC:
            {
                FmgrInfo    finfo;
                Const      *newc;
                int16       typlen;
                bool        typbyval;

                get_typlenbyval(dst_oid, &typlen, &typbyval);

                newc = makeConst(dst_oid,
                                 0,
                                 c->constcollid,
                                 typlen,
                                 0,
                                 c->constisnull,
                                 typbyval);
                fmgr_info(funcid, &finfo);
                newc->constvalue = FunctionCall1(&finfo, c->constvalue);

                return newc;
            }
        case COERCION_PATH_RELABELTYPE:
            /* Cast is not needed */
            break;
        case COERCION_PATH_COERCEVIAIO:
            {
                /*
                 * In this type of cast we need to output the value to a string
                 * and then feed this string to the input function of the
                 * target type.
                 */
                Const  *newc;
                int16   typlen;
                bool    typbyval;
                Oid     input_fn, output_fn;
                Oid     input_param;
                bool    isvarlena;
                char   *str;

                /* Construct a new Const node */
                get_typlenbyval(dst_oid, &typlen, &typbyval);
                newc = makeConst(dst_oid,
                                 0,
                                 c->constcollid,
                                 typlen,
                                 0,
                                 c->constisnull,
                                 typbyval);

                /* Get IO functions */
                getTypeOutputInfo(c->consttype, &output_fn, &isvarlena);
                getTypeInputInfo(dst_oid, &input_fn, &input_param);

                str = OidOutputFunctionCall(output_fn,
                                            c->constvalue);
                newc->constvalue = OidInputFunctionCall(input_fn, str,
                                                        input_param, 0);

                return newc;
            }
        default:
            elog(ERROR, "parquet_s3_fdw: cast function to %s is not found",
                 format_type_be(dst_oid));
    }
    return c;
}

/*
 * row_group_matches_filter
 *      Check if min/max values of the column of the row group match filter.
 */
static bool
row_group_matches_filter(parquet::Statistics *stats,
                         const arrow::DataType *arrow_type,
                         RowGroupFilter *filter)
{
    FmgrInfo finfo;
    Datum    val;
    int      collid = filter->value->constcollid;
    int      strategy = filter->strategy;

    if (arrow_type->id() == arrow::Type::MAP && filter->is_key)
    {
        /*
         * Special case for jsonb `?` (exists) operator. As key is always
         * of text type we need first convert it to the target type (if needed
         * of course).
         */

        /*
         * Extract the key type (we don't check correctness here as we've
         * already done this in `extract_rowgroups_list()`)
         */
        auto strct = arrow_type->fields()[0];
        auto key = strct->type()->fields()[0];
        arrow_type = key->type().get();

        /* Do conversion */
        filter->value = convert_const(filter->value,
                                      to_postgres_type(arrow_type->id()));
    }
    val = filter->value->constvalue;

    find_cmp_func(&finfo,
                  filter->value->consttype,
                  to_postgres_type(arrow_type->id()));

    switch (filter->strategy)
    {
        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber:
            {
                Datum   lower;
                int     cmpres;
                bool    satisfies;
                std::string min = std::move(stats->EncodeMin());

                lower = bytes_to_postgres_type(min.c_str(), min.length(),
                                               arrow_type);
                cmpres = FunctionCall2Coll(&finfo, collid, val, lower);

                satisfies =
                    (strategy == BTLessStrategyNumber      && cmpres > 0) ||
                    (strategy == BTLessEqualStrategyNumber && cmpres >= 0);

                if (!satisfies)
                    return false;
                break;
            }

        case BTGreaterStrategyNumber:
        case BTGreaterEqualStrategyNumber:
            {
                Datum   upper;
                int     cmpres;
                bool    satisfies;
                std::string max = std::move(stats->EncodeMax());

                upper = bytes_to_postgres_type(max.c_str(), max.length(),
                                               arrow_type);
                cmpres = FunctionCall2Coll(&finfo, collid, val, upper);

                satisfies =
                    (strategy == BTGreaterStrategyNumber      && cmpres < 0) ||
                    (strategy == BTGreaterEqualStrategyNumber && cmpres <= 0);

                if (!satisfies)
                    return false;
                break;
            }

        case BTEqualStrategyNumber:
        case JsonbExistsStrategyNumber:
            {
                Datum   lower,
                        upper;
                std::string min = std::move(stats->EncodeMin());
                std::string max = std::move(stats->EncodeMax());

                lower = bytes_to_postgres_type(min.c_str(), min.length(),
                                               arrow_type);
                upper = bytes_to_postgres_type(max.c_str(), max.length(),
                                               arrow_type);

                int l = FunctionCall2Coll(&finfo, collid, val, lower);
                int u = FunctionCall2Coll(&finfo, collid, val, upper);

                if (l < 0 || u > 0)
                    return false;
                break;
            }

        default:
            /* should not happen */
            Assert(false);
    }

    return true;
}

typedef enum
{
    PS_START = 0,
    PS_IDENT,
    PS_QUOTE
} ParserState;

/*
 * parse_filenames_list
 *      Parse space separated list of filenames.
 */
static List *
parse_filenames_list(const char *str)
{
    char       *cur = pstrdup(str);
    char       *f = cur;
    ParserState state = PS_START;
    List       *filenames = NIL;
    FileLocation loc = LOC_NOT_DEFINED;

    while (*cur)
    {
        switch (state)
        {
            case PS_START:
                switch (*cur)
                {
                    case ' ':
                        /* just skip */
                        break;
                    case '"':
                        f = cur + 1;
                        state = PS_QUOTE;
                        break;
                    default:
                        /* XXX we should check that *cur is a valid path symbol
                         * but let's skip it for now */
                        state = PS_IDENT;
                        f = cur;
                        break;
                }
                break;
            case PS_IDENT:
                switch (*cur)
                {
                    case ' ':
                        *cur = '\0';
                        loc = parquetFilenamesValidator(f, loc);
                        filenames = lappend(filenames, makeString(f));
                        state = PS_START;
                        f = NULL;
                        break;
                    default:
                        break;
                }
                break;
            case PS_QUOTE:
                switch (*cur)
                {
                    case '"':
                        *cur = '\0';
                        loc = parquetFilenamesValidator(f, loc);
                        filenames = lappend(filenames, makeString(f));
                        state = PS_START;
                        f = NULL;
                        break;
                    default:
                        break;
                }
                break;
            default:
                elog(ERROR, "parquet_s3_fdw: unknown parse state");
        }
        cur++;
    }
    if (f != NULL)
    {
        loc = parquetFilenamesValidator(f, loc);
        filenames = lappend(filenames, makeString(f));
    }

    return filenames;
}

static bool
parquet_s3_column_is_existed(parquet::arrow::SchemaManifest manifest, char *column_name)
{
    for (auto &schema_field : manifest.schema_fields)
    {
        auto       &field = schema_field.field;
        char        arrow_colname[NAMEDATALEN];

        if (field->name().length() > NAMEDATALEN - 1)
            throw Error("parquet column name '%s' is too long (max: %d)",
                        field->name().c_str(), NAMEDATALEN - 1);
        tolowercase(field->name().c_str(), arrow_colname);

        if (strcmp(column_name, arrow_colname) == 0)
            return true;    /* Found!!! */
    }

    /* Can not found column from parquet file */
    return false;
}

/*
 * extract_rowgroups_list
 *      Analyze query predicates and using min/max statistics determine which
 *      row groups satisfy clauses. Store resulting row group list to
 *      fdw_private.
 */
List *
extract_rowgroups_list(const char *filename,
                       const char *dirname,
                       Aws::S3::S3Client *s3_client,
                       TupleDesc tupleDesc,
                       std::list<RowGroupFilter> &filters,
                       uint64 *matched_rows,
                       uint64 *total_rows,
                       bool schemaless) noexcept
{
    std::unique_ptr<parquet::arrow::FileReader> reader;
    arrow::Status   status;
    List           *rowgroups = NIL;
    ReaderCacheEntry *reader_entry  = NULL;
    std::string     error;

    /* Open parquet file to read meta information */
    try
    {
        if (s3_client)
        {
            char *dname;
            char *fname;
            parquetSplitS3Path(dirname, filename, &dname, &fname);
            reader_entry = parquetGetFileReader(s3_client, dname, fname);
            reader = std::move(reader_entry->file_reader->reader);
            pfree(dname);
            pfree(fname);
        }
        else
        {
            status = parquet::arrow::FileReader::Make(
                    arrow::default_memory_pool(),
                    parquet::ParquetFileReader::OpenFile(filename, false),
                    &reader);
        }

        if (!status.ok())
            throw Error("parquet_s3_fdw: failed to open Parquet file: %s ('%s')",
                        status.message().c_str(), filename);

        auto meta = reader->parquet_reader()->metadata();
        parquet::ArrowReaderProperties  props;
        parquet::arrow::SchemaManifest  manifest;

        status = parquet::arrow::SchemaManifest::Make(meta->schema(), nullptr,
                                                      props, &manifest);
        if (!status.ok())
            throw Error("parquet_s3_fdw: error creating arrow schema ('%s')", filename);

        /* Check each row group whether it matches the filters */
        for (int r = 0; r < reader->num_row_groups(); r++)
        {
            bool match = true;
            auto rowgroup = meta->RowGroup(r);

            /* Skip empty rowgroups */
            if (!rowgroup->num_rows())
                continue;

            for (auto &filter : filters)
            {
                AttrNumber      attnum;
                char            pg_colname[NAMEDATALEN];

                if (schemaless)
                {
                    /* In schemaless mode, attname has already existed  */
                    tolowercase(filter.attname, pg_colname);

                    if (filter.is_column == true)
                    {
                        /*
                         * Check column existed for condition: v ? column
                         * If column is not existed, exclude current file from file list.
                         */
                        if ((match = parquet_s3_column_is_existed(manifest, pg_colname)) == false)
                        {
                            elog(DEBUG1, "parquet_s3_fdw: skip file %s", filename);
                            return NIL;
                        }
                        continue;
                    }
                }
                else
                {
                    attnum = filter.attnum - 1;
                    tolowercase(NameStr(TupleDescAttr(tupleDesc, attnum)->attname),
                                pg_colname);
                }

                /*
                 * Search for the column with the same name as filtered attribute
                 */
                for (auto &schema_field : manifest.schema_fields)
                {
                    MemoryContext   ccxt = CurrentMemoryContext;
                    bool            error = false;
                    char            errstr[ERROR_STR_LEN];
                    char            arrow_colname[NAMEDATALEN];
                    auto           &field = schema_field.field;
                    int             column_index;

                    /* Skip complex objects (lists, structs except maps) */
                    if (schema_field.column_index == -1
                        && field->type()->id() != arrow::Type::MAP)
                        continue;

                    if (field->name().length() > NAMEDATALEN - 1)
                        throw Error("parquet column name '%s' is too long (max: %d)",
                                    field->name().c_str(), NAMEDATALEN - 1);
                    tolowercase(field->name().c_str(), arrow_colname);

                    if (strcmp(pg_colname, arrow_colname) != 0)
                        continue;

                    /* in schemaless mode, skip filter if parquet column type is not match with actual column (explicit cast) type */
                    if (schemaless)
                    {
                        int arrow_type = field->type().get()->id();

                        if (!(filter.atttype == to_postgres_type(arrow_type) ||
                              (filter.atttype == JSONBOID &&
                               arrow_type == arrow::Type::MAP)))
                            continue;
                    }

                    if (field->type()->id() == arrow::Type::MAP)
                    {
                        /*
                         * Extract `key` column of the map.
                         * See `create_column_mapping()` for some details on
                         * map structure.
                         */
                        Assert(schema_field.children.size() == 1);
                        auto &strct = schema_field.children[0];

                        Assert(strct.children.size() == 2);
                        auto &key = strct.children[0];
                        column_index = key.column_index;
                    }
                    else
                        column_index = schema_field.column_index;

                    /* Found it! */
                    std::shared_ptr<parquet::Statistics>  stats;
                    auto column = rowgroup->ColumnChunk(column_index);
                    stats = column->statistics();

                    PG_TRY();
                    {
                        /*
                         * If at least one filter doesn't match rowgroup exclude
                         * the current row group and proceed with the next one.
                         */
                        if (stats && !row_group_matches_filter(stats.get(),
                                                               field->type().get(),
                                                               &filter))
                        {
                            match = false;
                            elog(DEBUG1, "parquet_s3_fdw: skip rowgroup %d", r + 1);
                        }
                    }
                    PG_CATCH();
                    {
                        ErrorData *errdata;

                        MemoryContextSwitchTo(ccxt);
                        error = true;
                        errdata = CopyErrorData();
                        FlushErrorState();

                        strncpy(errstr, errdata->message, ERROR_STR_LEN - 1);
                        FreeErrorData(errdata);
                    }
                    PG_END_TRY();
                    if (error)
                        throw Error("parquet_s3_fdw: row group filter match failed: %s", errstr);
                    break;
                }  /* loop over columns */

                if (!match)
                    break;

            }  /* loop over filters */

            /* All the filters match this rowgroup */
            if (match)
            {
                /* TODO: PG_TRY */
                rowgroups = lappend_int(rowgroups, r);
                *matched_rows += rowgroup->num_rows();
            }
            *total_rows += rowgroup->num_rows();
        }  /* loop over rowgroups */
    }
    catch(const std::exception& e) {
        error = e.what();
    }
    if (!error.empty()) {
        if (reader_entry)
            reader_entry->file_reader->reader = std::move(reader);
        elog(ERROR,
             "parquet_s3_fdw: failed to extract row groups from Parquet file: %s ('%s')",
             error.c_str(), filename);
    }

    return rowgroups;
}

struct FieldInfo
{
    char    name[NAMEDATALEN];
    Oid     oid;
};

/*
 * extract_parquet_fields
 *      Read parquet file and return a list of its fields
 */
List *
extract_parquet_fields(const char *path, const char *dirname, Aws::S3::S3Client *s3_client) noexcept
{
    List           *res = NIL;
    std::string     error;

    try
    {
        std::unique_ptr<parquet::arrow::FileReader> reader;
        parquet::ArrowReaderProperties props;
        parquet::arrow::SchemaManifest manifest;
        arrow::Status   status;
        FieldInfo      *fields;

        if (s3_client)
        {
            arrow::MemoryPool* pool = arrow::default_memory_pool();
            char *dname;
            char *fname;
            parquetSplitS3Path(dirname, path, &dname, &fname);
            std::shared_ptr<arrow::io::RandomAccessFile> input(new S3RandomAccessFile(s3_client, dname, fname));
            status = parquet::arrow::OpenFile(input, pool, &reader);
            pfree(dname);
            pfree(fname);
        }
        else
        {
            status = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(path, false),
                        &reader);
        }
        if (!status.ok())
            throw Error("parquet_s3_fdw: failed to open Parquet file %s ('%s')",
                        status.message().c_str(), path);

        auto p_schema = reader->parquet_reader()->metadata()->schema();
        if (!parquet::arrow::SchemaManifest::Make(p_schema, nullptr, props, &manifest).ok())
            throw Error("parquet_s3_fdw: error creating arrow schema ('%s')", path);

        fields = (FieldInfo *) exc_palloc(
                sizeof(FieldInfo) * manifest.schema_fields.size());

        for (auto &schema_field : manifest.schema_fields)
        {
            auto   &field = schema_field.field;
            auto   &type = field->type();
            Oid     pg_type;

            switch (type->id())
            {
                case arrow::Type::LIST:
                {
                    arrow::Type::type subtype_id;
                    Oid     pg_subtype;
                    bool    error = false;

                    if (type->num_fields() != 1)
                        throw Error("lists of structs are not supported ('%s')", path);

                    subtype_id = get_arrow_list_elem_type(type.get());
                    pg_subtype = to_postgres_type(subtype_id);

                    /* This sucks I know... */
                    PG_TRY();
                    {
                        pg_type = get_array_type(pg_subtype);
                    }
                    PG_CATCH();
                    {
                        error = true;
                    }
                    PG_END_TRY();

                    if (error)
                        throw Error("failed to get the type of array elements for %d",
                                    pg_subtype);
                    break;
                }
                case arrow::Type::MAP:
                    pg_type = JSONBOID;
                    break;
                default:
                    pg_type = to_postgres_type(type->id());
            }

            if (pg_type != InvalidOid)
            {
                if (field->name().length() > 63)
                    throw Error("parquet_s3_fdw: field name '%s' in '%s' is too long",
                                field->name().c_str(), path);

                memcpy(fields->name, field->name().c_str(), field->name().length() + 1);
                fields->oid = pg_type;
                res = lappend(res, fields++);
            }
            else
            {
                throw Error("parquet_s3_fdw: cannot convert field '%s' of type '%s' in '%s'",
                            field->name().c_str(), type->name().c_str(), path);
            }
        }
    }
    catch (std::exception &e)
    {
        error = e.what();
    }
    if (!error.empty())
        elog(ERROR, "parquet_s3_fdw: %s", error.c_str());

    return res;
}

/*
 * create_foreign_table_query
 *      Produce a query text for creating a new foreign table.
 */
char *
create_foreign_table_query(const char *tablename,
                           const char *schemaname,
                           const char *servername,
                           char **paths, int npaths,
                           List *fields, List *options)
{
    StringInfoData  str;
    ListCell       *lc;
    bool		    schemaless = false;
    bool            is_first = true;
    List           *key_columns = NIL;

    /* list options */
    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "schemaless") == 0)
            schemaless = defGetBoolean(def);
        else if (strcmp(def->defname, "key_columns") == 0)
            key_columns = parse_attributes_list(pstrdup(defGetString(def)));
    }

    initStringInfo(&str);
    appendStringInfo(&str, "CREATE FOREIGN TABLE ");

    /* append table name */
    if (schemaname)
        appendStringInfo(&str, "%s.%s (",
                         quote_identifier(schemaname), quote_identifier(tablename));
    else
        appendStringInfo(&str, "%s (", quote_identifier(tablename));

    /* append columns */
    if (schemaless == true)
    {
        /* for schemaless mode, columns specify always 'v jsonb' */
        appendStringInfoString(&str, "v jsonb");
    }
    else
    {
        foreach (lc, fields)
        {
            FieldInfo  *field = (FieldInfo *) lfirst(lc);
            char       *name = field->name;
            Oid         pg_type = field->oid;
            const char *type_name = format_type_be(pg_type);

            if (!is_first)
                appendStringInfo(&str, ", %s %s", quote_identifier(name), type_name);
            else
            {
                appendStringInfo(&str, "%s %s", quote_identifier(name), type_name);
                is_first = false;
            }

            if (key_columns)
            {
                ListCell *lc2;

                foreach (lc2, key_columns)
                {
                    char   *key_column_name = (char *) lfirst(lc2);

                    if (strcmp(name, key_column_name) == 0)
                    {
                        appendStringInfo(&str, " OPTIONS (key 'true')");
                        break;
                    }
                }
            }
        }
    }

    appendStringInfo(&str, ") SERVER %s ", quote_identifier(servername));
    appendStringInfo(&str, "OPTIONS (filename '");

    /* list paths */
    is_first = true;
    for (int i = 0; i < npaths; ++i)
    {
        if (!is_first)
            appendStringInfoChar(&str, ' ');
        else
            is_first = false;

        appendStringInfoString(&str, paths[i]);
    }
    appendStringInfoChar(&str, '\'');

    /* list options */
    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        /* ignore key_columns in non schemaless mode */
        if (!schemaless && strcmp(def->defname, "key_columns") == 0)
            continue;

        appendStringInfo(&str, ", %s '%s'", def->defname, defGetString(def));
    }

    appendStringInfo(&str, ")");

    return str.data;
}

static void
destroy_parquet_state(void *arg)
{
    ParquetS3FdwExecutionState *festate = (ParquetS3FdwExecutionState *) arg;

    if (festate)
        delete festate;
}

static void
destroy_parquet_modify_state(void *arg)
{
    ParquetS3FdwModifyState *fmstate = (ParquetS3FdwModifyState *) arg;

    if (fmstate && fmstate->has_s3_client())
    {
        /*
         * After modify, parquet file information on S3 server is different with cached one,
         * so, disable connection imediately after modify to reload this infomation.
         */
        parquet_disconnect_s3_server();
        delete fmstate;
    }

}

/*
 * C interface functions
 */

static List *
parse_attributes_list(char *start)
{
    List      *attrs = NIL;
    char      *token;
    const char *delim = " ";

    while ((token = strtok(start, delim)) != NULL)
    {
        attrs = lappend(attrs, pstrdup(token));
        start = NULL;
    }

    return attrs;
}

/*
 * OidFunctionCall1NullableArg
 *      Practically a copy-paste from FunctionCall1Coll with added capability
 *      of passing a NULL argument.
 */
static Datum
OidFunctionCall1NullableArg(Oid functionId, Datum arg, bool argisnull)
{
#if PG_VERSION_NUM < 120000
    FunctionCallInfoData    _fcinfo;
    FunctionCallInfoData    *fcinfo = &_fcinfo;
#else
	LOCAL_FCINFO(fcinfo, 1);
#endif
    FmgrInfo    flinfo;
    Datum		result;

    fmgr_info(functionId, &flinfo);
    InitFunctionCallInfoData(*fcinfo, &flinfo, 1, InvalidOid, NULL, NULL);

#if PG_VERSION_NUM < 120000
    fcinfo->arg[0] = arg;
    fcinfo->argnull[0] = false;
#else
    fcinfo->args[0].value = arg;
    fcinfo->args[0].isnull = argisnull;
#endif

    result = FunctionCallInvoke(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo->isnull)
        elog(ERROR, "parquet_s3_fdw: function %u returned NULL", flinfo.fn_oid);

    return result;
}

/*
 * OidFunctionCallnNullableArg
 *      Practically a copy-paste from FunctionCall2Coll with added capability
 *      of passing a NULL argument.
 */
static Datum
OidFunctionCallnNullableArg(Oid functionId, Datum *args, bool *arg_isnulls, int nargs)
{
	FunctionCallInfo fcinfo;
    FmgrInfo    flinfo;
    Datum		result;
    int         i;

    fcinfo = (FunctionCallInfo) palloc0(SizeForFunctionCallInfo(nargs));

    fmgr_info(functionId, &flinfo);
    InitFunctionCallInfoData(*fcinfo, &flinfo, 2, InvalidOid, NULL, NULL);

    for (i = 0; i < nargs; i++)
    {
        fcinfo->args[i].value = args[i];
        fcinfo->args[i].isnull = arg_isnulls[i];
    }

    result = FunctionCallInvoke(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo->isnull)
        elog(ERROR, "parquet_s3_fdw: function %u returned NULL", flinfo.fn_oid);

    return result;
}

static List *
get_filenames_from_userfunc(const char *funcname, const char *funcarg)
{
    Jsonb      *j = NULL;
    Oid         funcid;
#if PG_VERSION_NUM >= 160000
    List       *f = stringToQualifiedNameList(funcname, NULL);
#else
    List       *f = stringToQualifiedNameList(funcname);
#endif
    Datum       filenames;
    Oid         jsonboid = JSONBOID;
    Datum      *values;
    bool       *nulls;
    int         num;
    List       *res = NIL;
    ArrayType  *arr;

    if (funcarg)
        j = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(funcarg)));

    funcid = LookupFuncName(f, 1, &jsonboid, false);
    filenames = OidFunctionCall1NullableArg(funcid, (Datum) j, funcarg == NULL);

    arr = DatumGetArrayTypeP(filenames);
    if (ARR_ELEMTYPE(arr) != TEXTOID)
        elog(ERROR, "parquet_s3_fdw: function returned an array with non-TEXT element type");

    deconstruct_array(arr, TEXTOID, -1, false, 'i', &values, &nulls, &num);

    if (num == 0)
    {
        elog(WARNING,
             "parquet_s3_fdw: '%s' function returned an empty array; foreign table wasn't created",
             get_func_name(funcid));
        return NIL;
    }

    for (int i = 0; i < num; ++i)
    {
        if (nulls[i])
            elog(ERROR, "parquet_s3_fdw: user function returned an array containing NULL value(s)");
        res = lappend(res, makeString(TextDatumGetCString(values[i])));
    }

    return res;
}

static void
get_table_options(Oid relid, ParquetFdwPlanState *fdw_private)
{
    ForeignTable *table;
    ListCell     *lc;
    char         *funcname = NULL;
    char         *funcarg = NULL;

    fdw_private->use_mmap = false;
    fdw_private->use_threads = false;
    fdw_private->max_open_files = 0;
    fdw_private->files_in_order = false;
    fdw_private->schemaless = false;
    fdw_private->key_columns = NIL;
    table = GetForeignTable(relid);

    foreach(lc, table->options)
    {
		DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            fdw_private->filenames = parse_filenames_list(defGetString(def));
        }
        else if (strcmp(def->defname, "files_func") == 0)
        {
            funcname = defGetString(def);
        }
        else if (strcmp(def->defname, "files_func_arg") == 0)
        {
            funcarg = defGetString(def);
        }
        else if (strcmp(def->defname, "sorted") == 0)
        {
            fdw_private->attrs_sorted =
                parse_attributes_list(defGetString(def));
        }
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            fdw_private->use_mmap = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "use_threads") == 0)
        {
            fdw_private->use_threads = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "dirname") == 0)
        {
            fdw_private->dirname = defGetString(def);
        }
        else if (strcmp(def->defname, "max_open_files") == 0)
        {
            /* check that int value is valid */
#if PG_VERSION_NUM >= 150000
            fdw_private->max_open_files = pg_strtoint32(defGetString(def));
#else
            fdw_private->max_open_files = string_to_int32(defGetString(def));
#endif
        }
        else if (strcmp(def->defname, "files_in_order") == 0)
        {
            fdw_private->files_in_order = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "schemaless") == 0)
        {
            fdw_private->schemaless = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "insert_file_selector") == 0)
        {
            fdw_private->selector_function_name = defGetString(def);
        }
        else if (strcmp(def->defname, "key_columns") == 0)
        {
            fdw_private->key_columns = parse_attributes_list(defGetString(def));
        }
        else
            elog(ERROR, "parquet_s3_fdw: unknown option '%s'", def->defname);
    }

    if (funcname)
        fdw_private->filenames = get_filenames_from_userfunc(funcname, funcarg);
}

extern "C" void
parquetGetForeignRelSize(PlannerInfo *root,
                         RelOptInfo *baserel,
                         Oid foreigntableid)
{
    ParquetFdwPlanState *fdw_private;
    std::list<RowGroupFilter> filters;
    RangeTblEntry  *rte = planner_rt_fetch(baserel->relid, root);
    Relation        rel;
    TupleDesc       tupleDesc;
    List           *filenames_orig;
    ListCell       *lc;
    uint64          matched_rows = 0;
    uint64          total_rows = 0;

    fdw_private = (ParquetFdwPlanState *) palloc0(sizeof(ParquetFdwPlanState));
    get_table_options(foreigntableid, fdw_private);

    /* For PGSpider. Overwrite. */
    if (baserel->fdw_private != NIL)
    {
        fdw_private->filenames = (List *) list_nth((List *) baserel->fdw_private, FdwScanPrivateFileNames);
        fdw_private->dirname = NULL;
    }

    if (IS_S3_PATH(fdw_private->dirname) || parquetIsS3Filenames(fdw_private->filenames))
    {
#if (PG_VERSION_NUM < 160000)
        Oid         userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
#else
        Oid         userid = OidIsValid(baserel->userid) ? baserel->userid : GetUserId();
#endif

        fdw_private->s3client = parquetGetConnectionByTableid(foreigntableid, userid);
    }
    else
        fdw_private->s3client = NULL;
    get_filenames_in_dir(fdw_private);

    parquet_s3_get_schemaless_info(&fdw_private->slinfo, fdw_private->schemaless);

    /* Analyze query clauses and extract ones that can be of interest to us*/
    if (fdw_private->schemaless)
        schemaless_extract_rowgroup_filters(baserel->baserestrictinfo, filters, &fdw_private->slinfo);
    else
        extract_rowgroup_filters(baserel->baserestrictinfo, filters);

#if PG_VERSION_NUM < 120000
    rel = heap_open(rte->relid, AccessShareLock);
#else
    rel = table_open(rte->relid, AccessShareLock);
#endif
    tupleDesc = RelationGetDescr(rel);

    /*
     * Extract list of row groups that match query clauses. Also calculate
     * approximate number of rows in result set based on total number of tuples
     * in those row groups. It isn't very precise but it is best we got.
     */
    filenames_orig = fdw_private->filenames;
    fdw_private->filenames = NIL;
    foreach (lc, filenames_orig)
    {
        char *filename = strVal(lfirst(lc));
        List *rowgroups = extract_rowgroups_list(filename, fdw_private->dirname, fdw_private->s3client,
                                                 tupleDesc, filters, &matched_rows, &total_rows, fdw_private->schemaless);

        if (rowgroups)
        {
            fdw_private->rowgroups = lappend(fdw_private->rowgroups, rowgroups);
            fdw_private->filenames = lappend(fdw_private->filenames, lfirst(lc));
        }
    }
#if PG_VERSION_NUM < 120000
    heap_close(rel, AccessShareLock);
#else
    table_close(rel, AccessShareLock);
#endif
    list_free(filenames_orig);

    baserel->fdw_private = fdw_private;
    baserel->tuples = total_rows;
    baserel->rows = fdw_private->matched_rows = matched_rows;
}

static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost,
               Cost *run_cost, Cost *total_cost)
{
    auto    fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;
    double  ntuples;

    ntuples = baserel->tuples *
        clauselist_selectivity(root,
                               baserel->baserestrictinfo,
                               0,
                               JOIN_INNER,
                               NULL);

    /*
     * Here we assume that parquet tuple cost is the same as regular tuple cost
     * even though this is probably not true in many cases. Maybe we'll come up
     * with a smarter idea later. Also we use actual number of rows in selected
     * rowgroups to calculate cost as we need to process those rows regardless
     * of whether they're gonna be filtered out or not.
     */
    *run_cost = fdw_private->matched_rows * cpu_tuple_cost;
	*startup_cost = baserel->baserestrictcost.startup;
	*total_cost = *startup_cost + *run_cost;

    baserel->rows = ntuples;
}

static void
extract_used_attributes(RelOptInfo *baserel)
{
    ParquetFdwPlanState *fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;
    ListCell *lc;

    pull_varattnos((Node *) baserel->reltarget->exprs,
                   baserel->relid,
                   &fdw_private->attrs_used);

    foreach(lc, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        pull_varattnos((Node *) rinfo->clause,
                       baserel->relid,
                       &fdw_private->attrs_used);
    }

    if (bms_is_empty(fdw_private->attrs_used))
    {
        bms_free(fdw_private->attrs_used);
        fdw_private->attrs_used = bms_make_singleton(1 - FirstLowInvalidHeapAttributeNumber);
    }
}

/*
 * cost_merge
 *      Calculate the cost of merging nfiles files. The entire logic is stolen
 *      from cost_gather_merge().
 */
static void
cost_merge(Path *path, uint32 nfiles, Cost input_startup_cost,
           Cost input_total_cost, double rows)
{
    Cost		startup_cost = 0;
    Cost		run_cost = 0;
    Cost		comparison_cost;
    double		N;
    double		logN;

    N = nfiles;
    logN = LOG2(N);

    /* Assumed cost per tuple comparison */
    comparison_cost = 2.0 * cpu_operator_cost;

    /* Heap creation cost */
    startup_cost += comparison_cost * N * logN;

    /* Per-tuple heap maintenance cost */
    run_cost += rows * comparison_cost * logN;

    /* small cost for heap management, like cost_merge_append */
    run_cost += cpu_operator_cost * rows;

    path->startup_cost = startup_cost + input_startup_cost;
    path->total_cost = (startup_cost + run_cost + input_total_cost);
}

/*
 * get actual type for column in sorted option, coresponding type Oid list will be returned.
 */
static void
schemaless_get_sorted_column_type(Aws::S3::S3Client *s3_client, List *file_list, char *dirname, List *attrs_sorted, List **attrs_sorted_type)
{
    ListCell   *lc1, *lc2;
    int         attrs_sorted_num = list_length(attrs_sorted);
    Oid        *attrs_sorted_type_array = (Oid *)palloc(sizeof(Oid) * attrs_sorted_num);
    bool       *attrs_sorted_is_taken = (bool *)palloc(sizeof(bool) * attrs_sorted_num);

    memset(attrs_sorted_is_taken, false, attrs_sorted_num);

    foreach(lc1, file_list)
    {
        std::unique_ptr<parquet::arrow::FileReader> reader;
        arrow::Status   status;
        ReaderCacheEntry *reader_entry  = NULL;
        std::string     error;
        char           *filename = strVal((Node *) lfirst(lc1));;
        int             attrs_sorted_idx = 0;

        /* Open parquet file to read meta information */
        try
        {
            if (s3_client)
            {
                char *dname;
                char *fname;
                parquetSplitS3Path(dirname, filename, &dname, &fname);
                reader_entry = parquetGetFileReader(s3_client, dname, fname);
                reader = std::move(reader_entry->file_reader->reader);
                pfree(dname);
                pfree(fname);
            }
            else
            {
                status = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename, false),
                        &reader);
            }

            if (!status.ok())
                throw Error("parquet_s3_fdw: failed to open Parquet file %s", status.message().c_str());

            auto meta = reader->parquet_reader()->metadata();
            parquet::ArrowReaderProperties  props;
            parquet::arrow::SchemaManifest  manifest;

            status = parquet::arrow::SchemaManifest::Make(meta->schema(), nullptr,
                                                        props, &manifest);
            if (!status.ok())
                throw Error("parquet_s3_fdw: error creating arrow schema");

            /*
             * Search for the column with the same name as sorted attribute
             */
            foreach(lc2, attrs_sorted)
            {
                char *attname = (char *) lfirst(lc2);

                for (auto &schema_field : manifest.schema_fields)
                {
                    auto field_name = schema_field.field->name();
                    char arrow_colname[NAMEDATALEN];

                    if (field_name.length() > NAMEDATALEN - 1)
                        throw Error("parquet column name '%s' is too long (max: %d)",
                                    field_name.c_str(), NAMEDATALEN - 1);
                    tolowercase(field_name.c_str(), arrow_colname);

                    if (attrs_sorted_is_taken[attrs_sorted_idx] == false && strcmp(attname, arrow_colname) == 0)
                    {
                        /* Found it! */
                        auto arrow_type_id = schema_field.field->type()->id();
                        attrs_sorted_is_taken[attrs_sorted_idx] = true;

                        switch (arrow_type_id)
                        {
                            case arrow::Type::LIST:
                            case arrow::Type::MAP:
                                /* In schemaless mode, both NESTED LIST and MAP is mapping with JSONB  */
                                attrs_sorted_type_array[attrs_sorted_idx] = JSONBOID;
                                break;
                            default:
                                attrs_sorted_type_array[attrs_sorted_idx] = to_postgres_type(arrow_type_id);
                                break;
                        }

                        if (attrs_sorted_type_array[attrs_sorted_idx] == InvalidOid)
                            elog(ERROR, "parquet_s3_fdw: Can not get mapping type of '%s' column from parquet file.", attname);
                        break;
                    }
                }   /* loop over parquet file columns */
                attrs_sorted_idx++;
            }  /* loop over sorted columns */

            /* Get list type Oid from attrs_sorted_type_array */
            for (int i = list_length(*attrs_sorted_type); i < attrs_sorted_num; i++)
            {
                if (attrs_sorted_is_taken[i] == true)
                {
                    *attrs_sorted_type = lappend_oid(*attrs_sorted_type, attrs_sorted_type_array[i]);
                }
                else
                {
                    /* break to get missing sorted column from the next file */
                    break;
                }
            }

            /* All sorted column type is taken */
            if (list_length(*attrs_sorted_type) == attrs_sorted_num)
                return;
        }
        catch(const std::exception& e) {
            error = e.what();
        }
        if (!error.empty()) {
            if (reader_entry)
                reader_entry->file_reader->reader = std::move(reader);
            elog(ERROR,
                "parquet_s3_fdw: failed to exctract column from Parquet file: %s",
                error.c_str());
        }
    }   /* loop over list parquet file */

    elog(ERROR, "parquet_s3_fdw: '%s' column is not existed.", (char *) list_nth(attrs_sorted, list_length(*attrs_sorted_type)));
}

/*
 * create_path_key_from_sorted_option
 *      Create PathKey for the attribute from "sorted" option
 */
static List *
create_path_key_from_sorted_option(PlannerInfo *root,
                                   PathKey *root_pathkey,
                                   Oid typid,
                                   Relids relids,
                                   Expr *expr,
                                   List *existed_pathkeys)
{
    List       *attr_pathkeys = NIL;
    Oid         sort_op;

    /* Lookup sorting operator for the attribute type */
    get_sort_group_operators(typid,
                            true, false, false,
                            &sort_op, NULL, NULL,
                            NULL);

    /* Create PathKey for the attribute from "sorted" option */
#if PG_VERSION_NUM >= 160000
    attr_pathkeys = build_expression_pathkey(root, expr,
                                             sort_op, relids,
                                             true);
# else
    attr_pathkeys = build_expression_pathkey(root, expr, NULL,
                                             sort_op, relids,
                                             true);
#endif

    if (attr_pathkeys != NIL)
    {
        PathKey    *attr_pathkey = (PathKey *) linitial(attr_pathkeys);

        /*
         * Compare the attribute from "sorted" option and the attribute from
         * ORDER BY clause ("root"). If they don't match stop here and use
         * whatever pathkeys we've build so far. Postgres will use remaining
         * attributes from ORDER BY clause to sort data on higher level of
         * execution.
         */
        if (equal(attr_pathkey, root_pathkey))
        {
            if (existed_pathkeys == NIL)
                return attr_pathkeys;
            else
                return list_concat(existed_pathkeys, attr_pathkeys);
        }
    }

    return NIL;
}

static List *
schemaless_build_path_key(PlannerInfo *root, RelOptInfo *baserel, ParquetFdwPlanState *fdw_private, List *root_sort_pathkeys)
{
    ListCell       *lc1, *lc2, *lc3;
    List           *pathkeys = NIL;
    Oid             relid = root->simple_rte_array[baserel->relid]->relid;
    Relation        rel;
    List           *attrs_sorted_type = NIL;

    /* if there is no parquet file to scan, pathkey is not needed to build */
    if (fdw_private->filenames == NIL)
        return NIL;

    rel = table_open(relid, AccessShareLock);

    /* get the actual column type */
    schemaless_get_sorted_column_type(fdw_private->s3client, fdw_private->filenames, fdw_private->dirname, fdw_private->attrs_sorted, &attrs_sorted_type);

    forthree (lc1, fdw_private->attrs_sorted, lc2, attrs_sorted_type, lc3, root_sort_pathkeys)
    {
        char       *attname = (char *) lfirst(lc1);
        Oid         atttype = lfirst_oid(lc2);
        PathKey    *root_pathkey = (PathKey *) lfirst(lc3);
        int32       typmod;
        Expr       *expr;
        int         jsonb_col_attnum;
        TupleDesc   tupdesc = RelationGetDescr(rel);

        /* Get the first jsonb attribute number which not dropped */
        for (jsonb_col_attnum = 1; jsonb_col_attnum <= tupdesc->natts; jsonb_col_attnum++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, jsonb_col_attnum - 1);

            if (!attr->attisdropped && attr->atttypid == JSONBOID)
            {
                Datum       constvalue;
                Expr       *op_clause;
                Expr       *const_expr;
                Expr       *var_expr;
                Oid         typid,
                            collid;

                /* get atttype and typmod of jsonb column */
                get_atttypetypmodcoll(relid, jsonb_col_attnum, &typid, &typmod, &collid);
                /* build jsonb column var */
                var_expr = (Expr *) makeVar(baserel->relid, jsonb_col_attnum, typid, typmod, collid, 0);

                /* build constant expr from sorted column name (attname) */
                constvalue = (Datum) palloc0(strlen(attname) + VARHDRSZ);
                memcpy(VARDATA(constvalue), attname, strlen(attname));
                SET_VARSIZE(constvalue, strlen(attname) + VARHDRSZ);
                const_expr = (Expr *) makeConst(TEXTOID, -1, DEFAULT_COLLATION_OID, -1, constvalue, false, false);

                /* Build a schemaless var: v->>'col' */
                op_clause = (Expr *)make_opclause(fdw_private->slinfo.actual_col_fetch_oid, TEXTOID, false,
                                    var_expr,
                                    const_expr,
                                    DEFAULT_COLLATION_OID, DEFAULT_COLLATION_OID);

                /* Build CoerceviaIO node (v->>'col')::type */
                expr = (Expr *)coerce_type(NULL, (Node *) op_clause, TEXTOID,
                                atttype, -1,
                                COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);

                pathkeys = create_path_key_from_sorted_option(root, root_pathkey, atttype,
                                                              baserel->relids, expr, pathkeys);
                break;
            }
        }

        /* can not find the jsonb columb to build path keys */
        if (jsonb_col_attnum > tupdesc->natts)
        {
            table_close(rel, AccessShareLock);
            elog(ERROR, "parquet_s3_fdw: Schemaless table does not have jsonb column.");
        }
    }

    table_close(rel, AccessShareLock);
    return pathkeys;
}

extern "C" void
parquetGetForeignPaths(PlannerInfo *root,
                       RelOptInfo *baserel,
                       Oid /* foreigntableid */)
{
	ParquetFdwPlanState *fdw_private;
    Path       *foreign_path;
	Cost		startup_cost;
	Cost		total_cost;
    Cost        run_cost;
    bool        is_sorted, is_multi;
    List       *pathkeys = NIL;
    std::list<RowGroupFilter> filters;
    ListCell   *lc;
    ListCell   *lc2;
    bool        schemaless;

    fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;
    schemaless = fdw_private->schemaless;

    estimate_costs(root, baserel, &startup_cost, &run_cost, &total_cost);

    /* Collect used attributes to reduce number of read columns during scan */
    extract_used_attributes(baserel);

    is_sorted = fdw_private->attrs_sorted != NIL;
    is_multi = list_length(fdw_private->filenames) > 1;
    fdw_private->type = is_multi ? RT_MULTI :
        (list_length(fdw_private->filenames) == 0 ? RT_TRIVIAL : RT_SINGLE);

    if (schemaless)
    {
        pathkeys = schemaless_build_path_key(root, baserel, fdw_private, root->sort_pathkeys);
    }
    else
    {
        /*
         * Build pathkeys for the foreign table based on attrs_sorted and ORDER BY
         * clause passed by user.
         *
         * pathkeys is used by Postgres to sort the result. After we build pathkeys
         * for the foreign table Postgres will assume that the returned data is
         * already sorted. In the function parquetBeginForeignScan() we make sure
         * that the data from parquet files are sorted.
         *
         * We need to make sure that we don't add PathKey for an attribute which is
         * not passed by ORDER BY. We will stop building pathkeys as soon as we see
         * that an attribute on ORDER BY and "sorted" doesn't match, since in that
         * case Postgres will need to sort by remaining attributes by itself.
         */
        forboth (lc, fdw_private->attrs_sorted, lc2, root->sort_pathkeys)
        {
            PathKey    *root_pathkey = (PathKey *) lfirst(lc2);
            Oid         relid = root->simple_rte_array[baserel->relid]->relid;
            int         attnum = lfirst_int(lc);
            Oid         typid,
                        collid;
            int32       typmod;
            Var        *var;

            if (root_pathkey->pk_eclass->ec_has_volatile)
                break;

            if ((attnum = get_attnum(relid, (char *)lfirst(lc))) == InvalidAttrNumber)
                elog(ERROR, "parquet_s3_fdw: invalid attribute name '%s'", (char *)lfirst(lc));

            /* Build an expression (simple var) for the attribute */
            get_atttypetypmodcoll(relid, attnum, &typid, &typmod, &collid);
            var = makeVar(baserel->relid, attnum, typid, typmod, collid, 0);

            pathkeys = create_path_key_from_sorted_option(root, root_pathkey, typid,
                                                          baserel->relids, (Expr *) var, pathkeys);
        }
    }

    foreign_path = (Path *) create_foreignscan_path(root, baserel,
                                                    NULL,	/* default pathtarget */
                                                    baserel->rows,
                                                    startup_cost,
                                                    total_cost,
                                                    NULL,   /* no pathkeys */
                                                    baserel->lateral_relids,
                                                    NULL,	/* no extra plan */
                                                    (List *) fdw_private);
    if (!enable_multifile && is_multi)
        foreign_path->total_cost += disable_cost;

    add_path(baserel, (Path *) foreign_path);

    if (fdw_private->type == RT_TRIVIAL)
        return;

    /* Create a separate path with pathkeys for sorted parquet files. */
    if (is_sorted)
    {
        Path                   *path;
        ParquetFdwPlanState    *private_sort;

        private_sort = (ParquetFdwPlanState *) palloc(sizeof(ParquetFdwPlanState));
        memcpy(private_sort, fdw_private, sizeof(ParquetFdwPlanState));

        path = (Path *) create_foreignscan_path(root, baserel,
                                                NULL,	/* default pathtarget */
                                                baserel->rows,
                                                startup_cost,
                                                total_cost,
                                                pathkeys,
                                                baserel->lateral_relids,
                                                NULL,	/* no extra plan */
                                                (List *) private_sort);

        /* For multifile case calculate the cost of merging files */
        if (is_multi)
        {
            private_sort->type = private_sort->max_open_files > 0 ?
                RT_CACHING_MULTI_MERGE : RT_MULTI_MERGE;

            cost_merge((Path *) path, list_length(private_sort->filenames),
                       startup_cost, total_cost, private_sort->matched_rows);

            if (!enable_multifile_merge)
                path->total_cost += disable_cost;
        }
        add_path(baserel, path);
    }

    /* Parallel paths */
    if (baserel->consider_parallel > 0)
    {
        ParquetFdwPlanState *private_parallel;
        bool use_pathkeys = false;

        private_parallel = (ParquetFdwPlanState *) palloc(sizeof(ParquetFdwPlanState));
        memcpy(private_parallel, fdw_private, sizeof(ParquetFdwPlanState));
        private_parallel->type = is_multi ? RT_MULTI : RT_SINGLE;

        /* For mutifile reader only use pathkeys when files are in order */
        use_pathkeys = is_sorted && (!is_multi || (is_multi && fdw_private->files_in_order));

        Path *path = (Path *)
                 create_foreignscan_path(root, baserel,
                                         NULL,	/* default pathtarget */
                                         baserel->rows,
                                         startup_cost,
                                         total_cost,
                                         use_pathkeys ? pathkeys : NULL,
                                         baserel->lateral_relids,
                                         NULL,	/* no extra plan */
                                         (List *) private_parallel);

        int num_workers = max_parallel_workers_per_gather;

        path->rows = path->rows / (num_workers + 1);
        path->total_cost       = startup_cost + run_cost / (num_workers + 1);
        path->parallel_workers = num_workers;
        path->parallel_aware   = true;
        path->parallel_safe    = true;

        if (!enable_multifile)
            path->total_cost += disable_cost;

        add_partial_path(baserel, path);

        /* Multifile Merge parallel path */
        if (is_multi && is_sorted)
        {
            ParquetFdwPlanState *private_parallel_merge;

            private_parallel_merge = (ParquetFdwPlanState *) palloc(sizeof(ParquetFdwPlanState));
            memcpy(private_parallel_merge, fdw_private, sizeof(ParquetFdwPlanState));

            private_parallel_merge->type = private_parallel_merge->max_open_files > 0 ?
                RT_CACHING_MULTI_MERGE : RT_MULTI_MERGE;

            Path *path = (Path *)
                     create_foreignscan_path(root, baserel,
                                             NULL,	/* default pathtarget */
                                             baserel->rows,
                                             startup_cost,
                                             total_cost,
                                             pathkeys,
                                             baserel->lateral_relids,
                                             NULL,	/* no extra plan */
                                             (List *) private_parallel_merge);

            int num_workers = max_parallel_workers_per_gather;

            cost_merge(path, list_length(private_parallel_merge->filenames),
                       startup_cost, total_cost, private_parallel_merge->matched_rows);

            path->rows = path->rows / (num_workers + 1);
            path->total_cost = path->startup_cost + path->total_cost / (num_workers + 1);
            path->parallel_workers = num_workers;
            path->parallel_aware   = true;
            path->parallel_safe    = true;

            if (!enable_multifile_merge)
                path->total_cost += disable_cost;

            add_partial_path(baserel, path);
        }
    }
}

extern "C" ForeignScan *
parquetGetForeignPlan(PlannerInfo *root,
                      RelOptInfo *baserel,
                      Oid foreigntableid,
                      ForeignPath *best_path,
                      List *tlist,
                      List *scan_clauses,
                      Plan *outer_plan)
{
    ParquetFdwPlanState *fdw_private = (ParquetFdwPlanState *) best_path->fdw_private;
    Index		scan_relid = baserel->relid;
    List       *attrs_used = NIL;
    List       *attrs_sorted = NIL;
    AttrNumber  attr;
    List       *params = NIL;
    ListCell   *lc;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    parquet_s3_extract_slcols(fdw_private, root, baserel, tlist);

    /*
     * We can't just pass arbitrary structure into make_foreignscan() because
     * in some cases (i.e. plan caching) postgres may want to make a copy of
     * the plan and it can only make copy of something it knows of, namely
     * Nodes. So we need to convert everything in nodes and store it in a List.
     */
    attr = -1;
    while ((attr = bms_next_member(fdw_private->attrs_used, attr)) >= 0)
        attrs_used = lappend_int(attrs_used, attr);

    foreach (lc, fdw_private->attrs_sorted)
        attrs_sorted = lappend(attrs_sorted, makeString((char *)lfirst(lc)));

    /* Packing all the data needed by executor into the list */
    params = lappend(params, fdw_private->filenames);
    params = lappend(params, attrs_used);
    params = lappend(params, attrs_sorted);
    params = lappend(params, makeInteger(fdw_private->use_mmap));
    params = lappend(params, makeInteger(fdw_private->use_threads));
    params = lappend(params, makeInteger(fdw_private->type));
    params = lappend(params, makeInteger(fdw_private->max_open_files));
    params = lappend(params, fdw_private->rowgroups);
    params = lappend(params, makeInteger(fdw_private->schemaless));
    params = lappend(params, fdw_private->slcols);

    /*
     * Store foreign table id in order to enable to get S3 handle in
     * BeginForeignScan. If S3 handle is not used, it sets 0 so that
     * BeginForeignScan can recognize S3 handle is not used.
     * copyObject() called in PostgreSQL core cannot handle data of
     * which type is Aws::S3::S3Client*. So we recreate S3 handle via
     * foreign table id.
     */
    if (fdw_private->s3client)
    {
        if(fdw_private->dirname == NULL)
            params = lappend(params, makeString((char *) ""));
        else
            params = lappend(params, makeString(fdw_private->dirname));
        params = lappend(params, makeInteger(foreigntableid));
    }
    else
    {
        params = lappend(params, makeString((char *) ""));
        params = lappend(params, makeInteger(0));
    }

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							params,
							NIL,	/* no custom tlist */
							NIL,	/* no remote quals */
							outer_plan);
}

extern "C" void
parquetBeginForeignScan(ForeignScanState *node, int /* eflags */)
{
    ParquetS3FdwExecutionState   *festate = NULL;
    MemoryContextCallback      *callback;
    MemoryContext   reader_cxt;
	ForeignScan    *plan = (ForeignScan *) node->ss.ps.plan;
	EState         *estate = node->ss.ps.state;
    List           *fdw_private = plan->fdw_private;
    List           *attrs_list;
    List           *rowgroups_list = NIL;
    ListCell       *lc, *lc2;
    List           *filenames = NIL;
    std::set<int>   attrs_used;
    List           *attrs_sorted = NIL;
    bool            use_mmap = false;
    bool            use_threads = false;
    int             i = 0;
    ReaderType      reader_type = RT_SINGLE;
    char           *dirname = NULL;
    Aws::S3::S3Client *s3client = NULL;
    int             max_open_files = 0;
    std::string     error;
    List           *slcols_list;
    bool            schemaless = false;
    std::set<std::string> slcols;
    std::set<std::string> sorted_cols;

    /* Unwrap fdw_private */
    foreach (lc, fdw_private)
    {
        switch(i)
        {
            case FdwScanPrivateFileNames:
                filenames = (List *) lfirst(lc);
                break;
            case FdwScanPrivateAttributesUsed:
                attrs_list = (List *) lfirst(lc);
                foreach (lc2, attrs_list)
                    attrs_used.insert(lfirst_int(lc2));
                break;
            case FdwScanPrivateAttributesSorted:
                attrs_sorted = (List *) lfirst(lc);
                break;
            case FdwScanPrivateUseMmap:
                use_mmap = (bool) intVal(lfirst(lc));
                break;
            case FdwScanPrivateUse_Threads:
                use_threads = (bool) intVal(lfirst(lc));
                break;
            case FdwScanPrivateType:
                reader_type = (ReaderType) intVal(lfirst(lc));
                break;
            case FdwScanPrivateMaxOpenFiles:
                max_open_files = intVal(lfirst(lc));
                break;
            case FdwScanPrivateRowGroups:
                rowgroups_list = (List *) lfirst(lc);
                break;
            case FdwScanPrivateSchemalessOpt:
                schemaless = (bool) intVal((Node *) lfirst(lc));
                break;
            case FdwScanPrivateSchemalessColumn:
            {
                slcols_list = (List *) lfirst(lc);
                foreach(lc2, slcols_list)
                {
                    StringInfo *rcol = (StringInfo *)lfirst(lc2);
                    slcols.insert(std::string(strVal(rcol)));
                }
                break;
            }
            case FdwScanPrivateDirName:
                dirname = (char *) strVal((Node *) lfirst(lc));
                break;
            case FdwScanPrivateForeignTableId:
            {
                /* Recreate S3 handle by foreign table id. */
                Oid s3tableoid = intVal((Node *) lfirst(lc));
#if PG_VERSION_NUM < 160000
                int     rtindex;
                ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
                RangeTblEntry *rte;
                Oid     userid;

                if (fsplan->scan.scanrelid > 0)
                    rtindex = fsplan->scan.scanrelid;
                else
                    rtindex = bms_next_member(fsplan->fs_relids, -1);

                rte = rt_fetch(rtindex, estate->es_range_table);
                userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
#else
                /*
                 * Identify which user to do the remote access as.  This should match what
                 * ExecCheckPermissions() does.
                 */
                Oid     userid = OidIsValid(plan->checkAsUser) ? plan->checkAsUser : GetUserId();
#endif
                s3client = parquetGetConnectionByTableid(s3tableoid, userid);
                break;
            }
        }
        ++i;
    }

    MemoryContext   cxt = estate->es_query_cxt;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    TupleDesc       tupleDesc = slot->tts_tupleDescriptor;

    reader_cxt = AllocSetContextCreate(cxt,
                                       "parquet_s3_fdw tuple data",
                                       ALLOCSET_DEFAULT_SIZES);

    std::list<SortSupportData> sort_keys;
    foreach (lc, attrs_sorted)
    {
        SortSupportData sort_key;
        char   *attname = (char *) strVal((Node *)lfirst(lc));
        Oid     typid;
        int     typmod;
        Oid     collid;
        Oid     relid = RelationGetRelid(node->ss.ss_currentRelation);
        Oid     sort_op;
        int     attr;

        if (schemaless)
        {
            /*
             * Sort key for schemaless actual column will be get when create
             * column mapping. At now, get attname only.
             */
            sorted_cols.insert(std::string(attname));
        }
        else
        {
            attr = get_attnum(relid, attname);
            if (attr == InvalidAttrNumber)
            {
                elog(ERROR, "paruqet_s3_fdw: invalid attribute name '%s'", attname);
            }

            memset(&sort_key, 0, sizeof(SortSupportData));

            get_atttypetypmodcoll(relid, attr, &typid, &typmod, &collid);

            sort_key.ssup_cxt = reader_cxt;
            sort_key.ssup_collation = collid;
            sort_key.ssup_nulls_first = true;
            sort_key.ssup_attno = attr;
            sort_key.abbreviate = false;

            get_sort_group_operators(typid,
                                    true, false, false,
                                    &sort_op, NULL, NULL,
                                    NULL);

            PrepareSortSupportFromOrderingOp(sort_op, &sort_key);

            try {
                sort_keys.push_back(sort_key);
            } catch (std::exception &e) {
                error = e.what();
            }
            if (!error.empty())
                elog(ERROR, "parquet_s3_fdw: scan initialization failed: %s", error.c_str());
        }
    }

    try
    {
        festate = create_parquet_execution_state(reader_type, reader_cxt, dirname, s3client, tupleDesc,
                                                 attrs_used, sort_keys,
                                                 use_threads, use_mmap,
                                                 max_open_files, schemaless,
                                                 slcols, sorted_cols);

        forboth (lc, filenames, lc2, rowgroups_list)
        {
            char *filename = strVal(lfirst(lc));
            List *rowgroups = (List *) lfirst(lc2);

            festate->add_file(filename, rowgroups);
        }
    }
    catch(std::exception &e)
    {
        error = e.what();
    }
    if (!error.empty())
        elog(ERROR, "parquet_s3_fdw: %s", error.c_str());

    /*
     * Enable automatic execution state destruction by using memory context
     * callback
     */
    callback = (MemoryContextCallback *) palloc(sizeof(MemoryContextCallback));
    callback->func = destroy_parquet_state;
    callback->arg = (void *) festate;
    MemoryContextRegisterResetCallback(reader_cxt, callback);

    node->fdw_state = festate;
}

/*
 * find_cmp_func
 *      Find comparison function for two given types.
 */
static void
find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2)
{
    Oid cmp_proc_oid;
    TypeCacheEntry *tce_1, *tce_2;

    tce_1 = lookup_type_cache(type1, TYPECACHE_BTREE_OPFAMILY);
    tce_2 = lookup_type_cache(type2, TYPECACHE_BTREE_OPFAMILY);

    cmp_proc_oid = get_opfamily_proc(tce_1->btree_opf,
                                     tce_1->btree_opintype,
                                     tce_2->btree_opintype,
                                     BTORDER_PROC);
    fmgr_info(cmp_proc_oid, finfo);
}

extern "C" TupleTableSlot *
parquetIterateForeignScan(ForeignScanState *node)
{
    ParquetS3FdwExecutionState   *festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    TupleTableSlot             *slot = node->ss.ss_ScanTupleSlot;
    std::string                 error;

    ExecClearTuple(slot);
    try
    {
        festate->next(slot);
    }
    catch (std::exception &e)
    {
        error = e.what();
    }
    if (!error.empty())
        elog(ERROR, "parquet_s3_fdw: %s", error.c_str());

    return slot;
}

extern "C" void
parquetEndForeignScan(ForeignScanState *node)
{
    /*
     * Destruction of execution state is done by memory context callback. See
     * destroy_parquet_state()
     */
    ForeignScan    *plan = (ForeignScan *) node->ss.ps.plan;
    List           *fdw_private = plan->fdw_private;
    int             i = 0;
    ListCell       *lc;
    Oid             foreigntableid = 0;

    foreach (lc, fdw_private)
    {
        if (i == FdwScanPrivateForeignTableId)
        {
            foreigntableid = intVal((Node *) lfirst(lc));
            break;
        }
        ++i;
    }

    if (foreigntableid != 0)
    {
        parquet_s3_server_opt *options = parquet_s3_get_options(foreigntableid);

        if (options->keep_connections == false)
            parquet_disconnect_s3_server();
    }
}

extern "C" void
parquetReScanForeignScan(ForeignScanState *node)
{
    ParquetS3FdwExecutionState   *festate = (ParquetS3FdwExecutionState *) node->fdw_state;

    festate->rescan();
}

static int
parquetAcquireSampleRowsFunc(Relation relation, int /* elevel */,
                             HeapTuple *rows, int targrows,
                             double *totalrows,
                             double *totaldeadrows)
{
    ParquetS3FdwExecutionState   *festate;
    ParquetFdwPlanState         fdw_private = {0};
    MemoryContext               reader_cxt;
    TupleDesc       tupleDesc = RelationGetDescr(relation);
    TupleTableSlot *slot;
    std::set<int>   attrs_used;
    int             cnt = 0;
    uint64          num_rows = 0;
    ListCell       *lc;
    std::string     error;
    bool            schemaless;
    std::set<std::string> slcols;

    get_table_options(RelationGetRelid(relation), &fdw_private);

    for (int i = 0; i < tupleDesc->natts; ++i)
        attrs_used.insert(i + 1 - FirstLowInvalidHeapAttributeNumber);

    reader_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                       "parquet_s3_fdw tuple data",
                                       ALLOCSET_DEFAULT_SIZES);
    if (IS_S3_PATH(fdw_private.dirname) || parquetIsS3Filenames(fdw_private.filenames))
        fdw_private.s3client = parquetGetConnectionByTableid(RelationGetRelid(relation), relation->rd_rel->relowner);
    else
        fdw_private.s3client = NULL;
    get_filenames_in_dir(&fdw_private);

    schemaless = fdw_private.schemaless;
    foreach(lc, fdw_private.slcols)
    {
        StringInfo *rcol = (StringInfo *)lfirst(lc);
        slcols.insert(std::string(strVal(rcol)));
    }

    festate = create_parquet_execution_state(RT_MULTI, reader_cxt,
                                             fdw_private.dirname,
                                             fdw_private.s3client,
                                             tupleDesc,
                                             attrs_used, std::list<SortSupportData>(),
                                             fdw_private.use_threads,
                                             false, 0, schemaless, slcols,
                                             std::set<std::string>());

    foreach (lc, fdw_private.filenames)
    {
        char *filename = strVal(lfirst(lc));

        try
        {
            std::unique_ptr<parquet::arrow::FileReader> reader;
            arrow::Status   status;
            List           *rowgroups = NIL;

            if (fdw_private.s3client)
            {
                arrow::MemoryPool* pool = arrow::default_memory_pool();
                char *dname;
                char *fname;
                parquetSplitS3Path(fdw_private.dirname, filename, &dname, &fname);
                std::shared_ptr<arrow::io::RandomAccessFile> input(new S3RandomAccessFile(fdw_private.s3client, dname, fname));
                status = parquet::arrow::OpenFile(input, pool, &reader);
                pfree(dname);
                pfree(fname);
            }
            else
            {
                status = parquet::arrow::FileReader::Make(
                            arrow::default_memory_pool(),
                            parquet::ParquetFileReader::OpenFile(filename, false),
                            &reader);
            }
            if (!status.ok())
                throw Error("parquet_s3_fdw: failed to open Parquet file: %s",
                                     status.message().c_str());
            auto meta = reader->parquet_reader()->metadata();
            num_rows += meta->num_rows();

            /* We need to scan all rowgroups */
            for (int i = 0; i < meta->num_row_groups(); ++i)
                rowgroups = lappend_int(rowgroups, i);
            festate->add_file(filename, rowgroups);
        }
        catch(const std::exception &e)
        {
            error = e.what();
        }
        if (!error.empty())
            elog(ERROR, "parquet_s3_fdw: %s", error.c_str());
    }

    PG_TRY();
    {
        uint64  row = 0;
        int     ratio = num_rows / targrows;

        /* Set ratio to at least 1 to avoid devision by zero issue */
        ratio = ratio < 1 ? 1 : ratio;


#if PG_VERSION_NUM < 120000
        slot = MakeSingleTupleTableSlot(tupleDesc);
#else
        slot = MakeSingleTupleTableSlot(tupleDesc, &TTSOpsHeapTuple);
#endif

        while (true)
        {
            CHECK_FOR_INTERRUPTS();

            if (cnt >= targrows)
                break;

            bool fake = (row % ratio) != 0;
            ExecClearTuple(slot);
            try {
                if (!festate->next(slot, fake))
                    break;
            } catch(std::exception &e) {
                error = e.what();
            }
            if (!error.empty())
                elog(ERROR, "parquet_s3_fdw: %s", error.c_str());

            if (!fake)
            {
                rows[cnt++] = heap_form_tuple(tupleDesc,
                                              slot->tts_values,
                                              slot->tts_isnull);
            }

            row++;
        }

        *totalrows = num_rows;
        *totaldeadrows = 0;

        ExecDropSingleTupleTableSlot(slot);
    }
    PG_CATCH();
    {
        elog(LOG, "parquet_s3_fdw: Cancelled");
        delete festate;
        PG_RE_THROW();
    }
    PG_END_TRY();

    delete festate;

    return cnt - 1;
}

extern "C" bool
parquetAnalyzeForeignTable(Relation /* relation */,
                           AcquireSampleRowsFunc *func,
                           BlockNumber * /* totalpages */)
{
    *func = parquetAcquireSampleRowsFunc;
    return true;
}

/*
 * parquetExplainForeignScan
 *      Additional explain information, namely row groups list.
 */
extern "C" void
parquetExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    List	   *fdw_private;
    ListCell   *lc, *lc2, *lc3;
    StringInfoData str;
    List       *filenames;
    List       *rowgroups_list;
    ReaderType  reader_type;

    initStringInfo(&str);

    fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
    filenames = (List *) linitial(fdw_private);
    reader_type = (ReaderType) intVal(list_nth(fdw_private, FdwScanPrivateType));
    rowgroups_list = (List *) list_nth(fdw_private, FdwScanPrivateRowGroups);

    switch (reader_type)
    {
        case RT_TRIVIAL:
            ExplainPropertyText("Reader", "Trivial", es);
            return; /* no rowgroups list output required, just return here */
        case RT_SINGLE:
            ExplainPropertyText("Reader", "Single File", es);
            break;
        case RT_MULTI:
            ExplainPropertyText("Reader", "Multifile", es);
            break;
        case RT_MULTI_MERGE:
            ExplainPropertyText("Reader", "Multifile Merge", es);
            break;
        case RT_CACHING_MULTI_MERGE:
            ExplainPropertyText("Reader", "Caching Multifile Merge", es);
            break;
    }

    forboth(lc, filenames, lc2, rowgroups_list)
    {
        char   *filename = strVal(lfirst(lc));
        List   *rowgroups = (List *) lfirst(lc2);
        bool    is_first = true;

        /* Only print filename if there're more than one file */
        if (list_length(filenames) > 1)
        {
            appendStringInfoChar(&str, '\n');
            appendStringInfoSpaces(&str, (es->indent + 1) * 2);

#ifdef _GNU_SOURCE
        appendStringInfo(&str, "%s: ", basename(filename));
#else
        appendStringInfo(&str, "%s: ", basename(pstrdup(filename)));
#endif
        }

        foreach(lc3, rowgroups)
        {
            /*
             * As parquet-tools use 1 based indexing for row groups it's probably
             * a good idea to output row groups numbers in the same way.
             */
            int rowgroup = lfirst_int(lc3) + 1;

            if (is_first)
            {
                appendStringInfo(&str, "%i", rowgroup);
                is_first = false;
            }
            else
                appendStringInfo(&str, ", %i", rowgroup);
        }
    }

    ExplainPropertyText("Row groups", str.data, es);
}

/* Parallel query execution */

extern "C" bool
parquetIsForeignScanParallelSafe(PlannerInfo * /* root */,
                                 RelOptInfo *rel,
                                 RangeTblEntry * /* rte */)
{
    /* Plan nodes that reference a correlated SubPlan is always parallel restricted.
     * Therefore, return false when there is lateral join.
     */
    if (rel->lateral_relids)
        return false;
    return true;
}

extern "C" Size
parquetEstimateDSMForeignScan(ForeignScanState *node,
                              ParallelContext * /* pcxt */)
{
    ParquetS3FdwExecutionState   *festate;

    festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    return festate->estimate_coord_size();
}

extern "C" void
parquetInitializeDSMForeignScan(ForeignScanState *node,
                                ParallelContext * pcxt,
                                void *coordinate)
{
    ParallelCoordinator        *coord = (ParallelCoordinator *) coordinate;
    ParquetS3FdwExecutionState   *festate;

    /*
    coord->i.s.next_rowgroup = 0;
    coord->i.s.next_reader = 0;
    SpinLockInit(&coord->lock);
    */
    festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    festate->set_coordinator(coord);
    festate->init_coord();
}

extern "C" void
parquetReInitializeDSMForeignScan(ForeignScanState *node,
                                  ParallelContext * /* pcxt */,
                                  void * /* coordinate */)
{
    ParquetS3FdwExecutionState   *festate;

    festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    festate->init_coord();
}

extern "C" void
parquetInitializeWorkerForeignScan(ForeignScanState *node,
                                   shm_toc * /* toc */,
                                   void *coordinate)
{
    ParallelCoordinator        *coord   = (ParallelCoordinator *) coordinate;
    ParquetS3FdwExecutionState   *festate;

    coord = new(coordinate) ParallelCoordinator;
    festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    festate->set_coordinator(coord);
}

extern "C" void
parquetShutdownForeignScan(ForeignScanState * /* node */)
{
}

extern "C" List *
parquetImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    struct dirent  *f;
    DIR            *d;
    List           *cmds = NIL;
    char           *dirname = pstrdup(stmt->remote_schema);
    char           *back;

    if (IS_S3_PATH(stmt->remote_schema))
        return parquetImportForeignSchemaS3(stmt, serverOid);

    d = AllocateDir(stmt->remote_schema);
    if (!d)
    {
        int e = errno;

        elog(ERROR, "parquet_s3_fdw: failed to open directory '%s': %s",
             stmt->remote_schema,
             strerror(e));
    }

    /* remove redundant slash */
    back = dirname + strlen(dirname);
    while (*--back == '/')
    {
        *back = '\0';
    }

    while ((f = readdir(d)) != NULL)
    {

        /* TODO: use lstat if d_type == DT_UNKNOWN */
        if (f->d_type == DT_REG)
        {
            ListCell   *lc;
            bool        skip = false;
            List       *fields;
            char       *filename = pstrdup(f->d_name);
            char       *path;
            char       *query;

            path = psprintf("%s/%s", dirname, filename);

            /* check that file extension is "parquet" */
            char *ext = strrchr(filename, '.');

            if (ext && strcmp(ext + 1, "parquet") != 0)
                continue;

            /*
             * Set terminal symbol to be able to run strcmp on filename
             * without file extension
             */
            *ext = '\0';

            foreach (lc, stmt->table_list)
            {
                RangeVar *rv = (RangeVar *) lfirst(lc);

                switch (stmt->list_type)
                {
                    case FDW_IMPORT_SCHEMA_LIMIT_TO:
                        if (strcmp(filename, rv->relname) != 0)
                        {
                            skip = true;
                            break;
                        }
                        break;
                    case FDW_IMPORT_SCHEMA_EXCEPT:
                        if (strcmp(filename, rv->relname) == 0)
                        {
                            skip = true;
                            break;
                        }
                        break;
                    default:
                        ;
                }
            }
            if (skip)
                continue;

            fields = extract_parquet_fields(path, NULL, NULL);

            query = create_foreign_table_query(filename, stmt->local_schema,
                                               stmt->server_name, &path, 1,
                                               fields, stmt->options);
            cmds = lappend(cmds, query);
        }

    }
    FreeDir(d);

    return cmds;
}

extern "C" Datum
parquet_fdw_validator_impl(PG_FUNCTION_ARGS)
{
    List       *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid         catalog = PG_GETARG_OID(1);
    ListCell   *lc;
    bool        filename_provided = false;
    bool        func_provided = false;

    /* Only check table options */
    if (catalog != ForeignTableRelationId)
        PG_RETURN_VOID();

    foreach(lc, options_list)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            char   *filename = pstrdup(defGetString(def));
            List   *filenames;
            ListCell *lc;

            if (filename_provided)
                elog(ERROR, "parquet_s3_fdw: either filename or dirname can be specified");

            filenames = parse_filenames_list(filename);

            foreach(lc, filenames)
            {
                struct stat stat_buf;
                char       *fn = strVal(lfirst(lc));

                if (IS_S3_PATH(fn))
                    continue;

                if (stat(fn, &stat_buf) != 0)
                {
                    int e = errno;

                    ereport(ERROR,
                            (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                             errmsg("parquet_s3_fdw: %s ('%s')", strerror(e), fn)));
                }
            }
            pfree(filenames);
            pfree(filename);
            filename_provided = true;
        }
        else if (strcmp(def->defname, "files_func") == 0)
        {
            Oid     jsonboid = JSONBOID;
#if PG_VERSION_NUM >= 160000
            List   *funcname = stringToQualifiedNameList(defGetString(def), NULL);
#else
            List   *funcname = stringToQualifiedNameList(defGetString(def));
#endif
            Oid     funcoid;
            Oid     rettype;

            /*
             * Lookup the function with a single JSONB argument and fail
             * if there isn't one.
             */
            funcoid = LookupFuncName(funcname, 1, &jsonboid, false);
            if ((rettype = get_func_rettype(funcoid)) != TEXTARRAYOID)
            {
                elog(ERROR, "parquet_s3_fdw: return type of '%s' is %s; expected text[]",
                     defGetString(def), format_type_be(rettype));
            }
            func_provided = true;
        }
        else if (strcmp(def->defname, "files_func_arg") == 0)
        {
            /*
             * Try to convert the string value into JSONB to validate it is
             * properly formatted.
             */
            DirectFunctionCall1(jsonb_in, CStringGetDatum(defGetString(def)));
        }
        else if (strcmp(def->defname, "sorted") == 0)
            ;  /* do nothing */
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            /* Check that bool value is valid */
            (void) defGetBoolean(def);
        }
        else if (strcmp(def->defname, "use_threads") == 0)
        {
            /* Check that bool value is valid */
            (void) defGetBoolean(def);
        }
        else if (strcmp(def->defname, "dirname") == 0)
        {
            char *dirname = defGetString(def);

            if (filename_provided)
                elog(ERROR, "parquet_s3_fdw: either filename or dirname can be specified");

            if (!IS_S3_PATH(dirname))
            {
                struct stat stat_buf;

                if (stat(dirname, &stat_buf) != 0)
                {
                    int e = errno;

                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("parquet_s3_fdw: %s", strerror(e))));
                }
                if (!S_ISDIR(stat_buf.st_mode))
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("parquet_s3_fdw: %s is not a directory", dirname)));

            }
            filename_provided = true;
        }
        else if (parquet_s3_is_valid_server_option(def))
        {
            /* Do nothing. */
        }
        else if (strcmp(def->defname, "max_open_files") == 0)
        {
            /* check that int value is valid */
#if PG_VERSION_NUM >= 150000
            pg_strtoint32(defGetString(def));
#else
            string_to_int32(defGetString(def));
#endif
        }
        else if (strcmp(def->defname, "files_in_order") == 0)
        {
            /* Check that bool value is valid */
			(void) defGetBoolean(def);
        }
        else if (strcmp(def->defname, "schemaless") == 0)
        {
            /* Check that bool value is valid */
			(void) defGetBoolean(def);
        }
        else if (strcmp(def->defname, "insert_file_selector") == 0)
        {
            /* We does not have foreign table type here, so do nothing */
        }
        else if (strcmp(def->defname, "key_columns") == 0)
        {
            /* We does not have foreign table type here, so do nothing */
        }
        else
        {
#if PG_VERSION_NUM >= 160000
            /*
            * Unknown option specified, complain about it. Provide a hint
            * with a valid option that looks similar, if there is one.
            */
            const char *closest_match;
            ClosestMatchState match_state;
            bool		has_valid_options = false;

            initClosestMatch(&match_state, def->defname, 4);
            closest_match = getClosestMatch(&match_state);
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                        errmsg("parquet_s3_fdw: invalid option \"%s\"", def->defname),
                        has_valid_options ? closest_match ?
                        errhint("parquet_s3_fdw: Perhaps you meant the option \"%s\".",
                                closest_match) : 0 :
                        errhint("parquet_s3_fdw: There are no valid options in this context.")));
#else
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("parquet_s3_fdw: invalid option \"%s\"",
                            def->defname)));
#endif
        }
    }

    if (!filename_provided && !func_provided)
        elog(ERROR, "parquet_s3_fdw: filename or function is required");

    PG_RETURN_VOID();
}

static List *
jsonb_to_options_list(Jsonb *options)
{
    List           *res = NIL;
	JsonbIterator  *it;
    JsonbValue      v;
    JsonbIteratorToken  type = WJB_DONE;

    if (!options)
        return NIL;

    if (!JsonContainerIsObject(&options->root))
        elog(ERROR, "parquet_s3_fdw: options must be represented by a jsonb object");

    it = JsonbIteratorInit(&options->root);
    while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        switch (type)
        {
            case WJB_BEGIN_OBJECT:
            case WJB_END_OBJECT:
                break;
            case WJB_KEY:
                {
                    DefElem    *elem;
                    char       *key;
                    char       *val;

                    if (v.type != jbvString)
                        elog(ERROR, "parquet_s3_fdw: expected a string key");
                    key = pnstrdup(v.val.string.val, v.val.string.len);

                    /* read value directly after key */
                    type = JsonbIteratorNext(&it, &v, false);
                    if (type != WJB_VALUE || v.type != jbvString)
                        elog(ERROR, "parquet_s3_fdw: expected a string value");
                    val = pnstrdup(v.val.string.val, v.val.string.len);

                    elem = makeDefElem(key, (Node *) makeString(val), 0);
                    res = lappend(res, elem);

                    break;
                }
            default:
                elog(ERROR, "parquet_s3_fdw: wrong options format");
        }
    }

    return res;
}

static List *
array_to_fields_list(ArrayType *attnames, ArrayType *atttypes)
{
    List   *res = NIL;
    Datum  *names;
    Datum  *types;
    bool   *nulls;
    int     nnames;
    int     ntypes;

    if (!attnames || !atttypes)
        elog(ERROR, "parquet_s3_fdw: attnames and atttypes arrays must not be NULL");

    if (ARR_HASNULL(attnames))
        elog(ERROR, "parquet_s3_fdw: attnames array must not contain NULLs");

    if (ARR_HASNULL(atttypes))
        elog(ERROR, "parquet_s3_fdw: atttypes array must not contain NULLs");

    deconstruct_array(attnames, TEXTOID, -1, false, 'i', &names, &nulls, &nnames);
    deconstruct_array(atttypes, REGTYPEOID, 4, true, 'i', &types, &nulls, &ntypes);

    if (nnames != ntypes)
        elog(ERROR, "parquet_s3_fdw: attnames and attypes arrays must have same length");

    for (int i = 0; i < nnames; ++i)
    {
        FieldInfo  *field = (FieldInfo *) palloc(sizeof(FieldInfo));
        char       *attname;
        attname = text_to_cstring(DatumGetTextP(names[i]));

        if (strlen(attname) >= NAMEDATALEN)
            elog(ERROR, "parquet_s3_fdw: attribute name cannot be longer than %i", NAMEDATALEN - 1);

        strcpy(field->name, attname);
        field->oid = types[i];

        res = lappend(res, field);
    }

    return res;
}

static void
validate_import_args(const char *tablename, const char *servername, Oid funcoid)
{
    if (!tablename)
        elog(ERROR, "parquet_s3_fdw: foreign table name is mandatory");

    if (!servername)
        elog(ERROR, "parquet_s3_fdw: foreign server name is mandatory");

    if (!OidIsValid(funcoid))
        elog(ERROR, "parquet_s3_fdw: function must be specified");
}

static void
import_parquet_internal(const char *tablename, const char *schemaname,
                        const char *servername, List *fields, Oid funcid,
                        Jsonb *arg, Jsonb *options) noexcept
{
    Datum       res;
    FmgrInfo    finfo;
    ArrayType  *arr;
    Oid         ret_type;
    List       *optlist;
    char       *query;

    validate_import_args(tablename, servername, funcid);

    if ((ret_type = get_func_rettype(funcid)) != TEXTARRAYOID)
    {
        elog(ERROR,
             "parquet_s3_fdw: return type of '%s' function is %s; expected text[]",
             get_func_name(funcid), format_type_be(ret_type));
    }

    optlist = jsonb_to_options_list(options);

    /* Call the user provided function */
    fmgr_info(funcid, &finfo);
    res = FunctionCall1(&finfo, (Datum) arg);

    /*
     * In case function returns NULL the ERROR is thrown. So it's safe to
     * assume function returned something. Just for the sake of readability
     * I leave this condition
     */
    if (res != (Datum) 0)
    {
        Datum  *values;
        bool   *nulls;
        int     num;
        int     ret;

        arr = DatumGetArrayTypeP(res);
        deconstruct_array(arr, TEXTOID, -1, false, 'i', &values, &nulls, &num);

        if (num == 0)
        {
            elog(WARNING,
                 "parquet_s3_fdw: '%s' function returned an empty array; foreign table wasn't created",
                 get_func_name(funcid));
            return;
        }

        /* Convert values to cstring array */
        char **paths = (char **) palloc(num * sizeof(char *));
        for (int i = 0; i < num; ++i)
        {
            if (nulls[i])
                elog(ERROR, "parquet_s3_fdw: user function returned an array containing NULL value(s)");
            paths[i] = text_to_cstring(DatumGetTextP(values[i]));
        }

        /*
         * If attributes list is provided then use it. Otherwise get the list
         * from the first file provided by the user function. We trust the user
         * to provide a list of files with the same structure.
         */
        fields = parquetExtractParquetFields(fields, paths, servername);

        query = create_foreign_table_query(tablename, schemaname, servername,
                                           paths, num, fields, optlist);

        /* Execute query */
        if (SPI_connect() < 0)
            elog(ERROR, "parquet_s3_fdw: SPI_connect failed");

        if ((ret = SPI_exec(query, 0)) != SPI_OK_UTILITY)
            elog(ERROR, "parquet_s3_fdw: failed to create table '%s': %s",
                 tablename, SPI_result_code_string(ret));

        SPI_finish();
    }
}

extern "C"
{

PG_FUNCTION_INFO_V1(import_parquet_s3);

Datum
import_parquet_s3(PG_FUNCTION_ARGS)
{
    char       *tablename;
    char       *schemaname;
    char       *servername;
    Oid         funcid;
    Jsonb      *arg;
    Jsonb      *options;

    tablename = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
    schemaname = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));
    servername = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(2));
    funcid = PG_ARGISNULL(3) ? InvalidOid : PG_GETARG_OID(3);
    arg = PG_ARGISNULL(4) ? NULL : PG_GETARG_JSONB_P(4);
    options = PG_ARGISNULL(5) ? NULL : PG_GETARG_JSONB_P(5);

    import_parquet_internal(tablename, schemaname, servername, NULL, funcid, arg, options);

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(import_parquet_s3_with_attrs);

Datum
import_parquet_s3_with_attrs(PG_FUNCTION_ARGS)
{
    char       *tablename;
    char       *schemaname;
    char       *servername;
    ArrayType  *attnames;
    ArrayType  *atttypes;
    Oid         funcid;
    Jsonb      *arg;
    Jsonb      *options;
    List       *fields;
    bool        schemaless = false;
    ListCell   *lc;

    tablename = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
    schemaname = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));
    servername = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(2));
    attnames = PG_ARGISNULL(3) ? NULL : PG_GETARG_ARRAYTYPE_P(3);
    atttypes = PG_ARGISNULL(4) ? NULL : PG_GETARG_ARRAYTYPE_P(4);
    funcid = PG_ARGISNULL(5) ? InvalidOid : PG_GETARG_OID(5);
    arg = PG_ARGISNULL(6) ? NULL : PG_GETARG_JSONB_P(6);
    options = PG_ARGISNULL(7) ? NULL : PG_GETARG_JSONB_P(7);


    foreach(lc, jsonb_to_options_list(options))
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "schemaless") == 0)
			schemaless = defGetBoolean(def);
    }

    if (schemaless)
    {
        if (attnames != NULL || atttypes != NULL)
            ereport(WARNING,
                    errmsg("parquet_s3_fdw: Attnames and atttypes are expected to be NULL. They are meaningless for schemaless table."),
                    errhint("Schemaless table imported always contain \"v\" column with \"jsonb\" type."));
        fields = NIL;
    }
    else
    {
        fields = array_to_fields_list(attnames, atttypes);
    }

    import_parquet_internal(tablename, schemaname, servername, fields,
                            funcid, arg, options);

    PG_RETURN_VOID();
}
}

/*
 * Get file names in specified directory.
 */
static void
get_filenames_in_dir(ParquetFdwPlanState *fdw_private)
{
    if (fdw_private->filenames)
        return;

    if (IS_S3_PATH(fdw_private->dirname))
        fdw_private->filenames = parquetGetS3ObjectList(fdw_private->s3client, fdw_private->dirname);
    else
        fdw_private->filenames = parquetGetDirFileList(fdw_private->filenames, fdw_private->dirname);

    if (fdw_private->filenames == NIL)
        elog(ERROR, "parquet_s3_fdw: object not found on %s", fdw_private->dirname);
}

/*
 * parquet_s3_is_select_all: True if all variables are selected
 */
bool
parquet_s3_is_select_all(RangeTblEntry *rte, List *tlist)
{
	int         i;
	int         natts = 0;
	int         natts_valid = 0;
	Relation	rel = table_open(rte->relid, NoLock);
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Oid         rel_type_id;
	bool        has_rel_type_id = false;
    bool        has_whole_row = false;
    bool        has_slcol = false;

	rel_type_id = get_rel_type_id(rte->relid);

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
		ListCell          *lc;

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		natts_valid++;

		foreach(lc, tlist)
		{
			Node *node = (Node *)lfirst(lc);

            if (IsA(node, TargetEntry))
				node = (Node *)((TargetEntry *) node)->expr;

			if (IsA(node, Var))
			{
				Var *var = (Var *) node;

				if (var->vartype == rel_type_id)
				{
					has_rel_type_id = true;
					break;
				}

                if (var->varattno == 0)
				{
					has_whole_row = true;
					break;
				}

				if (var->varattno == attr->attnum)
				{
                    if (attr->atttypid == JSONBOID)
                        has_slcol = true;

					natts++;
					break;
				}
			}
		}
        if (has_rel_type_id || has_whole_row || has_slcol)
            break;
	}

	table_close(rel, NoLock);

	return (natts == natts_valid) || has_rel_type_id || has_whole_row || has_slcol;
}

static void
parquet_s3_extract_slcols(ParquetFdwPlanState *fpinfo, PlannerInfo *root, RelOptInfo *baserel, List *tlist)
{
	RangeTblEntry  *rte;
    bool            is_select_all = false;
    List           *exprs = NULL;
    ListCell       *lc = NULL;
    List           *input_tlist = NIL;

	if (fpinfo->slinfo.schemaless == false)
		return;

    fpinfo->slcols = NIL;
    input_tlist = (tlist != NIL) ? tlist : baserel->reltarget->exprs;

	rte = planner_rt_fetch(baserel->relid, root);
	is_select_all = parquet_s3_is_select_all(rte, input_tlist);

    if (is_select_all == true)
        return;

	/* Extract schemaless variable names from input_tlist */
    fpinfo->slcols = parquet_s3_pull_slvars((Expr *)input_tlist, baserel->relid,
											fpinfo->slcols, false, NULL, &(fpinfo->slinfo));

    /*
     * Pull slvar from baserestrictinfo only.
     */
    foreach(lc, baserel->baserestrictinfo)
    {
        RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
        exprs = parquet_s3_pull_slvars(ri->clause, baserel->relid,
                                            exprs, true, NULL, &(fpinfo->slinfo));
    }

    foreach(lc, exprs)
    {
        fpinfo->slcols = parquet_s3_pull_slvars((Expr *)lfirst(lc), baserel->relid,
                                                    fpinfo->slcols, false, NULL, &(fpinfo->slinfo));
    }

}

/*
 * parquetAddForeignUpdateTargets
 *      Add resjunk column(s) needed for update/delete on a foreign table
 */
extern "C" void
parquetAddForeignUpdateTargets(
#if (PG_VERSION_NUM >= 140000)
                              PlannerInfo *root,
                              Index rtindex,
#else
                              Query *parsetree,
#endif
                              RangeTblEntry *target_rte,
                              Relation target_relation)
{
    Oid         relid = RelationGetRelid(target_relation);
    TupleDesc   tupdesc = target_relation->rd_att;
    int         i = 0;
    bool        has_key = false;
    ForeignTable *table;
	ListCell   *lc;
	bool		schemaless_opt = false;
	bool		key_columns_opt = false;

    table = GetForeignTable(relid);

	foreach (lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "schemaless") == 0)
			schemaless_opt = defGetBoolean(def);
		if (strcmp(def->defname, "key_columns") == 0)
			key_columns_opt = true;
	}
    /* in schemaless mode, add a resjunk for jsonb column as default */
	if (schemaless_opt)
    {
        if (key_columns_opt)
        {
            /* loop through all columns of the foreign table */
            for (i = 0; i < tupdesc->natts; ++i)
            {
                Form_pg_attribute att = TupleDescAttr(tupdesc, 0);
                AttrNumber	attrno = att->attnum;
                Var		   *var;
#if PG_VERSION_NUM < 140000
                Index rtindex = parsetree->resultRelation;
                TargetEntry *tle;
#endif

                if (att->attisdropped || att->atttypid != JSONBOID)
                    continue;

                var = makeVar(rtindex,
                            attrno,
                            att->atttypid,
                            att->atttypmod,
                            att->attcollation,
                            0);
#if (PG_VERSION_NUM >= 140000)
                add_row_identity_var(root, var, rtindex, pstrdup(NameStr(att->attname)));
#else
                /* Wrap it in a resjunk TLE with the right name ... */
                tle = makeTargetEntry((Expr *) var,
                                        list_length(parsetree->targetList) + 1,
                                        pstrdup(NameStr(att->attname)),
                                        true);

                /* ... and add it to the query's targetlist */
                parsetree->targetList = lappend(parsetree->targetList, tle);
#endif
                has_key = true;
            }
        }
        else
        {
            has_key = false;
        }
    }
    else
    {
        /* loop through all columns of the foreign table */
        for (i = 0; i < tupdesc->natts; ++i)
        {
            Form_pg_attribute   att = TupleDescAttr(tupdesc, i);
            AttrNumber          attrno = att->attnum;
            List               *options;
            ListCell           *option;

            /* look for the "key" option on this column */
            options = GetForeignColumnOptions(relid, attrno);
            foreach(option, options)
            {
                DefElem *def = (DefElem *) lfirst(option);

                /* if "key" is set, add a resjunk for this column */
                if(IS_KEY_COLUMN(def))
                {
                    Var *var;
#if PG_VERSION_NUM < 140000
                    Index rtindex = parsetree->resultRelation;
                    TargetEntry *tle;
#endif
                    var = makeVar(rtindex,
                                attrno,
                                att->atttypid,
                                att->atttypmod,
                                att->attcollation,
                                0);
#if (PG_VERSION_NUM >= 140000)
                    add_row_identity_var(root, var, rtindex, pstrdup(NameStr(att->attname)));
#else
                    /* Wrap it in a resjunk TLE with the right name ... */
                    tle = makeTargetEntry((Expr *) var,
                                        list_length(parsetree->targetList) + 1,
                                        pstrdup(NameStr(att->attname)),
                                        true);

                    /* ... and add it to the query's targetlist */
                    parsetree->targetList = lappend(parsetree->targetList, tle);
#endif
                    has_key = true;
                }
                else if (strcmp(def->defname, "key") == 0)
                {
                    elog(ERROR, "parquet_s3_fdw: impossible column option \"%s\"", def->defname);
                }
            }
        }
    }

    if (!has_key)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                 errmsg("parquet_s3_fdw: no primary key column specified for foreign table")));

}

/*
 * parquetPlanForeignModify
 *        Plan an insert/update/delete operation on a foreign table
 */
extern "C" List *
parquetPlanForeignModify(PlannerInfo *root,
                         ModifyTable *plan,
                         Index resultRelation,
                         int subplan_index)
{
    CmdType         operation = plan->operation;
    RangeTblEntry  *rte = planner_rt_fetch(resultRelation, root);
    List           *targetAttrs = NULL;
    Relation        rel;
    List           *keyAttrs = NULL;
    Oid             foreignTableId;
    TupleDesc       tupdesc;

    /*
     * Core code already has some lock on each rel being planned, so we can
     * use NoLock here.
     */
    rel = table_open(rte->relid, NoLock);
    foreignTableId = RelationGetRelid(rel);
    tupdesc = RelationGetDescr(rel);

    /*
     * In an INSERT, we transmit all columns that are defined in the foreign
     * table.  In an UPDATE, if there are BEFORE ROW UPDATE triggers on the
     * foreign table, we transmit all columns like INSERT; else we transmit
     * only columns that were explicitly targets of the UPDATE, so as to avoid
     * unnecessary data transmission.  (We can't do that for INSERT since we
     * would miss sending default values for columns not listed in the source
     * statement, and for UPDATE if there are BEFORE ROW UPDATE triggers since
     * those triggers might change values for non-target columns, in which
     * case we would miss sending changed values for those columns.)
     */
    if (operation == CMD_INSERT ||
        (operation == CMD_UPDATE &&
         rel->trigdesc &&
         rel->trigdesc->trig_update_before_row))
    {
        int     attnum;

        for (attnum = 1; attnum <= tupdesc->natts; attnum++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

            if (!attr->attisdropped)
                targetAttrs = lappend_int(targetAttrs, attnum);
        }
    }
    else if (operation == CMD_UPDATE)
    {
        AttrNumber  col;
#if (PG_VERSION_NUM >= 130010 && PG_VERSION_NUM < 140000) || \
    (PG_VERSION_NUM >= 140007 && PG_VERSION_NUM < 150000) || \
    (PG_VERSION_NUM >= 150002 && PG_VERSION_NUM < 160000) || \
    (PG_VERSION_NUM >= 160000)
        RelOptInfo *rel = find_base_rel(root, resultRelation);
        Bitmapset  *allUpdatedCols = get_rel_all_updated_cols(root, rel);
#else
        Bitmapset  *allUpdatedCols = bms_union(rte->updatedCols, rte->extraUpdatedCols);
#endif
        col = -1;
        while ((col = bms_next_member(allUpdatedCols, col)) >= 0)
        {
            /* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
            AttrNumber  attno = col + FirstLowInvalidHeapAttributeNumber;

            if (attno <= InvalidAttrNumber) /* shouldn't happen */
                elog(ERROR, "parquet_s3_fdw: system-column update is not supported");
            targetAttrs = lappend_int(targetAttrs, attno);
        }
    }

    /*
     * Raise error message if there is WITH CHECK OPTION
     */
    if (plan->withCheckOptionLists)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                 errmsg("parquet_s3_fdw: unsupported feature WITH CHECK OPTION")));
    }

    /*
     * Raise error if there is RETURNING
     */
    if (plan->returningLists)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                 errmsg("parquet_s3_fdw: unsupported feature RETURNING")));
    }

    /*
     * Raise error if there is ON CONFLICT
     */
    if (plan->onConflictAction != ONCONFLICT_NONE)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                 errmsg("parquet_s3_fdw: unsupported feature ON CONFLICT")));
    }

    /*
     * Add all primary key attribute names to keyAttrs
     */
    for (int i = 0; i < tupdesc->natts; ++i)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        AttrNumber    attrno = att->attnum;
        List       *options;
        ListCell   *option;

        /* look for the "key" option on this column */
        options = GetForeignColumnOptions(foreignTableId, attrno);
        foreach(option, options)
        {
            DefElem    *def = (DefElem *) lfirst(option);

            if (IS_KEY_COLUMN(def))
            {
                keyAttrs = lappend(keyAttrs, makeString(att->attname.data));
            }
        }
    }

    table_close(rel, NoLock);
    return list_make2(targetAttrs, keyAttrs);
}

/*
 * parquetBeginForeignModify
 *      Begin an insert/update/delete operation on a foreign table
 */
extern "C" void
parquetBeginForeignModify(ModifyTableState *mtstate,
                          ResultRelInfo *resultRelInfo,
                          List *fdw_private,
                          int subplan_index,
                          int eflags)
{
    ParquetS3FdwModifyState *fmstate = NULL;
    EState	               *estate = mtstate->ps.state;
    Relation                rel = resultRelInfo->ri_RelationDesc;
    Oid                     foreignTableId = InvalidOid;
    std::string             error;
    arrow::Status           status;
    Plan                   *subplan;
    ParquetFdwPlanState    *plstate;
    int                     i = 0;
    ListCell               *lc;
    MemoryContext           temp_cxt;
    TupleDesc               tupleDesc;
    std::set<std::string>   sorted_cols;
    std::set<std::string>   key_attrs;
    std::set<int>           target_attrs;
    MemoryContextCallback  *callback;
    Oid                     userid = InvalidOid;

#if (PG_VERSION_NUM >= 140000)
    subplan = outerPlanState(mtstate)->plan;
#else
    subplan = mtstate->mt_plans[subplan_index]->plan;
#endif

    temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                     "parquet_s3_fdw temporary data",
                                     ALLOCSET_DEFAULT_SIZES);
    foreignTableId = RelationGetRelid(rel);

    /* get table option */
    plstate = (ParquetFdwPlanState *) palloc0(sizeof(ParquetFdwPlanState));
    get_table_options(foreignTableId, plstate);

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
     * stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Get S3 connection */
    if (IS_S3_PATH(plstate->dirname) || parquetIsS3Filenames(plstate->filenames))
    {
#if PG_VERSION_NUM < 160000
        /*
         * Identify which user to do the remote access as.  This should match what
         * ExecCheckRTEPerms() does.
         */
        RangeTblEntry *rte = rt_fetch(resultRelInfo->ri_RangeTableIndex, estate->es_range_table);

        userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
#else
        /* Identify which user to do the remote access as. */
        userid = ExecGetResultRelCheckAsUser(resultRelInfo, estate);
#endif
        plstate->s3client = parquetGetConnectionByTableid(foreignTableId, userid);
    }
    else
        plstate->s3client = NULL;

    if (!plstate->filenames)
    {
        if (IS_S3_PATH(plstate->dirname))
            plstate->filenames = parquetGetS3ObjectList(plstate->s3client, plstate->dirname);
        else
            plstate->filenames = parquetGetDirFileList(plstate->filenames, plstate->dirname);
    }

    tupleDesc = RelationGetDescr(rel);

    foreach(lc, (List *) list_nth(fdw_private, 0))
        target_attrs.insert(lfirst_int(lc));

    AttrNumber *junk_idx = (AttrNumber *)palloc0(RelationGetDescr(rel)->natts * sizeof(AttrNumber));

    if (plstate->schemaless == true)
    {
        foreach(lc, plstate->key_columns)
        {
            char *key_column_name = (char *) lfirst(lc);

            key_attrs.insert(std::string(key_column_name));
        }
    }
    else
    {
        List        *key_attrs_name_list = (List *) list_nth(fdw_private, 1);

        foreach(lc, key_attrs_name_list)
        {
            char *key_column_name = strVal((Node *) lfirst(lc));

            key_attrs.insert(std::string(key_column_name));
        }
    }

    if (mtstate->operation != CMD_INSERT)
    {
        if (key_attrs.size() == 0)
            elog(ERROR, "parquet_s3_fdw: no primary key column specified for foreign table");
    }

    /* loop through table columns */
    for (i = 0; i < RelationGetDescr(rel)->natts; ++i)
    {
        /*
         * for primary key columns, get the resjunk attribute number and store it
         */
        junk_idx[i] = ExecFindJunkAttributeInTlist(subplan->targetlist,
                                         get_attname(foreignTableId, i + 1, false));
    }

    /*
     *  Get sorted column list.
     */
    foreach (lc, plstate->attrs_sorted)
    {
        char   *attname = (char *)lfirst(lc);

        sorted_cols.insert(std::string(attname));
    }

    try
    {
        fmstate = create_parquet_modify_state(temp_cxt, plstate->dirname, plstate->s3client, tupleDesc,
                                                 target_attrs, key_attrs, junk_idx, plstate->use_threads,
                                                 plstate->use_threads, plstate->schemaless, sorted_cols);

        fmstate->set_rel_name(RelationGetRelationName(rel));
        foreach(lc, plstate->filenames)
        {
            char *filename = strVal((Node *) lfirst(lc));

            fmstate->add_file(filename);
        }

        if (plstate->selector_function_name)
            fmstate->set_user_defined_func(plstate->selector_function_name);
    }
    catch(std::exception &e)
    {
        error = e.what();
    }
    if (!error.empty())
        elog(ERROR, "parquet_s3_fdw: %s", error.c_str());

    /*
     * Enable automatic execution state destruction by using memory context
     * callback
     */
    callback = (MemoryContextCallback *) palloc(sizeof(MemoryContextCallback));
    callback->func = destroy_parquet_modify_state;
    callback->arg = (void *) fmstate;
    MemoryContextRegisterResetCallback(estate->es_query_cxt, callback);

    resultRelInfo->ri_FdwState = fmstate;
}

/*
 * parquetExecForeignInsert
 *      Insert one row into a foreign table
 */
extern "C" TupleTableSlot *
parquetExecForeignInsert(EState *estate,
                         ResultRelInfo *resultRelInfo,
                         TupleTableSlot *slot,
                         TupleTableSlot *planSlot)
{
    ParquetS3FdwModifyState *fmstate = (ParquetS3FdwModifyState *) resultRelInfo->ri_FdwState;

    fmstate->exec_insert(slot);

    return slot;
}

/*
 * parquetExecForeignUpdate
 *      Update one row in a foreign table
 */
extern "C" TupleTableSlot *
parquetExecForeignUpdate(EState *estate,
                          ResultRelInfo *resultRelInfo,
                          TupleTableSlot *slot,
                          TupleTableSlot *planSlot)
{
    ParquetS3FdwModifyState *fmstate = (ParquetS3FdwModifyState *) resultRelInfo->ri_FdwState;

    fmstate->exec_update(slot, planSlot);

    return slot;
}

/*
 * postgresExecForeignDelete
 *      Delete one row from a foreign table
 */
extern "C" TupleTableSlot *
parquetExecForeignDelete(EState *estate,
                         ResultRelInfo *resultRelInfo,
                         TupleTableSlot *slot,
                         TupleTableSlot *planSlot)
{
    ParquetS3FdwModifyState *fmstate = (ParquetS3FdwModifyState *) resultRelInfo->ri_FdwState;

    fmstate->exec_delete(slot, planSlot);

    return slot;
}

/*
 * postgresEndForeignModify
 *      Finish an insert/update/delete operation on a foreign table
 */
extern "C" void
parquetEndForeignModify(EState *estate,
                        ResultRelInfo *resultRelInfo)
{
    ParquetS3FdwModifyState   *fmstate = (ParquetS3FdwModifyState *) resultRelInfo->ri_FdwState;

    if (fmstate != NULL)
        fmstate->upload();
}

/*
 * parquet_slot_to_record_datum: Convert TupleTableSlot to record type
 */
Datum
parquet_slot_to_record_datum(TupleTableSlot *slot)
{
    HeapTuple tuple = heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);

    return HeapTupleHeaderGetDatum(tuple->t_data);
}

/*
 * get_selected_file_from_userfunc
 *      return file name get from user defined funcion in insert_file_selector option.
 */
char *
get_selected_file_from_userfunc(char *func_signature, TupleTableSlot *slot, const char *dirname)
{
    Oid         funcid;
    List       *f;
    Datum       target_file;
    Oid        *funcargtypes;
    std::string funcname;
    std::vector<std::string> param_names;
    int         nargs;
    Datum      *args;
    bool       *is_nulls;

    parse_function_signature(func_signature, funcname, param_names);

#if PG_VERSION_NUM >= 160000
    f = stringToQualifiedNameList(funcname.c_str(), NULL);
#else
    f = stringToQualifiedNameList(funcname.c_str());
#endif

    nargs = param_names.size();
    funcargtypes = (Oid *) palloc0(sizeof(Oid) * nargs);
    args = (Datum *) palloc0(sizeof(Datum) * nargs);
    is_nulls = (bool *) palloc0(sizeof(bool) * nargs);

    for (int i = 0; i < nargs; i++)
    {
        if (param_names[i] == "dirname")
        {
            if (dirname)
            {
                Datum dirname_datum = (Datum) palloc0(strlen(dirname ) + VARHDRSZ);
                memcpy(VARDATA(dirname_datum), dirname, strlen(dirname));
                SET_VARSIZE(dirname_datum, strlen(dirname) + VARHDRSZ);

                funcargtypes[i] = TEXTOID;
                args[i] = dirname_datum;
                is_nulls[i] = false;
            }
            else
            {
                is_nulls[i] = true;
            }
        }
        else if (!get_column_by_name(param_names[i].c_str(), slot, &funcargtypes[i], &args[i], &is_nulls[i]))
        {
            elog(ERROR, "parquet_s3_fdw: arguments of insert_file_selector must be dirname or column name.");
        }
    }

    funcid = LookupFuncName(f, nargs, funcargtypes, false);
    target_file = OidFunctionCallnNullableArg(funcid, args, is_nulls, nargs);
    return TextDatumGetCString(target_file);
}

/*
 * get_column_by_name
 *      get type, value, isnull from given slot base on attname
 *      return false if column does not exist.
 */
static bool
get_column_by_name(const char *name, TupleTableSlot *slot, Oid *type, Datum *value, bool *isnull)
{
    TupleDesc desc = slot->tts_tupleDescriptor;

    for (int i = 0; i < desc->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(desc, i);

        if (strcmp(att->attname.data, name) == 0)
        {
            *value = slot_getattr(slot, att->attnum, isnull);
            *type = att->atttypid;
            return true;
        }
    }
    return false;
}

inline std::string trim(std::string str)
{
    str.erase(str.find_last_not_of(' ') + 1);   /* suffixing spaces */
    str.erase(0, str.find_first_not_of(' '));   /* prefixing spaces */
    return str;
}

/*
 * parse function signature: funcname(arg1_name, arg2_name)
 */
static void
parse_function_signature(char *signature, std::string &funcname, std::vector<std::string> &param_names)
{
    std::string s = std::string(signature);
    size_t pos = 0;

    /* find function name */
    pos = s.find("(");
    if (pos == std::string::npos)
        elog(ERROR, "parquet_s3_fdw: Malformed function signature: %s", signature);

    funcname = s.substr(0, pos);
    s.erase(0, pos + 1); /* erase '(' also */

    /* find function args */
    pos = s.find(")");
    if (pos == std::string::npos)
        elog(ERROR, "parquet_s3_fdw: Malformed function signature: %s", signature);

    std::string arg_list = s.substr(0, pos);
    do
    {
        pos = arg_list.find(",");
        std::string arg = trim(arg_list.substr(0, pos));
        param_names.push_back(arg);
        arg_list.erase(0, pos + 1);
    }
    while (pos != std::string::npos);
}

/*
 * check data source is existed or not.
 *        if dirname specify: return true if dirname has any parquet files. Otherwise return false.
 *        if filename specify: return true if any file in "filenames" existed. Otherwise return false.
 */
static bool
parquet_s3_datasource_is_exists(const char *dirname, List *filenames, Aws::S3::S3Client *s3_client, bool error_if_exists)
{
    if (dirname != NULL)
    {
        /* either filenames or dirname can be specified */
        Assert(filenames == NULL);

        /* check directory empty or not */
        if (s3_client)
            filenames = parquetGetS3ObjectList(s3_client, dirname);
        else
            filenames = parquetGetDirFileList(filenames, dirname);

        if (filenames != NULL)
        {
            if (error_if_exists)
                elog(ERROR, "parquet_s3_fdw: '%s' already existed", dirname);
            return true;
        }
        else
            return false;
    }
    else    /* filenames specified */
    {
        ListCell   *lc;

        foreach(lc, filenames)
        {
            char       *fn = strVal((Node *) lfirst(lc));

            if (s3_client)
            {
                char       *bucket;
                char       *filepath;

                parquetSplitS3Path(dirname, fn, &bucket, &filepath);
                if (parquet_is_object_exist(bucket, filepath, s3_client))
                {
                    if (error_if_exists)
                        elog(ERROR, "parquet_s3_fdw: '%s' already existed.", fn);
                    return true;
                }

                pfree(bucket);
                pfree(filepath);
            }
            else
            {
                if (is_file_exist(fn))
                {
                    if (error_if_exists)
                        elog(ERROR, "parquet_s3_fdw: '%s' already existed.", fn);
                    return true;
                }
            }
        }

        return false;
    }
}

/*
 * remove datasource file:
 *  - file name list: delete all files
 *  - dirname: delete all content in this directory
 *  - s3_client: connection to foreign server
 *  - skip_not_exist_error: true `IF EXIST` is specified
 */
void
parquet_s3_delete_datasource(const char *dirname, List *filenames, Aws::S3::S3Client *s3_client, bool skip_not_exist_error)
{
    ListCell *lc;

    /*
     * if dirname is s3 path then get list object from dirname.
     * if dirname is local path then delete folder tree.
     */
    if(dirname)
    {
        if (IS_S3_PATH(dirname))
        {
            filenames = parquetGetS3ObjectList(s3_client, dirname);
            if (!filenames)
            {
                if (!skip_not_exist_error)
                    elog(ERROR, "parquet_s3_fdw: dirname '%s' does not exist from bucket.", dirname);
            }
        }
        else
        {
            /* delete all content of local directory */
            delete_folder_tree(dirname);
            return;
        }
    }

    /*
     * 'filename' option or dirname option with s3-path is specified.
     * remove all file in file name list.
     */
    foreach (lc, filenames)
    {
        char           *bucket;
        char           *filepath;
        char           *filename;

        filename = strVal(lfirst(lc));

        /* filename is specified with local */
        if (dirname == NULL && !IS_S3_PATH(filename))
        {
            /* filename is in local storage */
            std::remove(filename);
        }
        else
        {
            /* filename is object on S3 (not include 's3://bucketname') */
            parquetSplitS3Path(dirname, filename, &bucket, &filepath);

            if (parquet_is_object_exist(bucket, filepath, s3_client))
                parquet_delete_object(bucket, filepath, s3_client);
            else
            {
                if (!skip_not_exist_error)
                    elog(ERROR, "parquet_s3_fdw: Object '%s' does not exist from bucket '%s'.", filepath, bucket);
            }
            pfree(bucket);
            pfree(filepath);
        }
    }
    /* Releasing resource */
    parquetReleaseConnection(s3_client);
}

/*
 * ExecForeignDDL is a public function that is called by core code.
 * It executes DDL command on remote server.
 *
 * serverOid: remote server to get connected
 * rel: relation to be created
 * operation:
 *      0: CREATE command
 *      1: DROP command
 * exists_flag:
 *      in CREATE DDL: true if `IF NOT EXIST` is specified
 *      in DROP DDL: true if `IF EXIST` is specified
 */
#if (PG_VERSION_NUM >=160000)
PGDLLEXPORT
#endif
int ExecForeignDDL(Oid serverOid,
                   Relation rel,
                   int operation,
                   bool exists_flag)
{
    bool            use_minio = false;
    ForeignTable   *table;
    ForeignServer  *server;
    UserMapping    *user = NULL;
    char           *dirname = NULL;
    List           *filenames = NULL;
    ListCell       *lc;
    Aws::S3::S3Client *s3_client;

    elog(DEBUG1, "parquet_s3_fdw: %s", __func__);

    if (operation !=  SPD_CMD_CREATE && operation != SPD_CMD_DROP)
    {
        elog(ERROR, "parquet_s3_fdw: Only support CREATE/DROP DATASOURCE");
    }

    /*
     * Get option information from foreign server and foreign table.
     * Get connection to the foreign server. Connection manager will
     * establish new connection if necessary.
     */
    server = GetForeignServer(serverOid);
    table = GetForeignTable(RelationGetRelid(rel));
    user = GetUserMapping(GetUserId(), serverOid);

    parquet_s3_server_opt *options = parquet_s3_get_options(serverOid);
    foreach(lc, server->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, SERVER_OPTION_USE_MINIO) == 0)
        {
            use_minio = defGetBoolean(def);
            break;
        }
    }

    foreach(lc, table->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
            filenames = parse_filenames_list(defGetString(def));
        if (strcmp(def->defname, "dirname") == 0)
            dirname = defGetString(def);
    }

    if (IS_S3_PATH(dirname) || parquetIsS3Filenames(filenames))
        s3_client = parquetGetConnection(user, options);
    else
        s3_client = NULL;

    /* SPD_CMD_CREATE */
    if (operation == SPD_CMD_CREATE)
    {
        /* 'IF NOT EXISTS' does not specified, raise error if datasource already exist */
        if (exists_flag == false)
            parquet_s3_datasource_is_exists(dirname, filenames, s3_client, true);

        /* Releasing resource */
        parquetReleaseConnection(s3_client);

        /* The new file will be created during INSERT, so it just return success */
        return 0;
    }

    /* SPD_CMD_DROP */
    if (operation == SPD_CMD_DROP)
        parquet_s3_delete_datasource(dirname, filenames, s3_client, exists_flag);

    return 0;
}
