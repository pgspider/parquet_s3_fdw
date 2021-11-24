/*-------------------------------------------------------------------------
 *
 * parquet_impl.cpp
 *		  Parquet processing implementation for parquet_s3_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
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
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#else
#include "access/table.h"
#include "access/relation.h"
#include "optimizer/optimizer.h"
#endif
}


/* from costsize.c */
#define LOG2(x)  (log(x) / 0.693147180559945)

#if PG_VERSION_NUM < 110000
#define PG_GETARG_JSONB_P PG_GETARG_JSONB
#endif


static void find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2);
static void destroy_parquet_state(void *arg);


/*
 * Restriction
 */
struct RowGroupFilter
{
    AttrNumber  attnum;
    Const       *value;
    int         strategy;
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
    List       *rowgroups;      /* List of Lists (per filename) */
    uint64      ntuples;
    ReaderType  type;
    char       *dirname;
    Aws::S3::S3Client *s3client;
};

static void get_filenames_in_dir(ParquetFdwPlanState *fdw_private);

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
        TypeCacheEntry *tce;
        Expr       *clause = (Expr *) lfirst(lc);
        OpExpr     *expr;
        Expr       *left, *right;
        int         strategy;
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

            /* TODO */
            tce = lookup_type_cache(exprType((Node *) left),
                                    TYPECACHE_BTREE_OPFAMILY);
            strategy = get_op_opfamily_strategy(opno, tce->btree_opf);

            /* Not a btree family operator? */
            if (strategy == 0)
                continue;
        }
        else if (IsA(clause, Var))
        {
            /*
             * Trivial expression containing only a single boolean Var. This
             * also covers cases "BOOL_VAR = true"
             * */
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

        RowGroupFilter f
        {
            .attnum = v->varattno,
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

/*
 * row_group_matches_filter
 *      Check if min/max values of the column of the row group match filter.
 */
static bool
row_group_matches_filter(parquet::Statistics *stats,
                         arrow::DataType *arrow_type,
                         RowGroupFilter *filter)
{
    FmgrInfo finfo;
    Datum    val = filter->value->constvalue;
    int      collid = filter->value->constcollid;
    int      strategy = filter->strategy;

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

                lower = bytes_to_postgres_type(stats->EncodeMin().c_str(),
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

                upper = bytes_to_postgres_type(stats->EncodeMax().c_str(),
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
            {
                Datum   lower,
                        upper;

                lower = bytes_to_postgres_type(stats->EncodeMin().c_str(),
                                               arrow_type);
                upper = bytes_to_postgres_type(stats->EncodeMax().c_str(),
                                               arrow_type);

                int l = FunctionCall2Coll(&finfo, collid, val, lower);
                int u = FunctionCall2Coll(&finfo, collid, val, upper);

                if (l < 0 || u > 0)
                    return false;
                break;
            }

        default:
            /* should not happen */
            Assert(true);
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
    loc = parquetFilenamesValidator(f, loc);
    filenames = lappend(filenames, makeString(f));

    return filenames;
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
                       uint64 *ntuples) noexcept
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
            throw Error("parquet_s3_fdw: failed to open Parquet file %s", status.message().c_str());

        auto meta = reader->parquet_reader()->metadata();
        parquet::ArrowReaderProperties  props;
        parquet::arrow::SchemaManifest  manifest;

        status = parquet::arrow::SchemaManifest::Make(meta->schema(), nullptr,
                                                      props, &manifest);
        if (!status.ok())
            throw Error("parquet_s3_fdw: error creating arrow schema");

        /* Check each row group whether it matches the filters */
        for (int r = 0; r < reader->num_row_groups(); r++)
        {
            bool match = true;
            auto rowgroup = meta->RowGroup(r);

            for (auto &filter : filters)
            {
                AttrNumber      attnum;
                const char     *attname;

                attnum = filter.attnum - 1;
                attname = NameStr(TupleDescAttr(tupleDesc, attnum)->attname);

                /*
                 * Search for the column with the same name as filtered attribute
                 */
                for (auto &schema_field : manifest.schema_fields)
                {
                    MemoryContext   ccxt = CurrentMemoryContext;
                    bool            error = false;
                    char            errstr[ERROR_STR_LEN];
                    auto           &field = schema_field.field;

                    /* Skip complex objects (lists, maps) */
                    if (schema_field.column_index == -1)
                        continue;

                    if (strcmp(attname, schema_field.field->name().c_str()) != 0)
                        continue;

                    /* Found it! */
                    std::shared_ptr<parquet::Statistics>  stats;
                    auto column = rowgroup->ColumnChunk(schema_field.column_index);
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
                *ntuples += rowgroup->num_rows();
            }
        }  /* loop over rowgroups */
    }
    catch(const std::exception& e) {
        error = e.what();      
    }
    if (!error.empty()) {
        if (reader_entry)
            reader_entry->file_reader->reader = std::move(reader);
        elog(ERROR,
             "parquet_s3_fdw: failed to exctract row groups from Parquet file: %s",
             error.c_str());
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
            throw Error("parquet_s3_fdw: failed to open Parquet file %s",
                                 status.message().c_str());

        auto p_schema = reader->parquet_reader()->metadata()->schema();
        if (!parquet::arrow::SchemaManifest::Make(p_schema, nullptr, props, &manifest).ok())
            throw std::runtime_error("parquet_s3_fdw: error creating arrow schema");

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

                    if (type->fields().size() != 1)
                        throw std::runtime_error("lists of structs are not supported");

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
                        throw std::runtime_error("failed to get the type of array elements");
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
                throw Error("parquet_s3_fdw: cannot convert field '%s' of type '%s' in %s",
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

    initStringInfo(&str);
    appendStringInfo(&str, "CREATE FOREIGN TABLE ");

    /* append table name */
    if (schemaname)
        appendStringInfo(&str, "%s.%s (",
                         quote_identifier(schemaname), quote_identifier(tablename));
    else
        appendStringInfo(&str, "%s (", quote_identifier(tablename));

    /* append columns */
    bool is_first = true;
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

/*
 * C interface functions
 */

static List *
parse_attributes_list(char *start, Oid relid)
{
    List      *attrs = NIL;
    char      *token;
    const char *delim = " ";
    AttrNumber attnum;

    while ((token = strtok(start, delim)) != NULL)
    {
        if ((attnum = get_attnum(relid, token)) == InvalidAttrNumber)
            elog(ERROR, "paruqet_s3_fdw: invalid attribute name '%s'", token);
        attrs = lappend_int(attrs, attnum);
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

static List *
get_filenames_from_userfunc(const char *funcname, const char *funcarg)
{
    Jsonb      *j = NULL;
    Oid         funcid;
    List       *f = stringToQualifiedNameList(funcname);
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
                parse_attributes_list(defGetString(def), relid);
        }
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            if (!parse_bool(defGetString(def), &fdw_private->use_mmap))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("parquet_s3_fdw: invalid value for boolean option \"%s\": %s",
                                def->defname, defGetString(def))));
        }
        else if (strcmp(def->defname, "use_threads") == 0)
        {
            if (!parse_bool(defGetString(def), &fdw_private->use_threads))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("parquet_s3_fdw: invalid value for boolean option \"%s\": %s",
                                def->defname, defGetString(def))));
        }
        else if (strcmp(def->defname, "dirname") == 0)
        {
            fdw_private->dirname = defGetString(def);
        }
        else if (strcmp(def->defname, "max_open_files") == 0)
        {
            /* check that int value is valid */
            fdw_private->max_open_files = pg_atoi(defGetString(def), sizeof(int32), '\0');
        }
        else
            elog(ERROR, "parquet_s3_fdw: unknown option '%s'", def->defname);
    }

    if (funcname)
        fdw_private->filenames = get_filenames_from_userfunc(funcname, funcarg);
}

extern "C" void
parquetGetForeignRelSize(PlannerInfo * /* root */,
                         RelOptInfo *baserel,
                         Oid foreigntableid)
{
    ParquetFdwPlanState *fdw_private;

    fdw_private = (ParquetFdwPlanState *) palloc0(sizeof(ParquetFdwPlanState));
    get_table_options(foreigntableid, fdw_private);
    
    /* For PGSpider. Overwrite. */
    if (baserel->fdw_private != NIL)
    {
        fdw_private->filenames = (List *) list_nth((List *) baserel->fdw_private, FdwScanPrivateFileNames);
        fdw_private->dirname = NULL;
    }

    if (IS_S3_PATH(fdw_private->dirname) || parquetIsS3Filenames(fdw_private->filenames))
        fdw_private->s3client = parquetGetConnectionByTableid(foreigntableid);
    else
        fdw_private->s3client = NULL;
    get_filenames_in_dir(fdw_private);

    baserel->fdw_private = fdw_private;
}

static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost,
               Cost *run_cost, Cost *total_cost)
{
    auto    fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;
    double  ntuples;

    /* Use statistics if we have it */
    if (baserel->tuples)
    {
        ntuples = baserel->tuples *
            clauselist_selectivity(root,
                                   baserel->baserestrictinfo,
                                   0,
                                   JOIN_INNER,
                                   NULL);

    }
    else
    {
        /*
         * If there is no statistics then use estimate based on rows number
         * in the selected row groups.
         */
        ntuples = fdw_private->ntuples;
    }

    /*
     * Here we assume that parquet tuple cost is the same as regular tuple cost
     * even though this is probably not true in many cases. Maybe we'll come up
     * with a smarter idea later.
     */
    *run_cost = ntuples * cpu_tuple_cost;
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
    RangeTblEntry  *rte;
    Relation        rel;
    TupleDesc       tupleDesc;
    std::list<RowGroupFilter> filters;
    ListCell       *lc;

    fdw_private = (ParquetFdwPlanState *) baserel->fdw_private;
    is_sorted = fdw_private->attrs_sorted != NIL;
    is_multi = list_length(fdw_private->filenames) > 1;

    /* Analyze query clauses and extract ones that can be of interest to us*/
    extract_rowgroup_filters(baserel->baserestrictinfo, filters);

    rte = root->simple_rte_array[baserel->relid];
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
    foreach (lc, fdw_private->filenames)
    {
        char *filename = strVal((Value *) lfirst(lc));
        List *rowgroups = extract_rowgroups_list(filename, fdw_private->dirname, fdw_private->s3client, tupleDesc,
                                                 filters, &fdw_private->ntuples);

        fdw_private->rowgroups = lappend(fdw_private->rowgroups, rowgroups);
    }
#if PG_VERSION_NUM < 120000
    heap_close(rel, AccessShareLock);
#else
    table_close(rel, AccessShareLock);
#endif

    estimate_costs(root, baserel, &startup_cost, &run_cost, &total_cost);

    /* Collect used attributes to reduce number of read columns during scan */
    extract_used_attributes(baserel);

    fdw_private->type = is_multi ? RT_MULTI : RT_SINGLE;

    /* Build pathkeys based on attrs_sorted */
    foreach (lc, fdw_private->attrs_sorted)
    {
        Oid         relid = root->simple_rte_array[baserel->relid]->relid;
        int         attnum = lfirst_int(lc);
        Oid         typid,
                    collid;
        int32       typmod;
        Oid         sort_op;
        Var        *var;
        List       *attr_pathkey;

        /* Build an expression (simple var) */
        get_atttypetypmodcoll(relid, attnum, &typid, &typmod, &collid);
        var = makeVar(baserel->relid, attnum, typid, typmod, collid, 0);

        /* Lookup sorting operator for the attribute type */
        get_sort_group_operators(typid,
                                 true, false, false,
                                 &sort_op, NULL, NULL,
                                 NULL);

        attr_pathkey = build_expression_pathkey(root, (Expr *) var, NULL,
                                                sort_op, baserel->relids,
                                                true);
        pathkeys = list_concat(pathkeys, attr_pathkey);
    }

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
                       startup_cost, total_cost, baserel->rows);
        }
        add_path(baserel, path);
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
	add_path(baserel, (Path *) foreign_path);

    if (baserel->consider_parallel > 0)
    {
        ParquetFdwPlanState *private_parallel;

        private_parallel = (ParquetFdwPlanState *) palloc(sizeof(ParquetFdwPlanState));
        memcpy(private_parallel, fdw_private, sizeof(ParquetFdwPlanState));
        private_parallel->type = is_multi ? RT_MULTI : RT_SINGLE;

        Path *parallel_path = (Path *)
                 create_foreignscan_path(root, baserel,
                                         NULL,	/* default pathtarget */
                                         baserel->rows,
                                         startup_cost,
                                         total_cost,
                                         pathkeys,
                                         baserel->lateral_relids,
                                         NULL,	/* no extra plan */
                                         (List *) private_parallel);

        int num_workers = max_parallel_workers_per_gather;

        parallel_path->rows = fdw_private->ntuples / (num_workers + 1);
        parallel_path->total_cost       = startup_cost + run_cost / num_workers;
        parallel_path->parallel_workers = num_workers;
        parallel_path->parallel_aware   = true;
        parallel_path->parallel_safe    = true;

        /* Create GatherMerge path for sorted parquet files */
        if (is_sorted)
        {
            GatherMergePath *gather_merge =
                create_gather_merge_path(root, baserel, parallel_path, NULL,
                                         pathkeys, NULL, NULL);
            add_path(baserel, (Path *) gather_merge);
        }
        add_partial_path(baserel, parallel_path);
    }
}

extern "C" ForeignScan *
parquetGetForeignPlan(PlannerInfo * /* root */,
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
        attrs_sorted = lappend_int(attrs_sorted, lfirst_int(lc));

    /* Packing all the data needed by executor into the list */
    params = lappend(params, fdw_private->filenames);
    params = lappend(params, attrs_used);
    params = lappend(params, attrs_sorted);
    params = lappend(params, makeInteger(fdw_private->use_mmap));
    params = lappend(params, makeInteger(fdw_private->use_threads));
    params = lappend(params, makeInteger(fdw_private->type));
    params = lappend(params, makeInteger(fdw_private->max_open_files));
    params = lappend(params, fdw_private->rowgroups);

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
            params = lappend(params, makeString(""));
        else
            params = lappend(params, makeString(fdw_private->dirname));
        params = lappend(params, makeInteger(foreigntableid));
    }
    else
    {
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
    ParquetS3FdwExecutionState   *festate;
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
                use_mmap = (bool) intVal((Value *) lfirst(lc));
                break;
            case FdwScanPrivateUse_Threads:
                use_threads = (bool) intVal((Value *) lfirst(lc));
                break;
            case FdwScanPrivateType:
                reader_type = (ReaderType) intVal((Value *) lfirst(lc));
                break;
            case FdwScanPrivateMaxOpenFiles:
                max_open_files = intVal((Value *) lfirst(lc));
                break;
            case FdwScanPrivateRowGroups:
                rowgroups_list = (List *) lfirst(lc);
                break;
            case FdwScanPrivateDirName:
                dirname = (char *) strVal((Value *) lfirst(lc));
                break;
            case FdwScanPrivateForeignTableId:
            {
                /* Recreate S3 handle by foreign table id. */
                Oid s3tableoid = intVal((Value *) lfirst(lc));
                s3client = parquetGetConnectionByTableid(s3tableoid);
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
        int     attr = lfirst_int(lc);
        Oid     typid;
        int     typmod;
        Oid     collid;
        Oid     relid = RelationGetRelid(node->ss.ss_currentRelation);
        Oid     sort_op;

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

    try
    {
        festate = create_parquet_execution_state(reader_type, reader_cxt, dirname, s3client, tupleDesc,
                                                 attrs_used, sort_keys,
                                                 use_threads, use_mmap,
                                                 max_open_files);

        if (!filenames)
            throw std::runtime_error("parquet_s3_fdw: got an empty filenames list");

        forboth (lc, filenames, lc2, rowgroups_list)
        {
            char *filename = strVal((Value *) lfirst(lc));
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
            foreigntableid = intVal((Value *) lfirst(lc));
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

    get_table_options(RelationGetRelid(relation), &fdw_private);

    for (int i = 0; i < tupleDesc->natts; ++i)
        attrs_used.insert(i + 1 - FirstLowInvalidHeapAttributeNumber);

    reader_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                       "parquet_s3_fdw tuple data",
                                       ALLOCSET_DEFAULT_SIZES);
    if (IS_S3_PATH(fdw_private.dirname) || parquetIsS3Filenames(fdw_private.filenames))
        fdw_private.s3client = parquetGetConnectionByTableid(RelationGetRelid(relation));
    else
        fdw_private.s3client = NULL;
    get_filenames_in_dir(&fdw_private);

    festate = create_parquet_execution_state(RT_MULTI, reader_cxt, 
                                             fdw_private.dirname,
                                             fdw_private.s3client,
                                             tupleDesc,
                                             attrs_used, std::list<SortSupportData>(),
                                             fdw_private.use_threads,
                                             false, 0);

    foreach (lc, fdw_private.filenames)
    {
        char *filename = strVal((Value *) lfirst(lc));

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
    reader_type = (ReaderType) intVal((Value *) list_nth(fdw_private, FdwScanPrivateType));
    rowgroups_list = (List *) list_nth(fdw_private, FdwScanPrivateRowGroups);

    switch (reader_type)
    {
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
        char   *filename = strVal((Value *) lfirst(lc));
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
    /* Use parallel execution only when statistics are collected */
    return (rel->tuples > 0);
}

extern "C" Size
parquetEstimateDSMForeignScan(ForeignScanState * /* node */,
                              ParallelContext * /* pcxt */)
{
    return sizeof(ParallelCoordinator);
}

extern "C" void
parquetInitializeDSMForeignScan(ForeignScanState *node,
                                ParallelContext * /* pcxt */,
                                void *coordinate)
{
    ParallelCoordinator        *coord = (ParallelCoordinator *) coordinate;
    ParquetS3FdwExecutionState   *festate;

    coord->next_rowgroup = 0;
    coord->next_reader = 0;
    SpinLockInit(&coord->lock);
    festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    festate->set_coordinator(coord);
}

extern "C" void
parquetReInitializeDSMForeignScan(ForeignScanState * /* node */,
                                  ParallelContext * /* pcxt */,
                                  void *coordinate)
{
    ParallelCoordinator    *coord = (ParallelCoordinator *) coordinate;

    coord->next_rowgroup = 0;
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

            path = psprintf("%s/%s", stmt->remote_schema, filename);

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
                char       *fn = strVal((Value *) lfirst(lc));

               if (IS_S3_PATH(fn))
                   continue;

                if (stat(fn, &stat_buf) != 0)
                {
                    int e = errno;

                    ereport(ERROR,
                            (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                             errmsg("parquet_s3_fdw: %s", strerror(e))));
                }
            }
            pfree(filenames);
            pfree(filename);
            filename_provided = true;
        }
        else if (strcmp(def->defname, "files_func") == 0)
        {
            Oid     jsonboid = JSONBOID;
            List   *funcname = stringToQualifiedNameList(defGetString(def)); 
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
            bool    use_mmap;

            if (!parse_bool(defGetString(def), &use_mmap))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("parquet_s3_fdw: invalid value for boolean option \"%s\": %s",
                                def->defname, defGetString(def))));
        }
        else if (strcmp(def->defname, "use_threads") == 0)
        {
            /* Check that bool value is valid */
            bool    use_threads;

            if (!parse_bool(defGetString(def), &use_threads))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("parquet_s3_fdw: invalid value for boolean option \"%s\": %s",
                                def->defname, defGetString(def))));
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
            pg_atoi(defGetString(def), sizeof(int32), '\0');
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("parquet_s3_fdw: invalid option \"%s\"",
                            def->defname)));
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

    tablename = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
    schemaname = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));
    servername = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(2));
    attnames = PG_ARGISNULL(3) ? NULL : PG_GETARG_ARRAYTYPE_P(3);
    atttypes = PG_ARGISNULL(4) ? NULL : PG_GETARG_ARRAYTYPE_P(4);
    funcid = PG_ARGISNULL(5) ? InvalidOid : PG_GETARG_OID(5);
    arg = PG_ARGISNULL(6) ? NULL : PG_GETARG_JSONB_P(6);
    options = PG_ARGISNULL(7) ? NULL : PG_GETARG_JSONB_P(7);

    fields = array_to_fields_list(attnames, atttypes);

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

