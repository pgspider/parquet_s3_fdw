/*-------------------------------------------------------------------------
 *
 * common.cpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/common.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "common.hpp"

extern "C"
{
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/timestamp.h"
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>
#include <unistd.h>
}

#if PG_VERSION_NUM < 130000
#define MAXINT8LEN 25
#endif

/*
 * exc_palloc
 *      C++ specific memory allocator that utilizes postgres allocation sets.
 */
void *
exc_palloc(std::size_t size)
{
    /* duplicates MemoryContextAllocZero to avoid increased overhead */
    void	   *ret;
    MemoryContext context = CurrentMemoryContext;

    AssertArg(MemoryContextIsValid(context));

    if (!AllocSizeIsValid(size))
        throw std::bad_alloc();

    context->isReset = false;

    ret = context->methods->alloc(context, size);
    if (unlikely(ret == NULL))
        throw std::bad_alloc();

    VALGRIND_MEMPOOL_ALLOC(context, ret, size);

    return ret;
}

Oid
to_postgres_type(int arrow_type)
{
    switch (arrow_type)
    {
        case arrow::Type::BOOL:
            return BOOLOID;
        case arrow::Type::INT8:
        case arrow::Type::INT16:
            return INT2OID;
        case arrow::Type::INT32:
            return INT4OID;
        case arrow::Type::INT64:
            return INT8OID;
        case arrow::Type::FLOAT:
            return FLOAT4OID;
        case arrow::Type::DOUBLE:
            return FLOAT8OID;
        case arrow::Type::STRING:
            return TEXTOID;
        case arrow::Type::BINARY:
            return BYTEAOID;
        case arrow::Type::TIMESTAMP:
            return TIMESTAMPOID;
        case arrow::Type::DATE32:
            return DATEOID;
        default:
            return InvalidOid;
    }
}

/**
 * @brief return mapping arrow type of given postgres type
 *
 * @param postgres_type postgres type Oid
 * @return arrow::Type::type mapped arrow type
 */
arrow::Type::type
postgres_to_arrow_type(Oid postgres_type)
{
    switch (postgres_type)
    {
        case BOOLOID:
            return arrow::Type::BOOL;
        case INT2OID:
            return arrow::Type::INT16;
        case INT4OID:
            return arrow::Type::INT32;
        case INT8OID:
            return arrow::Type::INT64;
        case FLOAT4OID:
            return arrow::Type::FLOAT;
        case FLOAT8OID:
        case NUMERICOID:
            return arrow::Type::DOUBLE;
        case TEXTOID:
        case VARCHAROID:
            return arrow::Type::STRING;
        case BYTEAOID:
            return arrow::Type::BINARY;
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:  /* UTC timezone will be used */
            return arrow::Type::TIMESTAMP;
        case DATEOID:
            return arrow::Type::DATE32;
        default:
            elog(DEBUG3, "parquet_s3_fdw: Does not support create arrow mapping type for type Oid: %d", postgres_type);
            return arrow::Type::NA;
    }
}

/*
 * bytes_to_postgres_type
 *      Convert min/max values from column statistics stored in parquet file as
 *      plain bytes to postgres Datum.
 */
Datum
bytes_to_postgres_type(const char *bytes, Size len, const arrow::DataType *arrow_type)
{
    switch(arrow_type->id())
    {
        case arrow::Type::BOOL:
            return BoolGetDatum(*(bool *) bytes);
        case arrow::Type::INT8:
            return Int16GetDatum(*(int8 *) bytes);
        case arrow::Type::INT16:
            return Int16GetDatum(*(int16 *) bytes);
        case arrow::Type::INT32:
            return Int32GetDatum(*(int32 *) bytes);
        case arrow::Type::INT64:
            return Int64GetDatum(*(int64 *) bytes);
        case arrow::Type::FLOAT:
            return Float4GetDatum(*(float *) bytes);
        case arrow::Type::DOUBLE:
            return Float8GetDatum(*(double *) bytes);
        case arrow::Type::STRING:
            return CStringGetTextDatum(bytes);
        case arrow::Type::BINARY:
            return PointerGetDatum(cstring_to_text_with_len(bytes, len));
        case arrow::Type::TIMESTAMP:
            {
                TimestampTz ts;
                auto tstype = (arrow::TimestampType *) arrow_type;

                to_postgres_timestamp(tstype, *(int64 *) bytes, ts);
                return TimestampGetDatum(ts);
            }
            break;
        case arrow::Type::DATE32:
            return DateADTGetDatum(*(int32 *) bytes +
                                (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
        default:
            return PointerGetDatum(NULL);
    }
}

/*
 * XXX Currently only supports ascii strings
 */
char *
tolowercase(const char *input, char *output)
{
    int i = 0;

    Assert(strlen(input) < NAMEDATALEN - 1);

    do
    {
        output[i] = tolower(input[i]);
    }
    while (input[i++]);

    return output;
}

arrow::Type::type
get_arrow_list_elem_type(arrow::DataType *type)
{
    auto children = type->fields();

    Assert(children.size() == 1);
    return children[0]->type()->id();
}

void datum_to_jsonb(Datum value, Oid typoid, bool isnull, FmgrInfo *outfunc,
                    JsonbParseState *parseState, JsonbIteratorToken seq)
{
    JsonbValue  jb;
    bool        iskey = false;

    if (seq == WJB_KEY)
        iskey = true;

    if (isnull)
    {
        Assert(!iskey);
        jb.type = jbvNull;
        pushJsonbValue(&parseState, seq, &jb);
        return;
    }
    switch (typoid)
    {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        {
            /* If key is integer, we must convert it to text, not numeric */
            if (iskey) {
                char    *strval;

                strval = DatumGetCString(FunctionCall1(outfunc, value));

                jb.type = jbvString;
                jb.val.string.len = strlen(strval);
                jb.val.string.val = strval;
            }
            else {
                Datum numeric;

                switch (typoid)
                {
                    case INT2OID:
                    case INT4OID:
                        numeric = DirectFunctionCall1(int4_numeric, value);
                        break;
                    case INT8OID:
                        numeric = DirectFunctionCall1(int8_numeric, value);
                        break;
                    case FLOAT4OID:
                        numeric = DirectFunctionCall1(float4_numeric, value);
                        break;
                    case FLOAT8OID:
                        numeric = DirectFunctionCall1(float8_numeric, value);
                        break;
                    default:
                        Assert(false && "should never happen");
                }

                jb.type = jbvNumeric;
                jb.val.numeric = DatumGetNumeric(numeric);
            }
            break;
        }
        case TEXTOID:
        {
            char *str = TextDatumGetCString(value);

            jb.type = jbvString;
            jb.val.string.len = strlen(str);
            jb.val.string.val = str;
            break;
        }
        case JSONBOID:
        {
            Jsonb   *jsonb = DatumGetJsonbP(value);

            jb.type = jbvBinary;
            jb.val.binary.data = &jsonb->root;
            jb.val.binary.len = VARSIZE(jsonb) - VARHDRSZ;
            break;
        }
        default:
        {
            char    *strval;

            strval = DatumGetCString(FunctionCall1(outfunc, value));

            jb.type = jbvString;
            jb.val.string.len = strlen(strval);
            jb.val.string.val = strval;
        }
    }

    pushJsonbValue(&parseState, seq, &jb);
}

/*
 * Push jsonb key as string value.
 */
void push_jsonb_string_key(JsonbParseState *parseState, char *key_name)
{
    JsonbValue  jb;

    jb.type = jbvString;
    jb.val.string.len = strlen(key_name);
    jb.val.string.val = key_name;

    pushJsonbValue(&parseState, WJB_KEY, &jb);
}

/**
 * @brief get parquet timestamp value from given postgres timestamp
 *
 * @param tsunit parquet timestamp precision
 * @param ts postgres timestamp
 * @return int64 parquet timestamp
 */
int64
to_parquet_timestamp(arrow::TimeUnit::type tsunit, Timestamp ts)
{
    int64       parquet_ts;

    /*
     * Postgres timestamp starts with 2000-01-01 while unix timestamp (which
     * Parquet is using) starts with 1970-01-01. So, we need to do the
     * calculations below.
     */
    parquet_ts = (int64) (ts + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) * USECS_PER_SEC);

    switch (tsunit)
    {
        case arrow::TimeUnit::SECOND:
            parquet_ts /= USECS_PER_SEC;
            break;
        case arrow::TimeUnit::MILLI:
            parquet_ts /= 1000;
            break;
        case arrow::TimeUnit::MICRO:
            /* parquet_ts already in microsecond precision */
            break;
        case arrow::TimeUnit::NANO:
            parquet_ts *= 1000;
            break;
        default:
            elog(ERROR, "parquet_s3_fdw: Timestamp of unknown precision: %d", tsunit);
            break;
    }

    return parquet_ts;
}

/**
 * @brief return parquet date32 value from given postgres dateADT value
 *
 * @param date postgres DateADT
 * @return int32 parquet date32
 */
int32
to_parquet_date32(DateADT date)
{
    /*
     * Postgres date starts with 2000-01-01 while unix date (which
     * Parquet is using) starts with 1970-01-01. So we need to do
     * simple calculations here.
     */
    return date - (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
}

/**
 * @brief return value of JsonbValue in Datum
 *
 * @param jbv Jsonb value
 * @return Datum
 */
static Datum
jsonbValue_to_Datum(JsonbValue *jbv)
{
    switch (jbv->type)
    {
        case jbvBinary:
        case jbvArray:
        case jbvObject:
        {
            /* return jsonb datum */
            if (JsonContainerIsArray(jbv->val.binary.data) || JsonContainerIsObject(jbv->val.binary.data))
            {
                return JsonbPGetDatum(JsonbValueToJsonb(jbv));
            }
            else
                elog(ERROR, "parquet_s3_fdw:  unexpected jsonb value type: %d", jbv->type);
            break;
        }
        case jbvNumeric:
        {
            Numeric value = (Numeric) palloc0(VARSIZE(jbv->val.numeric));
            memcpy(value, jbv->val.numeric, VARSIZE(jbv->val.numeric));
            return NumericGetDatum(value);
        }
        case jbvString:
        {
            int64 bytea_len = jbv->val.string.len + VARHDRSZ;
            bytea *value = (bytea *) palloc0(bytea_len);
            SET_VARSIZE(value, bytea_len);
            memcpy(VARDATA(value), jbv->val.string.val, jbv->val.string.len);
            return PointerGetDatum(value);
        }
        case jbvBool:
        {
            bool value = jbv->val.boolean;
            return BoolGetDatum(value);
        }
        case jbvNull:
            return (Datum)0;
        default:
            elog(ERROR, "parquet_s3_fdw: unexpected jsonb value type: %d", jbv->type);
            return (Datum)0;
    }
}

/**
 * @brief  parse a jsonb to keys list and values list
 *
 * @param[in] jsonb given jsonb value
 * @param[out] keys parsed keys list
 * @param[out] values parsed values list
 * @param[out] value_types parsed values type list
 * @param[out] value_isnulls parsed keys value is null list
 * @param[out] data_len parsed keys/value length
 */
void
parquet_parse_jsonb(JsonbContainer *jsonb, Datum **keys, Datum **values, jbvType **value_types, bool **value_isnulls, size_t *data_len)
{
    JsonbValue	v;
    JsonbIterator *it;
    JsonbIteratorToken r;

    *data_len = JsonContainerSize(jsonb);
    if (data_len == 0)
        return;

    *keys = (Datum *) palloc0(sizeof(Datum) * (*data_len));
    *values = (Datum *) palloc0(sizeof(Datum) * (*data_len));
    *value_types = (jbvType *) palloc0(sizeof(jbvType) * (*data_len));
    *value_isnulls = (bool *) palloc0(sizeof(bool) * (*data_len));

    it = JsonbIteratorInit(jsonb);
    r = JsonbIteratorNext(&it, &v, true);

    switch (r)
    {
        case WJB_BEGIN_OBJECT:
        {
            size_t key_idx = 0;
            while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
            {
                if (r == WJB_KEY)
                {
                    /* json key in v, json value in val */
                    JsonbValue	val;

                    if (v.type == jbvNull)
                        elog(ERROR, "jsonb key must be not NULL.");

                    /* json key always in string value */
                    (*keys)[key_idx] = jsonbValue_to_Datum(&v);

                    if (JsonbIteratorNext(&it, &val, true) == WJB_VALUE)
                    {
                        /* get value */
                        (*value_types)[key_idx] = val.type;

                        if (val.type == jbvNull)
                            (*value_isnulls)[key_idx] = true;
                        else
                        {
                            (*value_isnulls)[key_idx] = false;
                            (*values)[key_idx] = jsonbValue_to_Datum(&val);
                        }

                    }
                    else
                    {
                        elog(ERROR, "parquet_s3_fdw: unexpected jsonb object token: %d", r);
                    }
                    key_idx++;
                }
            }
            break;
        }
        case WJB_BEGIN_ARRAY:
        {
            size_t key_idx = 0;
            while((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
            {
                if (r == WJB_ELEM)
                {
                    if (v.type == jbvNull)
                        (*value_isnulls)[key_idx] = true;
                    else
                        (*value_isnulls)[key_idx] = false;
                    (*value_types)[key_idx] = v.type;
                    (*values)[key_idx] = jsonbValue_to_Datum(&v);
                }
                else if (r != WJB_END_ARRAY)
                {
                    elog(ERROR, "parquet_s3_fdw: unexpected jsonb array token: %d", r);
                }
                key_idx++;
            }
            break;
        }
        default:
            elog(ERROR, "parquet_s3_fdw: unexpected jsonb token: %d", r);
    }
}

/**
 * @brief return mapping arrow type of given jbvType
 *
 * @param jbv_type jsonb type
 * @return Oid postgres type oid
 */
Oid
jbvType_to_postgres_type(jbvType jbv_type)
{
    switch (jbv_type)
    {
        case jbvNumeric:
            return NUMERICOID;
        case jbvBool:
            return BOOLOID;
        case jbvString:
            return TEXTOID;
        case jbvArray:
        case jbvObject:
        case jbvBinary:
            return JSONBOID;
        default:
            return InvalidOid;
    }
}

/**
 * @brief check whether given directory existed or not
 *
 * @param path directory path
 * @return true if directory path existed
 */
bool
is_dir_exist(const std::string& path)
{
    struct stat info;
    if (stat(path.c_str(), &info) != 0)
    {
        return false;
    }
    return (info.st_mode & S_IFDIR) != 0;
}

/**
 * @brief check whether given file existed or not
 *
 * @param path file path
 * @return true true if file path existed
 */
bool
is_file_exist(const std::string& path)
{
    struct stat info;
    if (stat(path.c_str(), &info) != 0)
    {
        return false;
    }
    return ((info.st_mode & S_IFMT) == S_IFREG);
}

/**
 * @brief create a directory
 *
 * @param path directory path
 * @return true create successfully
 */
bool
make_path(const std::string& path)
{
    mode_t mode = 0755;
    int ret = mkdir(path.c_str(), mode);

    if (ret == 0)
        return true;

    switch (errno)
    {
        case ENOENT:
        {
            /* parent didn't exist, try to create it */
            size_t pos = path.find_last_of('/');
            if (pos == std::string::npos)
                return false;
            if (!make_path( path.substr(0, pos) ))
                return false;

            /* now, try to create again */
            return 0 == mkdir(path.c_str(), mode);
        }

        case EEXIST:
            /* done! */
            return is_dir_exist(path);
        default:
            return false;
    }
}

/**
 * @brief remove recursively directoty if empty
 *
 * @param path
 * @return int
 */
int
remove_directory_if_empty(const char *path)
{
    DIR        *d = opendir(path);
    size_t      path_len = strlen(path);
    int         ret = -1;
    bool        is_empty = true;

    if (d)
    {
        struct dirent *p;
        ret = 0;

        while (!ret && (p = readdir(d)))
        {
            int     ret2 = -1;
            char   *buf;
            size_t  len;

            /* Skip the names "." and ".." as we don't want to recurse on them. */
            if (strcmp(p->d_name, ".") == 0 || strcmp(p->d_name, "..") == 0)
                continue;

            len = path_len + strlen(p->d_name) + 2;
            buf = (char *) palloc0(sizeof(char) * len);

            if (buf)
            {
                struct stat statbuf;

                pg_snprintf(buf, len, "%s/%s", path, p->d_name);

                if (!stat(buf, &statbuf))
                {
                    if (S_ISDIR(statbuf.st_mode))
                        ret2 = remove_directory_if_empty(buf);
                    else
                        is_empty = false;
                }
                pfree(buf);
            }
            ret = ret2;
        }
        closedir(d);
    }

    /* remove current dir if it not empty */
    if (!ret && is_empty)
        ret = rmdir(path);

    return ret;
}
