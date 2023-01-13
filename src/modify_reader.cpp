/*-------------------------------------------------------------------------
 *
 * modify_reader.cpp
 *      FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2022, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *      contrib/parquet_s3_fdw/src/modify_reader.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "modify_reader.hpp"
#include "common.hpp"

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/array.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

extern "C"
{
#include "postgres.h"
#include "access/sysattr.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "catalog/pg_type_d.h"
#include "pgstat.h"
}

#define TEMPORARY_DIR "/tmp/parquet_s3_fdw_temp"

/**
 * @brief Construct a new Modify Parquet Reader:: Modify Parquet Reader object
 *
 * @param filename target file name
 * @param cxt reader memory context
 * @param schema target file schema
 * @param is_new_file where target file is new
 * @param reader_id reder id
 */
ModifyParquetReader::ModifyParquetReader(const char* filename,
                                         MemoryContext cxt,
                                         std::shared_ptr<arrow::Schema> schema,
                                         bool is_new_file,
                                         int reader_id)
: ParquetReader(cxt)
{
    this->reader_entry = NULL;
    this->filename = filename;
    this->reader_id = reader_id;
    this->coordinator = NULL;
    this->initialized = false;
    this->schemaless = false;
    this->modified = false;
    this->data_is_cached = false;
    this->file_schema = schema;
    this->is_new_file = is_new_file;
}

/**
 * @brief Destroy the Modify Parquet Reader:: Modify Parquet Reader object
 */
ModifyParquetReader::~ModifyParquetReader()
{
    if (this->reader_entry && this->reader_entry->file_reader && this->reader)
        this->reader_entry->file_reader->reader = std::move(this->reader);
}

/**
 * @brief Create a modify parquet reader object
 *
 * @param filename target file name
 * @param cxt reader memory context
 * @param schema target file schema
 * @param is_new_file whether target file is new
 * @param reader_id reder id
 * @return ModifyParquetReader* modify parquet reader object
 */
ModifyParquetReader *create_modify_parquet_reader(const char *filename,
                                                  MemoryContext cxt,
                                                  std::shared_ptr<arrow::Schema> schema,
                                                  bool is_new_file,
                                                  int reader_id)
{
    return new ModifyParquetReader(filename, cxt, schema, is_new_file, reader_id);
}

/**
 * @brief init cast from postgres type to mapped parquet type
 *
 * @param typinfo column/attribute type information
 * @param attname column/attribute name
 */
void
ModifyParquetReader::initialize_postgres_to_parquet_cast(TypeInfo &typinfo, const char *attname)
{
    MemoryContext ccxt = CurrentMemoryContext;
    Oid         src_oid = typinfo.pg.oid;
    Oid         dst_oid = to_postgres_type(typinfo.arrow.type_id);
    bool        error = false;
    char        errstr[ERROR_STR_LEN];

    /* cast is needed for JSONB e.g. text */
    if (typinfo.arrow.type_id == arrow::Type::MAP)
        dst_oid = JSONBOID;

    if (typinfo.arrow.type_id == arrow::Type::LIST)
    {
        if (this->schemaless == true)
        {
            dst_oid = JSONBOID;
        }
        else
        {
            /* LIST must be created as postgres array to execute select => cast is not needed */
            typinfo.castfunc = nullptr;
            return;
        }
    }

    if (!OidIsValid(dst_oid))
    {
        elog(ERROR, "parquet_s3_fdw: failed to initialize cast function for column '%s'", attname);
    }

    PG_TRY();
    {
        if (IsBinaryCoercible(src_oid, dst_oid))
        {
            typinfo.castfunc = nullptr;
        }
        else
        {
            CoercionPathType ct;
            Oid     funcid;

            ct = find_coercion_pathway(dst_oid, src_oid,
                                       COERCION_EXPLICIT,
                                       &funcid);
            switch (ct)
            {
                case COERCION_PATH_FUNC:
                    {
                        MemoryContext   oldctx;

                        oldctx = MemoryContextSwitchTo(CurTransactionContext);
                        typinfo.castfunc = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
                        fmgr_info(funcid, typinfo.castfunc);
                        typinfo.need_cast = true;
                        MemoryContextSwitchTo(oldctx);
                        break;
                    }
                case COERCION_PATH_RELABELTYPE:
                    /* Cast is not needed */
                    typinfo.castfunc = nullptr;
                    break;
                case COERCION_PATH_COERCEVIAIO:
                    /* Cast via IO */
                    typinfo.outfunc = find_outfunc(src_oid);
                    typinfo.infunc = find_infunc(dst_oid);
                    typinfo.need_cast = true;
                    break;
                default:
                    elog(ERROR, "parquet_s3_fdw: coercion pathway from '%s' to '%s' not found",
                         format_type_be(src_oid), format_type_be(dst_oid));
            }
        }
    }
    PG_CATCH();
    {
        ErrorData *errdata;

        ccxt = MemoryContextSwitchTo(ccxt);
        error = true;
        errdata = CopyErrorData();
        FlushErrorState();

        strncpy(errstr, errdata->message, ERROR_STR_LEN - 1);
        FreeErrorData(errdata);
        MemoryContextSwitchTo(ccxt);
    }
    PG_END_TRY();
    if (error)
        throw Error("parquet_s3_fdw: failed to initialize cast function for column '%s' (%s)",
                    attname, errstr);
}

/**
 * @brief open a parquet file on s3 storage
 *
 * @param dirname directory path
 * @param s3_client aws client for s3 storage
 */
void
ModifyParquetReader::open(const char *dirname, Aws::S3::S3Client *s3_client)
{
    arrow::Status   status;
    char           *dname;
    char           *fname;

    parquetSplitS3Path(dirname, filename.c_str(), &dname, &fname);
    this->reader_entry = parquetGetFileReader(s3_client, dname, fname);
    elog(DEBUG1, "parquet_s3_fdw: open Parquet file on S3. %s%s", dname, fname);

    pfree(dname);
    pfree(fname);

    this->reader = std::move(this->reader_entry->file_reader->reader);
    /* Enable parallel columns decoding/decompression if needed */
    this->reader->set_use_threads(this->use_threads && parquet_fdw_use_threads);
}

/**
 * @brief open a parquet file from local storage
 */
void
ModifyParquetReader::open()
{
    arrow::Status   status;
    std::unique_ptr<parquet::arrow::FileReader> reader;

    status = parquet::arrow::FileReader::Make(
                    arrow::default_memory_pool(),
                    parquet::ParquetFileReader::OpenFile(filename, use_mmap),
                    &reader);
    if (!status.ok())
        throw Error("parquet_s3_fdw: failed to open Parquet file %s",
                                status.message().c_str());
    this->reader = std::move(reader);
    /* Enable parallel columns decoding/decompression if needed */
    this->reader->set_use_threads(this->use_threads && parquet_fdw_use_threads);
}

void
ModifyParquetReader::close()
{
    throw std::runtime_error("parquet_s3_fdw: ModifyParquetReader::close() not implemented");
}

/**
 * @brief set sorted column names list of target file
 *
 * @param sorted_cols sorted column name list
 */
void
ModifyParquetReader::set_sorted_col_list(std::set<std::string> sorted_cols)
{
    this->sorted_cols = sorted_cols;
}

/**
 * @brief set option for apache reader
 *
 * @param use_threads whether memory map operations will be used
 *                    instead of file read operations
 * @param use_mmap enables Apache Arrow's parallel columns
 *                 decoding/decompression
 */
void
ModifyParquetReader::set_options(bool use_threads, bool use_mmap)
{
    this->use_threads = use_threads;
    this->use_mmap = use_mmap;
}

/**
 * @brief set schemaless mode flag
 *
 * @param schemaless whether schemaless mode enable
 */
void
ModifyParquetReader::set_schemaless(bool schemaless)
{
    this->schemaless = schemaless;
}

/**
 * @brief set key column names list for target file
 *
 * @param keycol_names key column names list
 */
void
ModifyParquetReader::set_keycol_names(std::set<std::string> keycol_names)
{
    this->keycol_names = keycol_names;
}

/**
 * @brief compare given name and target file name
 *
 * @param filename given name
 * @return true if given name match with target file name.
 * @return false otherwise.
 */
bool
ModifyParquetReader::compare_filename(char *filename)
{
    if (filename == NULL)
        return false;
    else
        return strcmp(this->filename.c_str(), filename) == 0;
}

/**
 * @brief get target file schema from cached one or from reader
 *
 * @return std::shared_ptr<arrow::Schema> target file schema
 */
std::shared_ptr<arrow::Schema>
ModifyParquetReader::get_file_schema()
{
    if (this->file_schema == nullptr)
    {
        try
        {
            std::shared_ptr<arrow::Table>   tmptable;
            PARQUET_THROW_NOT_OK(reader->ReadTable(&tmptable));
            this->file_schema = tmptable->schema();
        }
        catch (const std::exception& e)
        {
            elog(ERROR, "parquet_s3_fdw: %s", e.what());
        }
    }

    return this->file_schema;
}

/**
 * @brief check all given not null arrtributes are existed on target parquet file.
 *
 * @param attrs
 * @param is_nulls
 * @return true if all not null columns are exist on target parquet file.
 */
bool
ModifyParquetReader::schema_check(std::vector<int> attrs, std::vector<bool> is_nulls)
{
    for (size_t i = 0; i < attrs.size(); i++)
    {
        if (this->map[attrs[i]] == -1 && is_nulls[i] == false)
            return false;
    }
    return true;
}

/**
 * @brief create arrow array builder object corresponding column type
 *
 * @param typeinfo column type information
 * @return std::shared_ptr<arrow::ArrayBuilder> array builder object
 */
std::shared_ptr<arrow::ArrayBuilder>
ModifyParquetReader::typeid_get_builder(const TypeInfo& typeinfo)
{
    switch(typeinfo.arrow.type_id)
    {
        case arrow::Type::BOOL:
            return std::make_shared<arrow::BooleanBuilder>();
        case arrow::Type::INT8:
            return std::make_shared<arrow::Int8Builder>();
        case arrow::Type::INT16:
            return std::make_shared<arrow::Int16Builder>();
        case arrow::Type::INT32:
            return std::make_shared<arrow::Int32Builder>();
        case arrow::Type::INT64:
            return std::make_shared<arrow::Int64Builder>();
        case arrow::Type::FLOAT:
            return std::make_shared<arrow::FloatBuilder>();
        case arrow::Type::DOUBLE:
            return std::make_shared<arrow::DoubleBuilder>();
        case arrow::Type::DATE32:
            return std::make_shared<arrow::Date32Builder>();
        case arrow::Type::TIMESTAMP:
            return std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(typeinfo.arrow.time_precision), arrow::default_memory_pool());
        case arrow::Type::STRING:
            return std::make_shared<arrow::StringBuilder>();
        default:
            elog(ERROR, "parquet_s3_fdw: can not make array builder for arrow type '%s'", typeinfo.arrow.type_name.c_str());
    }
}

/**
 * @brief get C corresponding type size of arrow type
 *
 * @param type_id arrow type id
 * @return size_t C corresponding type size
 */
size_t
ModifyParquetReader::get_arrow_type_size(arrow::Type::type type_id)
{
    size_t sz;

    switch(type_id)
    {
        case arrow::Type::BOOL:
            sz = sizeof(bool);
            break;
        case arrow::Type::INT8:
            sz = sizeof(int8);
            break;
        case arrow::Type::INT16:
            sz = sizeof(int16);
            break;
        case arrow::Type::INT32:
            sz = sizeof(int32);
            break;
        case arrow::Type::INT64:
            sz = sizeof(int64);
            break;
        case arrow::Type::FLOAT:
            sz = sizeof(float);
            break;
        case arrow::Type::DOUBLE:
            sz = sizeof(double);
            break;
        case arrow::Type::DATE32:
            sz = sizeof(int32);
            break;
        case arrow::Type::TIMESTAMP:
        {
            sz = sizeof(int64);
            break;
        }
        case arrow::Type::LIST:
        {
            sz = sizeof(parquet_list_value);
            break;
        }
        case arrow::Type::MAP:
        {
            sz = sizeof(parquet_map_value);
            break;
        }
        default:
            /* case arrow::Type::STRING: */
            /* case arrow::Type::BINARY: */
            sz = sizeof(void *);
    }

    return sz;
}

/**
 * @brief read primitive type value from arrow array and convert to `void *` value
 *
 * @param array arrow array
 * @param type_info column type information
 * @param i value index
 * @return void* value in `void *` pointer
 */
void *
ModifyParquetReader::read_primitive_type_raw(arrow::Array *array,
                        TypeInfo *type_info,
                        int64_t i)
{
    void *value = palloc0(get_arrow_type_size(type_info->arrow.type_id));

    /* Get datum depending on the column type */
    switch (type_info->arrow.type_id)
    {
        case arrow::Type::BOOL:
        {
            arrow::BooleanArray *boolarray = (arrow::BooleanArray *) array;
            *((bool *)value) = boolarray->Value(i);
            break;
        }
        case arrow::Type::INT8:
        {
            arrow::Int8Array *intarray = (arrow::Int8Array *) array;
            *((int8 *)value) = intarray->Value(i);
            break;
        }
        case arrow::Type::INT16:
        {
            arrow::Int16Array *intarray = (arrow::Int16Array *) array;
            *((int16 *)value) = intarray->Value(i);
            break;
        }
        case arrow::Type::INT32:
        {
            arrow::Int32Array *intarray = (arrow::Int32Array *) array;
            *((int32 *)value) = intarray->Value(i);
            break;
        }
        case arrow::Type::INT64:
        {
            arrow::Int64Array *intarray = (arrow::Int64Array *) array;
            *((int64 *)value) = intarray->Value(i);
            break;
        }
        case arrow::Type::FLOAT:
        {
            arrow::FloatArray *farray = (arrow::FloatArray *) array;
            *((float *)value) = farray->Value(i);
            break;
        }
        case arrow::Type::DOUBLE:
        {
            arrow::DoubleArray *darray = (arrow::DoubleArray *) array;
            *((double *)value) = darray->Value(i);
            break;
        }
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        {
            arrow::BinaryArray *binarray = (arrow::BinaryArray *) array;

            int32_t vallen = 0;
            const char *value_str = reinterpret_cast<const char*>(binarray->GetValue(i, &vallen));

            /* Build bytea to keep length of string */
            int64 bytea_len = vallen + VARHDRSZ;
            bytea *b = (bytea *) palloc0(bytea_len);
            SET_VARSIZE(b, bytea_len);
            memcpy(VARDATA(b), value_str, vallen);

            *((bytea **)value) = b;
            break;
        }
        case arrow::Type::TIMESTAMP:
        {
            arrow::TimestampArray *tsarray = (arrow::TimestampArray *) array;
            *((int64 *)value) = tsarray->Value(i);
            break;
        }
        case arrow::Type::DATE32:
        {
            arrow::Date32Array *darray = (arrow::Date32Array *) array;
            *((int32 *)value)= darray->Value(i);
            break;
        }
        default:
            elog(ERROR, "parquet_s3_fdw: read_primitive_type_raw: unsupported column type: %i",
                        type_info->arrow.type_id);
    }
    return value;
}

/**
 * @brief read a column from arrow table
 *
 * @param[in] table arrow table
 * @param[in] col column index
 * @param[in] type_info column type information
 * @param[out] data column data in `void **` pointer
 * @param[out] is_null column value null list
 */
void
ModifyParquetReader::read_column(std::shared_ptr<arrow::Table> table,
                                int col, TypeInfo *type_info,
                                void ***data, bool **is_null)
{
    std::shared_ptr<arrow::ChunkedArray> column = table->column(col); /* Get column by index */
    int     row = 0;
    size_t  num_rows = table->num_rows();

    *data = (void **) palloc0(sizeof(void *) * num_rows);
    *is_null = (bool *) palloc0(sizeof(bool) * num_rows);

    for (int i = 0; i < column->num_chunks(); ++i)
    {
        arrow::Array *array = column->chunk(i).get();

        for (int j = 0; j < array->length(); ++j)
        {
            if (array->IsNull(j))
            {
                (*is_null)[row++] = true;
                continue;
            }
            switch (type_info->arrow.type_id)
            {
                case arrow::Type::LIST:
                {
                    arrow::ListArray *listArray = (arrow::ListArray *) array;
                    parquet_list_value *list_value = (parquet_list_value *) palloc0(sizeof(parquet_list_value));
                    size_t      list_len;

                    /* Get value in each row of column with type LIST, these values in row is Array*/
                    std::shared_ptr<arrow::Array> arrayValue = listArray->values()->Slice(listArray->value_offset(row),
                                                                                            listArray->value_length(row));

                    /* get length of record in row */
                    list_len = arrayValue->length();

                    list_value->listValues = (void**) palloc0(sizeof(void*) * list_len);
                    list_value->listIsNULL = (bool*) palloc0(sizeof(bool) * list_len);
                    memset(list_value->listIsNULL, false, list_len);
                    list_value->len = list_len;

                    /* Loop through the length of the record in one row to get each value */
                    for (size_t i = 0; i < list_len; i++)
                    {
                        if (arrayValue->IsNull(i))
                        {
                            list_value->listIsNULL[i] = true;
                        }
                        else
                        {
                            list_value->listValues[i] = read_primitive_type_raw(arrayValue.get(), &type_info->children[0], i);
                        }
                    }
                    (*data)[row] = (void *) list_value;
                    break;
                }
                case arrow::Type::MAP:
                {
                    arrow::MapArray* maparray = (arrow::MapArray*) array;
                    parquet_map_value *map_value = (parquet_map_value *)palloc0(sizeof(parquet_map_value));

                    /* map is treated as 2 void * elem array */
                    auto keys = maparray->keys()->Slice(maparray->value_offset(j),
                                                        maparray->value_length(j));
                    auto items = maparray->items()->Slice(maparray->value_offset(j),
                                                        maparray->value_length(j));

                    Assert(keys->length() == items->length());
                    map_value->len = keys->length();
                    map_value->keys = (void **)palloc0(sizeof(void *) * keys->length());
                    map_value->key_nulls = (bool *)palloc0(sizeof(bool) * keys->length());
                    memset(map_value->key_nulls, false, keys->length());
                    map_value->items = (void **)palloc0(sizeof(void *) * items->length());
                    map_value->item_nulls = (bool *)palloc0(sizeof(bool) * items->length());
                    memset(map_value->item_nulls, false, items->length());

                    for (int64_t element_idx = 0; element_idx < keys->length(); element_idx++)
                    {
                        /* read key */
                        if (keys->IsNull(element_idx))
                            map_value->key_nulls[element_idx] = true;
                        else
                            map_value->keys[element_idx] = read_primitive_type_raw(keys.get(), &type_info->children[0], element_idx);
                        /* read value */
                        if (items->IsNull(element_idx))
                            map_value->item_nulls[element_idx] = true;
                        else
                            map_value->items[element_idx] = read_primitive_type_raw(items.get(), &type_info->children[1], element_idx);
                    }

                    (*data)[row] = (void *)map_value;
                    break;
                }
                default:
                    (*data)[row] = read_primitive_type_raw(array, type_info, j);
                    break;
            }

            (*is_null)[row] = false;
            row++;
        }
    }
}

/**
 * @brief create a temporary cache for the new file
 */
void
ModifyParquetReader::create_new_file_temp_cache()
{
    auto fields = this->file_schema->fields();

    Assert(this->data_is_cached == false);
    Assert(this->file_schema != nullptr);

    this->data_is_cached = true;
    this->cache_data = (parquet_file_info *)palloc0(sizeof(parquet_file_info));
    this->cache_data->column_num = fields.size();
    this->cache_data->columnsValue = (void ***)palloc0(sizeof(void **) * this->cache_data->column_num);
    this->cache_data->columnsNulls = (bool **)palloc0(sizeof(bool *) * this->cache_data->column_num);
    this->cache_data->columnNames.insert(this->cache_data->columnNames.begin(), this->column_names.begin(), this->column_names.end());
    this->cache_data->row_num = 0;
}

/**
 * @brief cache parquet file data on C structure, which can be modified.
 */
void
ModifyParquetReader::cache_parquet_file_data()
{
    std::shared_ptr<arrow::Table>   tmptable;
    parquet::ArrowReaderProperties  props;
    parquet::arrow::SchemaManifest  manifest;
    auto                            schema = this->reader->parquet_reader()->metadata()->schema();

    if (!parquet::arrow::SchemaManifest::Make(schema, nullptr, props, &manifest).ok())
        throw std::runtime_error("parquet_s3_fdw: error creating arrow schema");

    this->cache_data = (parquet_file_info *)palloc0(sizeof(parquet_file_info));

    try
    {
        auto meta = reader->parquet_reader()->metadata();
        if (meta->num_rows() > 0)
            PARQUET_THROW_NOT_OK(reader->ReadTable(&tmptable));

        this->cache_data->column_num = manifest.schema_fields.size();
        this->cache_data->columnsValue = (void ***)palloc0(sizeof(void **) * this->cache_data->column_num);
        this->cache_data->columnsNulls = (bool **)palloc0(sizeof(bool *) * this->cache_data->column_num);
        this->cache_data->columnNames.resize(this->cache_data->column_num);
        this->cache_data->row_num = meta->num_rows();

        if (this->cache_data->row_num > 0)
        {
            for (size_t i = 0; i < this->cache_data->column_num; i++)
            {
                /* Get column name */
                auto field = manifest.schema_fields[i].field;
                auto field_name = field->name();
                char arrow_colname[NAMEDATALEN];

                if (field_name.length() > NAMEDATALEN - 1)
                    throw Error("parquet column name '%s' is too long (max: %d)",
                                field_name.c_str(), NAMEDATALEN - 1);
                tolowercase(field_name.c_str(), arrow_colname);

                this->cache_data->columnNames[i] = std::string(arrow_colname);

                /* Get values in columns */
                read_column(tmptable, i, &this->types[i],
                            &this->cache_data->columnsValue[i], &this->cache_data->columnsNulls[i]);
            }
        }
    }
    catch (const std::exception& e)
    {
       elog(ERROR, "parquet_s3_fdw: %s", e.what());
    }

    this->data_is_cached = true;
}

/**
 * @brief create void* pointer from postgres Datum coresponding with column type
 *
 * @param type_info column type information
 * @param value postgres value in Datum
 * @param[out] res returned value in void * pointer
 */
void
ModifyParquetReader::postgres_val_to_voidp(const TypeInfo &type_info, Datum value, void **res)
{
    if (res == NULL)
        return;

    if (*res == NULL)
        *res = palloc0(sizeof(void *));

    switch(type_info.arrow.type_id)
    {
        case arrow::Type::BOOL:
        {
            *((bool *)*res) = DatumGetBool(value);
            break;
        }
        case arrow::Type::INT8:
        {
            *((int8 *)*res) = DatumGetChar(value);
            break;
        }
        case arrow::Type::INT16:
        {
            *((int16 *)*res) = DatumGetInt16(value);
            break;
        }
        case arrow::Type::INT32:
        {
            *((int32 *)*res) = DatumGetInt32(value);
            break;
        }
        case arrow::Type::INT64:
        {
            *((int64 *)*res) = DatumGetInt64(value);
            break;
        }
        case arrow::Type::FLOAT:
        {
            *((float *)*res) = DatumGetFloat4(value);
            break;
        }
        case arrow::Type::DOUBLE:
        {
            *((double *)*res) = DatumGetFloat8(value);
            break;
        }
        case arrow::Type::DATE32:
        {
            *((int32 *)*res) = to_parquet_date32(DatumGetDateADT(value));
            break;
        }
        case arrow::Type::TIMESTAMP:
        {
            int64 parquet_timestamp = to_parquet_timestamp(type_info.arrow.time_precision, DatumGetTimestampTz(value));
            *((int64 *)*res) = parquet_timestamp;
            break;
        }
        case arrow::Type::BINARY:
        {
            *((bytea **)*res) = DatumGetByteaP(value);
            break;
        }
        case arrow::Type::STRING:
        {
            Oid			typoutput;
            bool		typIsVarlena;
            char	   *extval;
            int64       bytea_len;
            bytea      *byteaval;

            /* value type Oid is TEXTOID or has been cast to TEXTOID before */
            getTypeOutputInfo(TEXTOID, &typoutput, &typIsVarlena);
	        extval = OidOutputFunctionCall(typoutput, value);

            /* build bytea data from output string */
            bytea_len = strlen(extval) + VARHDRSZ;
            byteaval = (bytea *) palloc0(bytea_len);
            SET_VARSIZE(byteaval, bytea_len);
            memcpy(VARDATA(byteaval), extval, strlen(extval));

            *((bytea **)*res) = byteaval;
            break;
        }
        default:
        {
            elog(ERROR, "parquet_s3_fdw: does not support get data for arrow type '%s'", type_info.arrow.type_name.c_str());
        }
    }
}

/**
 * @brief create internal MAP data from given Jsonb value
 *
 * @param jb Jsonb value
 * @param column_type column type information
 * @return ModifyParquetReader::parquet_map_value internal MAP data
 */
ModifyParquetReader::parquet_map_value
ModifyParquetReader::Jsonb_to_MAP(Jsonb *jb, TypeInfo &column_type)
{
    parquet_map_value map_value;
    Datum      *keys;
    Datum      *values;
    jbvType    *value_jbv_types;
    bool       *value_isnulls;
    size_t      len;
    Oid         key_type = to_postgres_type(column_type.children[0].arrow.type_id);
    Oid         input_fn;
    Oid         input_param;

    if (!JsonContainerIsObject(&jb->root))
        elog(ERROR, "parquet_s3_fdw: only jsonb object is acceptable for MAP column.");

    parquet_parse_jsonb(&jb->root, &keys, &values, &value_jbv_types, &value_isnulls, &len);

    Assert(column_type.children.size() == 2);

    map_value.len = len;
    map_value.keys = (void **)palloc0(sizeof(void *) * len);
    map_value.key_nulls = (bool *)palloc0(sizeof(bool) * len);
    memset(map_value.key_nulls, false, sizeof(bool) * len);
    map_value.items = (void **)palloc0(sizeof(void *) * len);
    map_value.item_nulls = (bool *)palloc0(sizeof(bool) * len);
    memset(map_value.item_nulls, false, sizeof(bool) * len);

    /* key is alway in text and not null */
    getTypeInputInfo(key_type, &input_fn, &input_param);

    for (size_t key_idx = 0; key_idx < len; key_idx++)
    {
        bytea *bytea_val = DatumGetByteaP(keys[key_idx]);
        size_t str_len = VARSIZE(bytea_val) - VARHDRSZ;
        char *str = (char *)palloc0(sizeof(char) * (str_len + 1));
        memcpy(str, VARDATA(bytea_val), str_len);
        postgres_val_to_voidp(column_type.children[0],
                                OidInputFunctionCall(input_fn, str, input_param, 0),
                                &map_value.keys[key_idx]);
    }

    /* get value */
    for (size_t val_idx = 0; val_idx < len; val_idx++)
    {
        if (value_jbv_types[val_idx] == jbvNull)
        {
            map_value.items[val_idx] = NULL;
            map_value.item_nulls[val_idx] = true;
        }
        else
        {
            Oid     elem_type = jbvType_to_postgres_type(value_jbv_types[val_idx]);
            TypeInfo &elem = column_type.children[1];

            get_element_type_info(elem_type, "attname", elem);
            initialize_postgres_to_parquet_cast(elem, "attname");

            postgres_val_to_voidp(column_type.children[1],
                                    do_cast(values[val_idx], elem),
                                    &map_value.items[val_idx]);
        }
    }
    return map_value;
}

/**
 * @brief create internal LIST data from given Jsonb value
 *
 * @param jb Jsonb value
 * @param column_type column type information
 * @return ModifyParquetReader::parquet_list_value internal LIST data
 */
ModifyParquetReader::parquet_list_value
ModifyParquetReader::Jsonb_to_LIST(Jsonb *jb, TypeInfo &column_type)
{
    parquet_list_value list_value;
    Datum      *keys;
    Datum      *values;
    jbvType    *value_jbv_types;
    bool       *value_isnulls;
    size_t      len;
    Oid         val_type = to_postgres_type(column_type.children[0].arrow.type_id);
    Oid         input_fn;
    Oid         input_param;

    parquet_parse_jsonb(&jb->root, &keys, &values, &value_jbv_types, &value_isnulls, &len);

    list_value.listValues = (void**) palloc0(sizeof(void*) * len);
    list_value.listIsNULL = (bool*) palloc0(sizeof(bool) * len);
    list_value.len = len;

    /* convert value to parquet type: */
    getTypeInputInfo(val_type, &input_fn, &input_param);

    for (size_t val_idx = 0; val_idx < len; val_idx++)
    {

        if (value_jbv_types[val_idx] == jbvNull)
        {
            list_value.listValues[val_idx] = NULL;
            list_value.listIsNULL[val_idx] = true;
        }
        else
        {
            Oid     elem_type = jbvType_to_postgres_type(value_jbv_types[val_idx]);;
            TypeInfo &elem = column_type.children[0];

            get_element_type_info(elem_type, "attname", elem);
            initialize_postgres_to_parquet_cast(elem, "attname");

            postgres_val_to_voidp(column_type.children[0],
                                do_cast(values[val_idx], elem),
                                &list_value.listValues[val_idx]);
        }
    }
    return list_value;
}

/**
 * @brief create internal LIST data from given ArrayType value
 *
 * @param arr ArrayType value
 * @param column_type column type information
 * @return ModifyParquetReader::parquet_list_value internal LIST data
 */
ModifyParquetReader::parquet_list_value
ModifyParquetReader::Array_to_LIST(ArrayType *arr, TypeInfo &column_type)
{
    Datum      *values;
    bool       *nulls;
    int         num;
    int16       elmlen;
    bool        elmbyval;
    char        elmalign;

    get_typlenbyvalalign(ARR_ELEMTYPE(arr),
                            &elmlen, &elmbyval, &elmalign);

    deconstruct_array(arr, arr->elemtype, elmlen, elmbyval, elmalign, &values, &nulls, &num);

    parquet_list_value list_value;
    list_value.listValues = (void**) palloc0(sizeof(void*) * num);
    list_value.listIsNULL = (bool*) palloc0(sizeof(bool) * num);
    list_value.len = num;

    for (size_t i = 0; i < list_value.len; i++)
    {
        postgres_val_to_voidp(column_type.children[0],
                              do_cast(values[i], column_type.children[0]),
                              &list_value.listValues[i]);
    }

    return list_value;
}

/**
 * @brief insert value to a column at given index
 *
 * @param column target column
 * @param isnulls target column isnulls
 * @param column_len target column len
 * @param column_type target column type information
 * @param value inserted value
 * @param value_null whether inserted value is null
 * @param idx insert index
 */
void
ModifyParquetReader::add_value_to_column(void ***column, bool **isnulls, size_t column_len, TypeInfo &column_type, Datum value, bool value_null, size_t idx)
{
    size_t      type_size = get_arrow_type_size(column_type.arrow.type_id);
    size_t      new_col_len = column_len + 1;
    void      **new_col = (void **) palloc0(sizeof(void *) * new_col_len);
    bool       *new_isnulls = (bool *) palloc0(sizeof(bool) * new_col_len);
    void      **old_col = *column;
    bool       *old_isnulls = *isnulls;
    size_t      i;

    void *new_value = palloc0(type_size);

    /* copy to new array */
    if (column_len != 0)
    {
        memcpy(new_col, *column, (sizeof(void *)) * column_len);
        memcpy(new_isnulls, *isnulls, sizeof(bool) * column_len);
    }

    /* update null inform */
    i = new_col_len - 1;
    while (i > idx)
    {
        new_isnulls[i] = new_isnulls[i-1];
        new_col[i] = new_col[i-1];
        --i;
    }
    new_isnulls[i] = value_null;
    new_col[i] = new_value;

    if (!value_null)
    {
        switch(column_type.arrow.type_id)
        {
            case arrow::Type::MAP:
            {
                Jsonb *jb = DatumGetJsonbP(value);

                *((parquet_map_value *)new_value) = Jsonb_to_MAP(jb, column_type);
                break;
            }
            case arrow::Type::LIST:
            {
                if (this->schemaless)
                {
                    Jsonb *jb = DatumGetJsonbP(value);
                    *((parquet_list_value *) new_value) = Jsonb_to_LIST(jb, column_type);
                }
                else
                {
                    ArrayType  *arr = DatumGetArrayTypeP(value);
                    *((parquet_list_value *) new_value) = Array_to_LIST(arr, column_type);
                }
                break;
            }
            default:
                postgres_val_to_voidp(column_type, value, &new_value);
        }
    }

    *column = new_col;
    *isnulls = new_isnulls;
    if (old_col)
        pfree(old_col);
    if (old_isnulls)
        pfree(old_isnulls);
}

/**
 * @brief Build arrow array from given internal cache column (void **)
 *
 * @tparam builderTypeP array builder pointer
 * @tparam ValueType element value C type
 * @param builder array builder object
 * @param column_values source column data
 * @param isnulls source column data null list
 * @param len source column length
 * @param need_finished whether create arrow array or not,
 *                      just append value to builder object if need_finished is false
 * @return std::shared_ptr<arrow::Array> arrow array
 */
template <typename builderTypeP, typename ValueType> inline std::shared_ptr<arrow::Array>
build_array(arrow::ArrayBuilder *builder, void **column_values, bool *isnulls, size_t len, bool need_finished)
{
    std::shared_ptr<arrow::Array> array;
    builderTypeP    b = static_cast<builderTypeP>(builder);

    try
    {
        for (size_t idx = 0; idx < len; idx++)
        {
            if (isnulls[idx] == true)
            {
                PARQUET_THROW_NOT_OK(b->AppendNull());
            }
            else
            {
                const ValueType val = *((ValueType *)column_values[idx]);
                PARQUET_THROW_NOT_OK(b->Append(val));
            }
        }
        if (need_finished == true)
        {
            PARQUET_THROW_NOT_OK(b->Finish(&array));
            return array;
        }
    }
    catch (const std::exception& e)
    {
        elog(ERROR, "parquet_s3_fdw: %s", e.what());
    }
    return nullptr;
}

/**
 * @brief append a value in primitive type to an arrow array builder,
 *        create an arrow array if need_finished is true
 *
 * @param builder arrow array builder
 * @param type_info column type information
 * @param column_values source column data
 * @param isnulls source column null list
 * @param len source column length
 * @param need_finished whether build arrow array or not
 * @return std::shared_ptr<arrow::Array> created arrow array
 */
std::shared_ptr<arrow::Array>
ModifyParquetReader::builder_append_primitive_type(arrow::ArrayBuilder *builder, TypeInfo &type_info, void **column_values, bool *isnulls, size_t len, bool need_finished)
{
    switch (type_info.arrow.type_id)
    {
        case arrow::Type::BOOL:
            return build_array<arrow::BooleanBuilder *, bool>(builder, column_values, isnulls, len, need_finished);
        case arrow::Type::INT8:
            return build_array<arrow::Int8Builder *, int8>(builder, column_values, isnulls, len, need_finished);
        case arrow::Type::INT16:
            return build_array<arrow::Int16Builder *, int16>(builder, column_values, isnulls, len, need_finished);
        case arrow::Type::INT32:
            return build_array<arrow::Int32Builder *, int32>(builder, column_values, isnulls, len, need_finished);
        case arrow::Type::INT64:
            return build_array<arrow::Int64Builder *, int64>(builder, column_values, isnulls, len, need_finished);
        case arrow::Type::FLOAT:
            return build_array<arrow::FloatBuilder *, float>(builder, column_values, isnulls, len, need_finished);
        case arrow::Type::DOUBLE:
            return build_array<arrow::DoubleBuilder *, double>(builder, column_values, isnulls, len, need_finished);
        case arrow::Type::DATE32:
            return build_array<arrow::Date32Builder *, int32>(builder, column_values, isnulls, len, need_finished);
        case arrow::Type::TIMESTAMP:
            return build_array<arrow::TimestampBuilder *, int64>(builder, column_values, isnulls, len, need_finished);
        case arrow::Type::BINARY:
        case arrow::Type::STRING:
        {
            auto stringBuilder = static_cast<arrow::StringBuilder *>(builder);
            std::shared_ptr<arrow::Array> array;

            try
            {
                for (size_t idx = 0; idx < len; idx++)
                {
                    if (isnulls[idx] == true)
                    {
                        PARQUET_THROW_NOT_OK(stringBuilder->AppendNull());
                    }
                    else
                    {
                        bytea *value = *((bytea **)column_values[idx]);
                        char *str = VARDATA(value);
                        size_t len = VARSIZE(value) - VARHDRSZ;
                        PARQUET_THROW_NOT_OK(stringBuilder->Append(str, len));
                    }
                }
                if (need_finished == true)
                {
                    PARQUET_THROW_NOT_OK(stringBuilder->Finish(&array));
                    return array;
                }
            }
            catch (const std::exception& e)
            {
                elog(ERROR, "parquet_s3_fdw: %s", e.what());
            }
            return nullptr;
        }
        default:
        {
            elog(ERROR, "parquet_s3_fdw: builder_append_primitive_type not support append arrow type: '%s'", type_info.arrow.type_name.c_str());
            break;
        }
    }
    return nullptr;
}

/**
 * @brief write arrow table as a parquet file to storage system
 *
 * @param dirname directory path
 * @param s3_client aws s3 client
 * @param table source table
 */
void
ModifyParquetReader::parquet_write_file(const char *dirname, Aws::S3::S3Client *s3_client, const arrow::Table& table)
{
    try
    {
        std::string local_path;

        /* create a local one */
        if (s3_client)
        {
            local_path = TEMPORARY_DIR;
            if (IS_S3_PATH(filename.c_str()))
                /* remove 's3:/' */
                local_path += filename.substr(5);
            else
                local_path += filename;
        }
        else
        {
            local_path = filename;
        }

        /* Get parent directory */
        std::string dir;
        const size_t last_slash_idx = local_path.rfind('/');
        if (std::string::npos != last_slash_idx)
        {
            dir = local_path.substr(0, last_slash_idx);
        }

        if (dir.empty())
            elog(ERROR, "parquet_s3_fdw: Unformed file path: %s", local_path.c_str());

        /* Create parent directory if needed */
        if (!is_dir_exist(dir))
            make_path(dir);

        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(local_path));
        const int64_t chunk_size = std::max(static_cast<int64_t>(1), table.num_rows());

        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, chunk_size));

        /* Upload to S3 system if needed */
        if (s3_client)
        {
            bool    uploaded = parquet_upload_file_to_s3(dirname, s3_client, filename.c_str(), local_path.c_str());

            /* clean-up the local temporary file */
            /* delete temporary file */
            std::remove(local_path.c_str());
            /* remove parent directory if it empty */
            remove_directory_if_empty(TEMPORARY_DIR);

            if (!uploaded)
                elog(ERROR, "parquet_s3_fdw: upload file to s3 system failed!");
        }
    }
    catch (const std::exception& e)
    {
       elog(ERROR, "parquet_s3_fdw: %s", e.what());
    }
}

/**
 * @brief create an arrow table from cached data
 *
 * @return std::shared_ptr<arrow::Table> arrow table
 */
std::shared_ptr<arrow::Table>
ModifyParquetReader::create_arrow_table()
{
    arrow::ArrayVector arrays;
    try
    {
        for (size_t col_idx = 0; col_idx < this->cache_data->column_num; col_idx ++)
        {
            void** column = this->cache_data->columnsValue[col_idx];
            std::shared_ptr<arrow::Array> array;
            switch (this->types[col_idx].arrow.type_id)
            {
                case arrow::Type::LIST:
                {
                    std::shared_ptr<arrow::ArrayBuilder> valueBuilder = typeid_get_builder(this->types[col_idx].children[0]);
                    arrow::ListBuilder builder(arrow::default_memory_pool(), valueBuilder, this->file_schema->field(col_idx)->type());

                    for (size_t row_idx = 0; row_idx < this->cache_data->row_num; row_idx++)
                    {
                        if (this->cache_data->columnsNulls[col_idx][row_idx] == true)
                        {
                            PARQUET_THROW_NOT_OK(builder.AppendNull());
                        }
                        else
                        {
                            /* Get inform of each row of column LIST type, each value is struct parquet_list_value */
                            parquet_list_value list_type = *((parquet_list_value *) column[row_idx]);
                            builder.Append();
                            builder_append_primitive_type(builder.value_builder(), this->types[col_idx].children[0],
                                                          list_type.listValues, list_type.listIsNULL, list_type.len, false);
                        }
                    }

                    PARQUET_THROW_NOT_OK(builder.Finish(&array));
                    break;
                }
                case arrow::Type::MAP:
                {
                    std::shared_ptr<arrow::ArrayBuilder> keyBuilder = typeid_get_builder(this->types[col_idx].children[0]);
                    std::shared_ptr<arrow::ArrayBuilder> valueBuilder = typeid_get_builder(this->types[col_idx].children[1]);
                    arrow::MapBuilder builder(arrow::default_memory_pool(),
                                                keyBuilder,
                                                valueBuilder,
                                                this->file_schema->field(col_idx)->type()
                                             );
                    for (size_t row_idx = 0; row_idx < this->cache_data->row_num; row_idx++)
                    {
                        if (this->cache_data->columnsNulls[col_idx][row_idx] == true)
                        {
                            PARQUET_THROW_NOT_OK(builder.AppendNull());
                        }
                        else
                        {
                            parquet_map_value map_value = *((parquet_map_value *)column[row_idx]);
                            /* start add value */
                            builder.Append();
                            builder_append_primitive_type(builder.key_builder(), this->types[col_idx].children[0],
                                                          map_value.keys, map_value.key_nulls, map_value.len, false);
                            builder_append_primitive_type(builder.item_builder(), this->types[col_idx].children[1],
                                                         map_value.items, map_value.item_nulls, map_value.len, false);
                        }
                    }

                    PARQUET_THROW_NOT_OK(builder.Finish(&array));
                    break;
                }
                default:
                {
                    auto builder = typeid_get_builder(this->types[col_idx]);
                    array = builder_append_primitive_type(builder.get(), this->types[col_idx], this->cache_data->columnsValue[col_idx],
                                                          this->cache_data->columnsNulls[col_idx], this->cache_data->row_num, true);
                    break;
                }
            }
            arrays.push_back(std::move(array));
        }
    }
    catch (const std::exception& e)
    {
       elog(ERROR, "parquet_s3_fdw: %s", e.what());
    }

    return arrow::Table::Make(this->file_schema, arrays);
}

/**
 * @brief upload cached data to storage system
 *
 * @param dirname directory path
 * @param s3_client aws s3 client
 */
void
ModifyParquetReader::upload(const char *dirname, Aws::S3::S3Client *s3_client)
{
    instr_time	start, duration;

    if (this->modified == false)
        return;

    std::shared_ptr<arrow::Table> table = create_arrow_table();

    INSTR_TIME_SET_CURRENT(start);
    /* Upload file to the storage system */
    parquet_write_file(dirname, s3_client, *table);
    INSTR_TIME_SET_CURRENT(duration);
    INSTR_TIME_SUBTRACT(duration, start);
    elog(DEBUG1, "'%s' file has been uploaded in %ld seconds %ld microseconds.", this->filename.c_str(), duration.tv_sec, duration.tv_nsec / 1000);
}

/**
 * @brief check whether the given position is the correct position of the value
 *        in the sorted column after inserted.
 *        Apply null first.
 *
 * @tparam T column C type
 * @param column source column data
 * @param col_isnulls source column null list
 * @param col_len source column length
 * @param value value need insert
 * @param row_idx position
 * @return true if in column: `column[row_idx - 1]` <= `value` <= `column[row_idx]`
 */
template<typename T> inline bool
is_right_insert_position_in_sorted_col(void **column, bool *col_isnulls, size_t col_len, T value, size_t row_idx)
{
    Assert(row_idx <= col_len);

    if (col_len == 0)
        return true;
    /* no `next value` */
    if (row_idx >= col_len)
    {
        /* check `previous value` is null or <= `value`*/
        if (col_isnulls[col_len - 1] == true || *((T *)column[col_len - 1]) <= value)
            return true;
        else
            return false;
    }

    /* false if value is greater than next value after insert */
    if (*((T *)column[row_idx]) < value)
        return false;

    /* `value` <= `next value` from here */
    /* no previous value */
    if (row_idx == 0 || col_isnulls[row_idx - 1] == true)
        return true;

    /* check `previous value` <= `value`*/
    if (row_idx > 0 && *((T *)column[row_idx - 1]) <= value)
        return true;

    return false;
}

/**
 * @brief check whether given position is correct position of the new inserted row
 *
 * @param attrs inserted attribute numbers list
 * @param row_values inserted attribute values list in Datum
 * @param is_nulls inserted attribute values is null list
 * @param row_idx position
 * @return true if sorted columns are bigger (or eq) than which in `row_idx - 1` and smaller than which in (or eq) `row_idx`
 */
bool
ModifyParquetReader::is_right_position_in_sorted_column_datum(std::vector<int> attrs, std::vector<Datum> row_values, std::vector<bool> is_nulls, size_t row_idx)
{
    if (this->sorted_cols.size() == 0)
        return true;

    for (int column_idx: this->sorted_col_map)
    {
        void          **column = cache_data->columnsValue[column_idx];
        bool           *column_isnull = cache_data->columnsNulls[column_idx];
        TypeInfo       &column_type = types[column_idx];
        size_t          col_len = cache_data->row_num;
        Datum           value = (Datum) 0;
        bool            value_null = true;

        /* 0 >= row_idx >= col_len */
        if (row_idx > col_len)
            return false;

        for (size_t attr_idx = 0; attr_idx < attrs.size(); attr_idx++)
        {
            if (this->map[attr_idx] == column_idx)
            {
                value = row_values[attr_idx];
                value_null = is_nulls[attr_idx];
                break;
            }
        }

        if (value_null == true)
        {
            if (row_idx == 0)
                return true; /* null first */
            else if (row_idx > 0 && (column_isnull[row_idx - 1] == true))
                return true; /* true if previous value is null */
            else
                return false;
        }

        /* next value is null */
        if (row_idx != col_len && column_isnull[row_idx] == true)
            return false;

        switch (column_type.arrow.type_id)
        {
            case arrow::Type::INT8:
                if (!is_right_insert_position_in_sorted_col<int8>(column, column_isnull, col_len, DatumGetChar(value), row_idx))
                    return false;
                break;
            case arrow::Type::INT16:
                if (!is_right_insert_position_in_sorted_col<int16>(column, column_isnull, col_len, DatumGetInt16(value), row_idx))
                    return false;
                break;
            case arrow::Type::INT32:
                if (!is_right_insert_position_in_sorted_col<int32>(column, column_isnull, col_len, DatumGetInt32(value), row_idx))
                    return false;
                break;
            case arrow::Type::INT64:
                if (!is_right_insert_position_in_sorted_col<int64>(column, column_isnull, col_len, DatumGetInt64(value), row_idx))
                    return false;
                break;
            case arrow::Type::FLOAT:
                if (!is_right_insert_position_in_sorted_col<float>(column, column_isnull, col_len, DatumGetFloat4(value), row_idx))
                    return false;
                break;
            case arrow::Type::DOUBLE:
                if (!is_right_insert_position_in_sorted_col<double>(column, column_isnull, col_len, DatumGetFloat8(value), row_idx))
                    return false;
                break;
            case arrow::Type::TIMESTAMP:
            {
                int64 parquet_timestamp = to_parquet_timestamp(column_type.arrow.time_precision, DatumGetTimestampTz(value));
                if (!is_right_insert_position_in_sorted_col<int64>(column, column_isnull, col_len, parquet_timestamp, row_idx))
                    return false;
                break;
            }
            case arrow::Type::DATE32:
            {
                int32 parquet_date32 = to_parquet_date32(DatumGetDateADT(value));
                if (!is_right_insert_position_in_sorted_col<int32>(column, column_isnull, col_len, parquet_date32, row_idx))
                    return false;
                break;
            }
            default:
                elog(ERROR, "parquet_s3_fdw: does not support arrow type '%s' of sorted column '%s'.", column_type.arrow.type_name.c_str(), this->column_names[column_idx].c_str());
        }
    }
    return true;
}

/**
 * @brief check whether given position is correct position of the new inserted row
 *
 * @param row inserted row values list in raw data (void *)
 * @param is_nulls inserted values is null list
 * @param row_len inserted row length
 * @param row_idx insert position
 * @return true if sorted columns are bigger (or eq) than which in `row_idx - 1` and smaller than which in (or eq) `row_idx`
 */
bool
ModifyParquetReader::is_right_position_in_sorted_column_voidp(void **row, bool *is_nulls, size_t row_len, size_t row_idx)
{
    if (this->sorted_cols.size() == 0)
        return true;

    for (int column_idx: this->sorted_col_map)
    {
        void          **column = cache_data->columnsValue[column_idx];
        bool           *column_isnull = cache_data->columnsNulls[column_idx];
        TypeInfo       &column_type = types[column_idx];
        size_t          col_len = cache_data->row_num;
        void           *value = NULL;
        bool            value_null = true;

        /* 0 >= row_idx >= col_len */
        if (row_idx > col_len)
            return false;

        for (size_t attr_idx = 0; attr_idx < row_len; attr_idx++)
        {
            if (this->map[attr_idx] == column_idx)
            {
                value = row[attr_idx];
                value_null = is_nulls[attr_idx];
                break;
            }
        }

        if (value_null == true)
        {
            if (row_idx == 0)
                return true; /* null first */
            else if (row_idx > 0 && (column_isnull[row_idx - 1] == true))
                return true; /* true if previous value is null */
            else
                return false;
        }

        /* next value is null */
        if (row_idx != col_len && column_isnull[row_idx] == true)
            return false;

        switch (column_type.arrow.type_id)
        {
            case arrow::Type::INT8:
                if (!is_right_insert_position_in_sorted_col<int8>(column, column_isnull, col_len, *(int8 *)value, row_idx))
                    return false;
                break;
            case arrow::Type::INT16:
                if (!is_right_insert_position_in_sorted_col<int16>(column, column_isnull, col_len, *(int16 *)value, row_idx))
                    return false;
                break;
            case arrow::Type::INT32:
                if (!is_right_insert_position_in_sorted_col<int32>(column, column_isnull, col_len, *(int32 *)value, row_idx))
                    return false;
                break;
            case arrow::Type::INT64:
                if (!is_right_insert_position_in_sorted_col<int64>(column, column_isnull, col_len, *(int64 *)value, row_idx))
                    return false;
                break;
            case arrow::Type::FLOAT:
                if (!is_right_insert_position_in_sorted_col<float>(column, column_isnull, col_len, *(float *)value, row_idx))
                    return false;
                break;
            case arrow::Type::DOUBLE:
                if (!is_right_insert_position_in_sorted_col<double>(column, column_isnull, col_len, *(double *)value, row_idx))
                    return false;
                break;
            case arrow::Type::TIMESTAMP:
            {
                if (!is_right_insert_position_in_sorted_col<int64>(column, column_isnull, col_len, *(int64 *)value, row_idx))
                    return false;
                break;
            }
            case arrow::Type::DATE32:
            {
                if (!is_right_insert_position_in_sorted_col<int32>(column, column_isnull, col_len, *(int64 *)value, row_idx))
                    return false;
                break;
            }
            default:
                elog(ERROR, "parquet_s3_fdw: sorted column does not support '%s' type.", column_type.arrow.type_name.c_str());
        }
    }

    return true;
}

/**
 * @brief execute cast from postgres type to parquet mapped type
 *
 * @param attrs attributes need to cast
 * @param row_values attributes value
 * @param is_nulls attributes value is null
 */
void
ModifyParquetReader::exec_cast(std::vector<int> attrs, std::vector<Datum> &row_values, std::vector<bool> is_nulls)
{
    for (size_t attr_idx = 0; attr_idx < attrs.size(); attr_idx++)
    {
        if (is_nulls[attr_idx] == true)
            continue;

        auto arrow_col = this->map[attrs[attr_idx]];
        if (arrow_col >= 0 && this->types[arrow_col].need_cast == true)
            row_values[attr_idx] = do_cast(row_values[attr_idx], this->types[arrow_col]);
    }
}

/**
 * @brief parse schemaless column v:
 *          - check key column
 *          - init cast base on jsonb type
 *
 * @param attr_value jsonb Datum
 * @param[out] attrs parsed attribute
 * @param[out] values parsed attribute value
 * @param[out] is_nulls parsed attribute value is null
 * @param[out] key_attrs parsed key attribute
 * @param[out] key_values parsed key attribute value
 * @param[out] key_check whether need check null for all key column in target file or not
 * @return false if file schema is not match
 */
bool
ModifyParquetReader::schemaless_parse_column(Datum attr_value, std::vector<int> *attrs,
                                             std::vector<Datum> *values, std::vector<bool> *is_nulls,
                                             std::vector<int> *key_attrs, std::vector<Datum> *key_values,
                                             bool key_check)
{
    Jsonb       *jb = DatumGetJsonbP(attr_value);
    Datum       *cols;
    Datum       *col_vals;
    jbvType     *col_types;
    bool        *col_isnulls;
    size_t      len;
    std::set<std::string> col_names;
    std::vector<int> col_attrnum;

    parquet_parse_jsonb(&jb->root, &cols, &col_vals, &col_types, &col_isnulls, &len);

    for (size_t col_idx = 0; col_idx < len; col_idx++)
    {
        bytea      *bytea_val = DatumGetByteaP(cols[col_idx]);
        size_t      str_len = VARSIZE(bytea_val) - VARHDRSZ;
        char       *str = (char *)palloc0(sizeof(char) * (str_len + 1));

        memcpy(str, VARDATA(bytea_val), str_len);

        /* find column in file schema */
        const auto iterator = this->column_name_map.find(str);
        if (iterator == this->column_name_map.end())
            return false;

        /* check null if str is the key column */
        size_t key_idx = std::distance(this->keycol_names.begin(), this->keycol_names.find(str));
        if (key_idx < this->keycol_names.size() && col_types[col_idx] == jbvNull)
            elog(ERROR, "parquet_s3_fdw: key column %s must not be NULL.", str);

        col_names.insert(str);
        col_attrnum.push_back(iterator->second);

        if (attrs != nullptr)
            attrs->push_back(iterator->second);
        if (values != nullptr)
            values->push_back(col_vals[col_idx]);

        if (is_nulls != nullptr)
        {
            if (col_types[col_idx] == jbvNull)
                is_nulls->push_back(true);
            else
                is_nulls->push_back(false);
        }
        /* init cast for not null value */
        if (col_types[col_idx] != jbvNull)
        {
            auto &typinfo = this->types[this->map[iterator->second]];
            typinfo.pg.oid = jbvType_to_postgres_type(col_types[col_idx]);
            initialize_postgres_to_parquet_cast(typinfo, str);
        }
    }

    /* all key columns in this->keycol_names must existed and not be NULL */
    if (key_check)
    {
        for (std::string key: this->keycol_names)
        {
            size_t key_idx = std::distance(col_names.begin(), col_names.find(key));
            if (key_idx >= col_names.size() || col_types[key_idx] == jbvNull)
            {
                elog(ERROR, "parquet_s3_fdw: key column %s must not be NULL.", key.c_str());
            }

            if (key_attrs != nullptr)
                key_attrs->push_back(col_attrnum[key_idx]);
            if (key_values != nullptr)
                key_values->push_back(col_vals[key_idx]);
        }
    }

    return true;
}

/**
 * @brief insert a record to cached parquet file data
 *
 * @param attrs inserted attributes
 * @param row_values inserted attribute values
 * @param is_nulls inserted attribute values is null
 * @return true successfully inserted
 */
bool
ModifyParquetReader::exec_insert(std::vector<int> attrs, std::vector<Datum> row_values, std::vector<bool> is_nulls)
{
    size_t      insert_idx;

    Assert(attrs.size() == row_values.size());
    Assert(attrs.size() == is_nulls.size());

    try
    {
        if (this->schemaless)
        {
            Datum   jsonbval = row_values[0];

            attrs.clear();
            row_values.clear();
            is_nulls.clear();

            if (!schemaless_parse_column(jsonbval, &attrs, &row_values, &is_nulls, nullptr, nullptr, true))
                return false; /* file schema is not map */
        }
        else if (!schema_check(attrs, is_nulls))
            return false;

        /* cache parquet file data if needed */
        if (this->data_is_cached == false)
            cache_parquet_file_data();

        /* do cast if needed */
        exec_cast(attrs, row_values, is_nulls);

        /* default insert_idx is last row index */
        insert_idx = this->cache_data->row_num;

        /* find insert_idx base on sorted column if needed */
        if (this->sorted_cols.size() > 0)
        {
            bool        found = false;

            for(size_t row_idx = 0; row_idx <= cache_data->row_num; row_idx++)
            {
                if (is_right_position_in_sorted_column_datum(attrs, row_values, is_nulls, row_idx))
                {
                    insert_idx = row_idx;
                    found = true;
                    break;
                }
            }

            /* Can not find right position of inserted row */
            if (found == false)
                elog(ERROR, "parquet_s3_fdw: Can not find right position of inserted row in parquet file: %s", this->filename.c_str());
        }

        for (size_t column_idx = 0; column_idx < this->cache_data->columnNames.size(); column_idx++)
        {
            bool need_inserted = false;
            size_t attr_idx;

            for (attr_idx = 0; attr_idx < attrs.size(); attr_idx++)
            {
                if (this->map[attrs[attr_idx]] == static_cast<int>(column_idx))
                {
                    need_inserted = true;
                    break;
                }
            }

            if (need_inserted == true)
                add_value_to_column(&(this->cache_data->columnsValue[column_idx]), &(this->cache_data->columnsNulls[column_idx]),
                                      this->cache_data->row_num, this->types[column_idx],
                                      row_values[attr_idx], is_nulls[attr_idx], insert_idx);
            else
                /* insert null */
                add_value_to_column(&(this->cache_data->columnsValue[column_idx]), &(this->cache_data->columnsNulls[column_idx]),
                                      this->cache_data->row_num, this->types[column_idx],
                                      (Datum) 0, true, insert_idx);
        }
        this->cache_data->row_num++;
        this->modified = true;
    }
    catch (const std::exception& e)
    {
        elog(ERROR, "parquet_s3_fdw: %s", e.what());
    }
    return true;
}

/**
 * @brief check whether row needs to be updated by key columns
 *
 * @param key_attrs key attributes
 * @param key_values key attribute values
 * @param row_idx row index
 * @return true if all key column is match
 */
bool
ModifyParquetReader::is_modify_row(std::vector<int> key_attrs, std::vector<Datum> key_values, size_t row_idx)
{
    for (size_t key_idx = 0; key_idx < key_attrs.size(); key_idx++)
    {
        int             column_idx = this->map[key_attrs[key_idx]];
        void          **column = cache_data->columnsValue[column_idx];
        bool           *column_isnull = cache_data->columnsNulls[column_idx];
        TypeInfo       &column_type = types[column_idx];

        if (column_isnull[row_idx] == true)
            return false;

        switch (column_type.arrow.type_id)
        {
            case arrow::Type::INT8:
                if (*((int8 *)column[row_idx]) != DatumGetChar(key_values[key_idx]))
                    return false;
                break;
            case arrow::Type::INT16:
                if (*((int16 *)column[row_idx]) != DatumGetInt16(key_values[key_idx]))
                    return false;
                break;
            case arrow::Type::INT32:
                if (*((int32 *)column[row_idx]) != DatumGetInt32(key_values[key_idx]))
                    return false;
                break;
            case arrow::Type::INT64:
                if (*((int64 *)column[row_idx]) != DatumGetInt64(key_values[key_idx]))
                    return false;
                break;
            case arrow::Type::FLOAT:
                if (*((float *)column[row_idx]) != DatumGetFloat4(key_values[key_idx]))
                    return false;
                break;
            case arrow::Type::DOUBLE:
                if (*((double *)column[row_idx]) != DatumGetFloat8(key_values[key_idx]))
                    return false;
                break;
            case arrow::Type::TIMESTAMP:
            {
                int64 parquet_timestamp = to_parquet_timestamp(column_type.arrow.time_precision, DatumGetTimestampTz(key_values[key_idx]));
                if (*((int64 *)column[row_idx]) != parquet_timestamp)
                    return false;
                break;
            }
            case arrow::Type::DATE32:
            {
                int32 parquet_date32 = to_parquet_date32(DatumGetDateADT(key_values[key_idx]));
                if (*((int32 *)column[row_idx]) != parquet_date32)
                    return false;
                break;
            }
            case arrow::Type::BINARY:
            case arrow::Type::STRING:
            {
                char *desc_str = VARDATA(*((bytea **)column[row_idx]));
                char *src_str = VARDATA(DatumGetByteaP(key_values[key_idx]));
                int32 len = VARSIZE(*((bytea **)column[row_idx])) - VARHDRSZ;

                if (strncmp(desc_str, src_str, len) != 0)
                    return false;
                break;
            }
            default:
                elog(ERROR, "parquet_s3_fdw: key column does not support '%s' type.", column_type.arrow.type_name.c_str());
        }
    }

    return true;
}

/**
 * @brief remove value in idx and return it
 *
 * @param column_value target column values
 * @param isnulls target column values is null
 * @param len target column length
 * @param idx removed index
 */
static void
column_remove_idx(void ***column_value, bool **isnulls, size_t len, size_t idx)
{
    void      **new_col = (void **)palloc0(sizeof(void *) * (len - 1));
    bool       *new_isnulls = (bool *)palloc0(sizeof(bool) * (len - 1));
    void      **old_col = *column_value;
    bool       *old_isnull = *isnulls;
    size_t      row_idx = 0;

    /* copy to new buffer and ignore deleted idx */
    for (size_t i = 0; i < len; i++)
    {
        if (i == idx)   /* ignore deleted idx */
            continue;

        new_col[row_idx] = old_col[i];
        new_isnulls[row_idx] = old_isnull[i];
        row_idx++;
    }

    *column_value = new_col;
    *isnulls = new_isnulls;

    pfree(old_col);
    pfree(old_isnull);
}

/**
 * @brief remove row in cache data
 *
 * @param idx removed column index
 */
void
ModifyParquetReader::remove_row(size_t idx)
{
    for(size_t column_idx = 0; column_idx < cache_data->column_num; column_idx++)
    {
        column_remove_idx(&cache_data->columnsValue[column_idx], &cache_data->columnsNulls[column_idx], cache_data->row_num, idx);
    }
    cache_data->row_num -= 1;
}

/**
 * @brief delete a row by key column
 *
 * @param key_attrs key attributes
 * @param key_values key attributes
 * @return true if delete successfully
 */
bool
ModifyParquetReader::exec_delete(std::vector<int> key_attrs, std::vector<Datum> key_values)
{
    Assert(key_attrs.size() == key_values.size());

    try
    {
        if (this->schemaless)
        {
            Datum jsonbval = key_values[0];
            key_attrs.clear();
            key_values.clear();

            if (!schemaless_parse_column(jsonbval, nullptr, nullptr, nullptr, &key_attrs, &key_values, true))
                return false; /* file schema is not map */
        }

        /* cache parquet file data if needed */
        if (this->data_is_cached == false)
            cache_parquet_file_data();

        /* do cast if needed */
        exec_cast(key_attrs, key_values, std::vector<bool>(key_attrs.size(), false));

        for(size_t row_idx = 0; row_idx < cache_data->row_num; row_idx++)
        {
            if (is_modify_row(key_attrs, key_values, row_idx))
            {
                remove_row(row_idx);
                this->modified = true;
                return true;
            }
        }
    }
    catch(const std::exception& e)
    {
        elog(ERROR, "parquet_s3_fdw: %s", e.what());
    }

    return false;
}

/**
 * @brief update row by index
 *
 * @param row_idx row index
 * @param attrs updated attributes
 * @param values updated attributes values
 * @param is_nulls updated attributes values is null
 */
void
ModifyParquetReader::update_row(size_t row_idx, std::vector<int> attrs, std::vector<Datum> values, std::vector<bool> is_nulls)
{
    for(size_t attr_idx = 0; attr_idx < attrs.size(); attr_idx++)
    {
        int             col_idx = this->map[attrs[attr_idx]];
        void          **col = cache_data->columnsValue[col_idx];
        bool           *col_isnull = cache_data->columnsNulls[col_idx];
        TypeInfo       &col_type = types[col_idx];

        if (is_nulls[attr_idx] == true)
        {
            col_isnull[row_idx] = true;
            continue;
        }

        col_isnull[row_idx] = false;

        switch(col_type.arrow.type_id)
        {
            case arrow::Type::MAP:
            {
                Jsonb *jb = DatumGetJsonbP(values[attr_idx]);

                *((parquet_map_value *) col[row_idx]) = Jsonb_to_MAP(jb, col_type);
                break;
            }
            case arrow::Type::LIST:
            {
                if (this->schemaless)
                {
                    Jsonb *jb = DatumGetJsonbP(values[attr_idx]);
                    *((parquet_list_value *) col[row_idx]) = Jsonb_to_LIST(jb, col_type);
                }
                else
                {
                    ArrayType  *arr = DatumGetArrayTypeP(values[attr_idx]);
                    *((parquet_list_value *) col[row_idx]) = Array_to_LIST(arr, col_type);
                }
                break;
            }
            default:
                postgres_val_to_voidp(col_type, values[attr_idx], &col[row_idx]);
        }
    }
}

/**
 * @brief Move the row in row_idx to the correct position based on the sorted column value.
 *
 * @param row_idx row index
 */
void
ModifyParquetReader::reorder_row(size_t row_idx)
{
    /* get row */
    void      **row;
    bool       *row_isnulls;
    size_t      new_pos;

    if (this->sorted_cols.size() == 0)
        return;

    /* get row */
    row = (void **) palloc0(sizeof(void *) * cache_data->column_num);
    row_isnulls = (bool *) palloc0(sizeof(bool ) * cache_data->column_num);
    for (size_t col_idx = 0; col_idx < cache_data->column_num; col_idx++)
    {
        row[col_idx] = cache_data->columnsValue[col_idx][row_idx];
        row_isnulls[col_idx] = cache_data->columnsNulls[col_idx][row_idx];
    }

    /*
     * sorted columns still bigger than in `row_idx - 1` and smaller than `row_idx + 1`
     */
    if (is_right_position_in_sorted_column_voidp(row, row_isnulls, cache_data->column_num, row_idx) &&   /* check bigger than in `row_idx - 1` */
        is_right_position_in_sorted_column_voidp(row, row_isnulls, cache_data->column_num, row_idx + 1)) /* check smaller than `row_idx + 1` */
        return;

    if (this->sorted_cols.size() > 0)
    {
        bool        found = false;

        /* find position if shift up */
        for(size_t i = 0; i < row_idx; i++)
        {
            if (is_right_position_in_sorted_column_voidp(row, row_isnulls, cache_data->column_num, i))
            {
                new_pos = i;
                found = true;
                break;
            }
        }

        if (found == false)
        {
            /* try to find position if shift down */
            for(size_t i = cache_data->row_num; i > row_idx + 1; i--) /* skip check in row_idx */
            {
                if (is_right_position_in_sorted_column_voidp(row, row_isnulls, cache_data->column_num, i))
                {
                    new_pos = i - 1;
                    found = true;
                    break;
                }
            }
        }

        if (found == false)
            elog(ERROR, "parquet_s3_fdw: UPDATE failed: Can not find new position for updated row");

        /* move column from row_idx to new_pos */
        /* shift rows between row_idx and new_pos */
        if (row_idx < new_pos)
        {
            /* shift down */
            for (size_t i = row_idx; i < new_pos; i++)
            {
                for (size_t col_idx = 0; col_idx < cache_data->column_num; col_idx++)
                {
                    cache_data->columnsValue[col_idx][i] = cache_data->columnsValue[col_idx][i + 1];
                    cache_data->columnsNulls[col_idx][i] = cache_data->columnsNulls[col_idx][i + 1];
                }
            }
        }
        else
        {
            /* shift up */
            for (size_t i = row_idx; i > new_pos; i--)
            {
                for (size_t col_idx = 0; col_idx < cache_data->column_num; col_idx++)
                {
                    cache_data->columnsValue[col_idx][i] = cache_data->columnsValue[col_idx][i - 1];
                    cache_data->columnsNulls[col_idx][i] = cache_data->columnsNulls[col_idx][i - 1];
                }
            }
        }
        /* store row to new pos */
        for (size_t col_idx = 0; col_idx < cache_data->column_num; col_idx++)
        {
            cache_data->columnsValue[col_idx][new_pos] = row[col_idx];
            cache_data->columnsNulls[col_idx][new_pos] = row_isnulls[col_idx];
        }
    }
    pfree(row);
    pfree(row_isnulls);
}

/**
 * @brief update row by key columns
 *
 * @param key_attrs key attributes
 * @param key_values key attributes values
 * @param attrs updated attributes
 * @param values updated attributes values
 * @param is_nulls updated attributes values is null
 * @return true if update successfully
 */
bool
ModifyParquetReader::exec_update(std::vector<int> key_attrs, std::vector<Datum> key_values,
                                 std::vector<int> attrs, std::vector<Datum> values, std::vector<bool> is_nulls)
{
    Assert(key_attrs.size() == key_values.size());
    Assert(attrs.size() == values.size());
    Assert(attrs.size() == is_nulls.size());

    try
    {
        if (this->schemaless)
        {
            Datum jsonbval = key_values[0];
            bool is_null =  false;
            key_attrs.clear();
            key_values.clear();

            if (!schemaless_parse_column(jsonbval, nullptr, nullptr, nullptr, &key_attrs, &key_values, true))
                return false; /* file schema is not map */

            jsonbval = values[0];
            is_null = is_nulls[0];
            attrs.clear();
            values.clear();
            is_nulls.clear();

            if (is_null == false)
                /* Do not need check all column key in updated tuple */
                if (!schemaless_parse_column(jsonbval, &attrs, &values, &is_nulls, nullptr, nullptr, false))
                    return false; /* file schema is not map */
        }

        /* cache parquet file data if needed */
        if (this->data_is_cached == false)
            cache_parquet_file_data();

        /* do cast if needed */
        exec_cast(key_attrs, key_values, std::vector<bool>(key_attrs.size(), false));
        exec_cast(attrs, values, is_nulls);

        for(size_t row_idx = 0; row_idx < cache_data->row_num; row_idx++)
        {
            if (is_modify_row(key_attrs, key_values, row_idx))
            {
                if (!this->schema_check(attrs, is_nulls))
                    elog(ERROR, "parquet_s3_fdw: can not update %s file because of schema is not match.", this->filename.c_str());
                update_row(row_idx, attrs, values, is_nulls);
                reorder_row(row_idx);
                this->modified = true;
                return true;
            }
        }
    }
    catch(const std::exception& e)
    {
        elog(ERROR, "parquet_s3_fdw: %s", e.what());
    }

    return false;
}

/**
 * @brief get column type information and cache to reader object
 *
 * @param schema target file schema
 */
void
ModifyParquetReader::get_columns_type(std::shared_ptr<arrow::Schema> schema)
{
    for (size_t arrow_col_idx = 0; arrow_col_idx < schema->fields().size(); arrow_col_idx++)
    {
        auto        schema_field = schema->fields()[arrow_col_idx];
        auto       &arrow_type = schema_field->type();
        TypeInfo    typinfo(arrow_type);

        switch (arrow_type->id())
        {
            case arrow::Type::LIST:
            {
                Assert(arrow_type->fields().size() == 1);
                auto     &child = arrow_type->fields()[0];
                Oid pg_elem_type = to_postgres_type(child->type()->id());

                typinfo.children.emplace_back(child->type(),
                                                pg_elem_type);
                break;
            }
            case arrow::Type::MAP:
            {
                /*
                 * Map has the following structure:
                 *
                 * Type::MAP
                 * └─ Type::STRUCT
                 *    ├─  key type
                 *    └─  item type
                 */
                Assert(arrow_type->fields().size() == 1);
                auto &strct = arrow_type->fields()[0]->type();
                Assert(strct->fields().size() == 2);
                auto &key = strct->field(0);
                auto &item = strct->field(1);
                Oid pg_key_type = to_postgres_type(key->type()->id());
                Oid pg_item_type = to_postgres_type(item->type()->id());

                typinfo.children.emplace_back(key->type(),
                                              pg_key_type);
                typinfo.children.emplace_back(item->type(),
                                              pg_item_type);
                break;
            }
            case arrow::Type::TIMESTAMP:
            {
                auto tstype = (arrow::TimestampType *) arrow_type.get();
                typinfo.arrow.time_precision = tstype->unit();
                break;
            }
            default:
                break;
        }
        this->types.push_back(std::move(typinfo));
    }
}

/**
 * @brief schemaless_create_column_mapping: override
 *      - get column names and types
 *
 * @param schema target file schema
 */
void
ModifyParquetReader::schemaless_create_column_mapping(std::shared_ptr<arrow::Schema> schema)
{
    for (size_t arrow_col_idx = 0; arrow_col_idx < schema->fields().size(); arrow_col_idx++)
    {
        auto        schema_field = schema->fields()[arrow_col_idx];
        std::string field_name = schema_field->name();
        char        arrow_colname[NAMEDATALEN];

        if (field_name.length() > NAMEDATALEN - 1)
            throw Error("parquet column name '%s' is too long (max: %d)",
                        field_name.c_str(), NAMEDATALEN - 1);
        tolowercase(field_name.c_str(), arrow_colname);

        this->column_names.push_back(arrow_colname);

        /* arrow column index */
        this->map[arrow_col_idx] = arrow_col_idx;

        /* index of last element */
        this->column_name_map.insert({arrow_colname, arrow_col_idx});

        /* create mapping between sorted attributes and parquet column index */
        size_t sorted_col_idx = std::distance(sorted_cols.begin(), sorted_cols.find(arrow_colname));
        if (sorted_col_idx < sorted_cols.size())
            this->sorted_col_map[sorted_col_idx] = arrow_col_idx;
    }
}

/**
 * @brief [override]
 *        Create mapping between tuple descriptor and parquet columns.
 *        Create cast for postgres column type -> parquet mapped type.
 *
 * @param tupleDesc tuple descriptor
 * @param attrs_used attribute in used (unused for ModifyParquetReader)
 */
void
ModifyParquetReader::create_column_mapping(TupleDesc tupleDesc, const std::set<int> &attrs_used)
{
    std::shared_ptr<arrow::Schema>  schema;

    if (!this->is_new_file)
    {
        parquet::ArrowReaderProperties  props;
        parquet::arrow::SchemaManifest  manifest;
        auto    p_schema = this->reader->parquet_reader()->metadata()->schema();

        if (!parquet::arrow::SchemaManifest::Make(p_schema, nullptr, props, &manifest).ok())
            throw std::runtime_error("parquet_s3_fdw: error creating arrow schema");

        parquet::arrow::FromParquetSchema(p_schema, &this->file_schema);
    }

    schema = this->file_schema;

    this->sorted_col_map.resize(this->sorted_cols.size(), -1);

    /*
     * Modify feature required all columns type information to cache data.
     */
    get_columns_type(schema);

    if (this->schemaless)
    {
        /*
         * In schemaless mode, the column mapping between slot attributes
         * and column can not be created, the mapping is create by column index
         * in parquet file to make consistant with non-schemaless mode
         */
        this->map.resize(schema->fields().size());
        schemaless_create_column_mapping(schema);
        return;
    }
    else
    {
        this->map.resize(tupleDesc->natts);
    }

    for (int i = 0; i < tupleDesc->natts; i++)
    {
        char        pg_colname[NAMEDATALEN];
        const char *attname = NameStr(TupleDescAttr(tupleDesc, i)->attname);

        this->map[i] = -1;

        tolowercase(NameStr(TupleDescAttr(tupleDesc, i)->attname), pg_colname);

        for (size_t arrow_col_idx = 0; arrow_col_idx < schema->fields().size(); arrow_col_idx++)
        {
            auto        schema_field = schema->fields()[arrow_col_idx];
            std::string field_name = schema_field->name();
            char        arrow_colname[NAMEDATALEN];

            if (field_name.length() > NAMEDATALEN - 1)
                throw Error("parquet column name '%s' is too long (max: %d)",
                            field_name.c_str(), NAMEDATALEN - 1);
            tolowercase(field_name.c_str(), arrow_colname);

            /*
             * Compare postgres attribute name to the column name in arrow
             * schema.
             */
            if (strcmp(pg_colname, arrow_colname) == 0)
            {
                auto           &typinfo = this->types[arrow_col_idx];
                size_t          sorted_col_idx;

                /* mapping founded */
                this->column_names.push_back(arrow_colname);

                /* create mapping between slot attributes and parquet column index */
                this->map[i] = arrow_col_idx;

                /* create mapping between sorted column list and parquet column index */
                sorted_col_idx = std::distance(sorted_cols.begin(), sorted_cols.find(arrow_colname));
                if (sorted_col_idx < sorted_cols.size())
                    this->sorted_col_map[sorted_col_idx] = arrow_col_idx;

                /* init cast and slot attributes type information */
                typinfo.pg.oid = TupleDescAttr(tupleDesc, i)->atttypid;

                if (typinfo.arrow.type_id == arrow::Type::LIST)
                {
                    Oid     elem_type;
                    bool    error(false);

                    PG_TRY();
                    {
                        elem_type = get_element_type(typinfo.pg.oid);
                    }
                    PG_CATCH();
                    {
                        error = true;
                    }
                    PG_END_TRY();
                    if (error)
                        throw Error("failed to get type length (column '%s')",
                                    pg_colname);

                    if (!OidIsValid(elem_type))
                        throw Error("cannot convert parquet column of type "
                                    "LIST to scalar type of postgres column '%s'",
                                    pg_colname);

                    /* init cast for list's element */
                    get_element_type_info(elem_type, pg_colname, typinfo.children[0]);
                    initialize_postgres_to_parquet_cast(typinfo.children[0], pg_colname);
                }
                else
                    initialize_postgres_to_parquet_cast(typinfo, attname);

                break;
            }
        }
    }
}

/**
 * @brief get element type information from postgres type
 *
 * @param[in] type postgres array type
 * @param[in] colname column name
 * @param[out] elem element type information
 */
void
ModifyParquetReader::get_element_type_info(Oid type, const char *colname, TypeInfo &elem)
{
    int16   elem_len;
    bool    elem_byval;
    char    elem_align;

    if (OidIsValid(type))
    {
        get_typlenbyvalalign(type, &elem_len,
                            &elem_byval, &elem_align);
    }

    elem.pg.oid = type;
    elem.pg.len = elem_len;
    elem.pg.byval = elem_byval;
    elem.pg.align = elem_align;
}
