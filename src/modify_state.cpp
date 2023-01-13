/*-------------------------------------------------------------------------
 *
 * modify_state.cpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2022, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/modify_state.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "modify_state.hpp"

#include <sys/time.h>
#include <functional>
#include <list>

extern "C"
{
#include "executor/executor.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
}

/**
 * @brief Create a parquet modify state object
 *
 * @param reader_cxt memory context for reader
 * @param dirname directory path
 * @param s3_client aws s3 client
 * @param tuple_desc tuple descriptor
 * @param target_attrs target attribute
 * @param key_attrs key attribute
 * @param junk_idx junk column index
 * @param use_threads use_thread option
 * @param use_mmap use_mmap option
 * @param schemaless schemaless flag
 * @param sorted_cols sorted column list
 * @return ParquetS3FdwModifyState* parquet modify state object
 */
ParquetS3FdwModifyState *create_parquet_modify_state(MemoryContext reader_cxt,
                                                     const char *dirname,
                                                     Aws::S3::S3Client *s3_client,
                                                     TupleDesc tuple_desc,
                                                     std::set<int> target_attrs,
                                                     std::set<std::string> key_attrs,
                                                     AttrNumber *junk_idx,
                                                     bool use_threads,
                                                     bool use_mmap,
                                                     bool schemaless,
                                                     std::set<std::string> sorted_cols)

{
    return new ParquetS3FdwModifyState(reader_cxt, dirname, s3_client, tuple_desc, target_attrs, key_attrs,
                                       junk_idx, use_threads, use_mmap, schemaless, sorted_cols);
}

/**
 * @brief Construct a new Parquet S 3 Fdw Modify State:: Parquet S 3 Fdw Modify State object
 *
 * @param reader_cxt memory context for reader
 * @param dirname directory path
 * @param s3_client aws s3 client
 * @param tuple_desc tuple descriptor
 * @param target_attrs target attribute
 * @param key_attrs key attribute
 * @param junk_idx junk column index
 * @param use_threads use_thread option
 * @param use_mmap use_mmap option
 * @param schemaless schemaless flag
 * @param sorted_cols sorted column list
 */
ParquetS3FdwModifyState::ParquetS3FdwModifyState(MemoryContext reader_cxt,
                                                 const char *dirname,
                                                 Aws::S3::S3Client *s3_client,
                                                 TupleDesc tuple_desc,
                                                 std::set<int> target_attrs,
                                                 std::set<std::string> key_attrs,
                                                 AttrNumber *junk_idx,
                                                 bool use_threads,
                                                 bool use_mmap,
                                                 bool schemaless,
                                                 std::set<std::string> sorted_cols)
    : cxt(reader_cxt), dirname(dirname), s3_client(s3_client), tuple_desc(tuple_desc),
      target_attrs(target_attrs), key_names(key_attrs), junk_idx(junk_idx), use_threads(use_threads),
      use_mmap(use_mmap), schemaless(schemaless), sorted_cols(sorted_cols), user_defined_func(NULL)
{ }

/**
 * @brief Destroy the Parquet S 3 Fdw Modify State:: Parquet S3 Fdw Modify State object
 */
ParquetS3FdwModifyState::~ParquetS3FdwModifyState()
{
    for (auto reader: readers)
        delete reader;

    readers.clear();
}

/**
 * @brief add a parquet file
 *
 * @param filename file path
 */
void
ParquetS3FdwModifyState::add_file(const char *filename)
{
    ModifyParquetReader *reader = create_modify_parquet_reader(filename, cxt);

    if (s3_client)
        reader->open(dirname, s3_client);
    else
        reader->open();

    reader->set_schemaless(schemaless);
    reader->set_sorted_col_list(sorted_cols);
    reader->create_column_mapping(this->tuple_desc, this->target_attrs);
    reader->set_options(use_threads, use_mmap);
    reader->set_keycol_names(key_names);

    readers.push_back(reader);
}

/**
 * @brief add a new parquet file
 *
 * @param filename new file path
 * @param slot tuple table slot data
 * @return ModifyParquetReader* reader to new file
 */
ModifyParquetReader *
ParquetS3FdwModifyState::add_new_file(const char *filename, TupleTableSlot *slot)
{
    std::shared_ptr<arrow::Schema> new_file_schema;
    ModifyParquetReader *reader;

    if (schemaless)
        new_file_schema = schemaless_create_new_file_schema(slot);
    else
        new_file_schema = create_new_file_schema(slot);

    reader = create_modify_parquet_reader(filename, cxt, new_file_schema, true);
    reader->set_sorted_col_list(sorted_cols);
    reader->set_schemaless(schemaless);
    reader->set_options(use_threads, use_mmap);
    reader->set_keycol_names(key_names);

    /* create temporary file */
    reader->create_column_mapping(this->tuple_desc, this->target_attrs);
    reader->create_new_file_temp_cache();

    readers.push_back(reader);
    return reader;
}

/**
 * @brief check aws s3 client is existed
 *
 * @return true if s3_client is existed
 */
bool
ParquetS3FdwModifyState::has_s3_client()
{
    if (this->s3_client)
        return true;
    return false;
}

/**
 * @brief upload all cached data on readers list
 */
void
ParquetS3FdwModifyState::upload()
{
    for(auto reader: readers)
    {
        reader->upload(dirname, s3_client);
    }
}

/**
 * @brief check whether given column name is existed on key columns list
 *
 * @param name column name
 * @return true if given column name is existed on key columns list
 */
bool
ParquetS3FdwModifyState::is_key_column(std::string name)
{
    for (std::string key_name: this->key_names)
    {
        if (key_name == name)
            return true;
    }

    return false;
}

/**
 * @brief insert a postgres tuple table slot to list parquet file
 *
 * @param slot tuple table slot
 * @return true if insert successfully
 */
bool
ParquetS3FdwModifyState::exec_insert(TupleTableSlot *slot)
{
    std::vector<int>            attrs;
    std::vector<Datum>          values;
    std::vector<Oid>            types;
    std::vector<bool>           is_nulls;
    char                       *user_selects_file = NULL;

    /* get value from slot to corresponding vector */
    for (int attnum: target_attrs)
    {
        char        pg_colname[NAMEDATALEN];
        bool        is_null;
        Datum       attr_value = 0;
        Oid         attr_type;

        tolowercase(NameStr(TupleDescAttr(slot->tts_tupleDescriptor, attnum-1)->attname),
                    pg_colname);
        attr_value = slot_getattr(slot, attnum, &is_null);
        attr_type = TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->atttypid;

        if (this->schemaless == true)
        {
            /* get first jsonb value */
            if (attr_type != JSONBOID)
                continue;

            if (is_null)
                elog(ERROR, "parquet_s3_fdw: schemaless column %s must not be null", pg_colname);
        }
        else
        {
            if (is_key_column(pg_colname) && is_null)
                elog(ERROR, "parquet_s3_fdw: key column %s must not be NULL.", pg_colname);
        }

        attrs.push_back(attnum - 1);
        values.push_back(attr_value);
        is_nulls.push_back(is_null);
        types.push_back(attr_type);
    }

    if (this->schemaless == true && attrs.size() == 0)
        elog(ERROR, "parquet_s3_fdw: can not find any record for schemaless mode.");

    if (this->user_defined_func)
    {
        user_selects_file = get_selected_file_from_userfunc(user_defined_func, slot, this->dirname);
        if (IS_S3_PATH(user_selects_file))
        {
            if (dirname)
            {
                const char *pch = strstr(user_selects_file, dirname);
                if (pch == NULL || pch != user_selects_file)
                    elog(ERROR, "parquet_s3_fdw: %s file does not belong to directory %s.", user_selects_file, dirname);

                /* remove directory path in file path */
                user_selects_file += strlen(dirname);
            }
        }
    }

    /* loop over parquet reader */
    for (auto reader: readers)
    {
        if (user_selects_file == NULL || reader->compare_filename(user_selects_file))
        {
            if (reader->exec_insert(attrs, values, is_nulls))
            {
                return true;
            }
            else if (user_selects_file != NULL)
            {
                elog(ERROR, "parquet_s3_fdw: schema of %s file is not match with insert value.", user_selects_file);
            }
        }
    }

    /* create new file to hold this value */
    if (user_selects_file)
    {
        return add_new_file(user_selects_file, slot)->exec_insert(attrs, values, is_nulls);
    }
    else if (dirname != NULL)
    {
        /* create file name base on syntax: [foreign table name]-[data time].parquet */
        char	   *timeString = NULL;
        Oid			typOutput = InvalidOid;
        bool		typIsVarlena = false;
        std::string new_file;

        getTypeOutputInfo(TIMESTAMPOID, &typOutput, &typIsVarlena);
        timeString = OidOutputFunctionCall(typOutput, GetCurrentTimestamp());
        new_file = "/" + std::string(this->rel_name)
                       + "-"
                       + std::string(timeString)
                       + ".parquet";

        /* only add directory name for local file */
        if (s3_client == NULL)
            new_file = std::string(dirname) + new_file;

        return add_new_file(new_file.c_str(), slot)->exec_insert(attrs, values, is_nulls);
    }
    else
    {
        elog(ERROR, "parquet_s3_fdw: can not find modify target file");
    }
    return false;
}

/**
 * @brief update a record in list parquet file by key column
 *
 * @param slot updated values
 * @param planSlot junk values
 * @return true if update successfully
 */
bool
ParquetS3FdwModifyState::exec_update(TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    std::vector<int>            key_attrs;
    std::vector<Datum>          key_values;
    std::vector<Oid>            key_types;
    std::vector<int>            attrs;
    std::vector<Datum>          values;
    std::vector<Oid>            types;
    std::vector<bool>           is_nulls;

    Assert(this->key_names.size() > 0);

    /* get value from slot to corresponding vector */
    for(int attnum: target_attrs)
    {
        char        pg_colname[NAMEDATALEN];
        bool        is_null;
        Datum       attr_value = 0;
        Oid         attr_type;
		Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1);

        tolowercase(NameStr(attr->attname), pg_colname);
        attr_value = slot_getattr(slot, attnum, &is_null);
        attr_type = attr->atttypid;

        if (this->schemaless == true)
        {
            /* get first jsonb value */
            if (attr_type != JSONBOID)
                continue;
        }
        else
        {
            if (is_key_column(pg_colname) && is_null)
                elog(ERROR, "parquet_s3_fdw: key column %s must not be NULL.", pg_colname);
        }

        attrs.push_back(attnum - 1);
        values.push_back(attr_value);
        is_nulls.push_back(is_null);
        types.push_back(attr_type);
    }

    /* Get column key data from retrieve->cond_attrs */
    for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);
        Datum       value;
        bool		is_null = false;

        /* look for the "key" option on this column */
        if (this->junk_idx[i] == InvalidAttrNumber)
            continue;

        value = ExecGetJunkAttribute(planSlot, this->junk_idx[i], &is_null);
        if (this->schemaless)
        {
            /* get first jsonb value */
            if (att->atttypid != JSONBOID)
                continue;

            if (is_null)
                elog(ERROR, "parquet_s3_fdw: schemaless column %s must not be null", att->attname.data);

            key_attrs.push_back(i);
            key_values.push_back(value);
            key_types.push_back(att->atttypid);
        }
        else
        {
            if (is_key_column(att->attname.data))
            {
                if (is_null)
                    elog(ERROR, "parquet_s3_fdw: key column %s must not be null", att->attname.data);

                key_attrs.push_back(i);
                key_values.push_back(value);
                key_types.push_back(att->atttypid);
            }
        }
    }

    /* loop over parquet reader */
    for (auto reader: readers)
    {
        if (reader->exec_update(key_attrs, key_values, attrs, values, is_nulls))
            return true;
    }

    return false;
}

/**
 * @brief delete a record in list parquet file by key column
 *
 * @param slot tuple table slot
 * @param planSlot junk values
 * @return true if delete successfully
 */
bool
ParquetS3FdwModifyState::exec_delete(TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    std::vector<int>            key_attrs;
    std::vector<Datum>          key_values;
    std::vector<Oid>            key_types;

    /* Get column key data from retrieve->cond_attrs */
    for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);
        Datum       value;
        bool		is_null = false;

        /* look for the "key" option on this column */
        if (this->junk_idx[i] == InvalidAttrNumber)
            continue;

        value = ExecGetJunkAttribute(planSlot, this->junk_idx[i], &is_null);

        if (this->schemaless)
        {
            if (att->atttypid != JSONBOID)
                continue;

            key_attrs.push_back(i);
            key_values.push_back(value);
            key_types.push_back(att->atttypid);
        }
        else
        {
            if (is_key_column(att->attname.data))
            {
                if (is_null)
                    elog(ERROR, "parquet_s3_fdw: key column must not be null");

                key_attrs.push_back(i);
                key_values.push_back(value);
                key_types.push_back(att->atttypid);
            }
        }
    }

    if (key_attrs.size() == 0)
        elog(ERROR, "parquet_s3_fdw: delete failed: all key columns are null");

    /* loop over parquet reader */
    for (auto reader: readers)
    {
        if (reader->exec_delete(key_attrs, key_values))
            return true;
    }

    return false;
}

/**
 * @brief set relation name
 *
 * @param name relation name
 */
void
ParquetS3FdwModifyState::set_rel_name(char *name)
{
    this->rel_name = name;
}

/**
 * @brief set user defined function name
 *
 * @param func_name user defined function name
 */
void
ParquetS3FdwModifyState::set_user_defined_func(char *func_name)
{
    this->user_defined_func = func_name;
}

/**
 * @brief get arrow::DataType from given arrow type id
 *
 * @param type_id arrow type id
 * @return std::shared_ptr<arrow::DataType>
 */
static std::shared_ptr<arrow::DataType>
to_primitive_DataType(arrow::Type::type type_id)
{
    switch(type_id)
    {
        case arrow::Type::BOOL:
            return arrow::boolean();
        case arrow::Type::INT8:
            return arrow::int8();
        case arrow::Type::INT16:
            return arrow::int16();
        case arrow::Type::INT32:
            return arrow::int32();
        case arrow::Type::INT64:
            return arrow::int64();
        case arrow::Type::FLOAT:
            return arrow::float32();
        case arrow::Type::DOUBLE:
            return arrow::float64();
        case arrow::Type::DATE32:
            return arrow::date32();
        case arrow::Type::TIMESTAMP:
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        default:
            return arrow::utf8(); /* all other type is convert as text */
    }
}

/**
 * @brief get arrow::DataType from given Jsonb value type
 *
 * @param jbv_type Jsonb value type
 * @return std::shared_ptr<arrow::DataType>
 */
static std::shared_ptr<arrow::DataType>
jbvType_to_primitive_DataType(jbvType jbv_type)
{
    switch (jbv_type)
    {
        case jbvNumeric:
            return arrow::float64();
        case jbvBool:
            return arrow::boolean();
        default:
            return arrow::utf8(); /* all other type is convert as text */
    }
}

/**
 * @brief parse schemaless/jsonb column
 *
 * @param[in] attr_value Jsonb Datum
 * @param[out] names parsed columns name
 * @param[out] values parsed columns value
 * @param[out] is_nulls parsed columns value isnull
 * @param[out] types parsed columns Jsonb type
 */
static void
parse_jsonb_column(Datum attr_value, std::vector<std::string> &names,
                   std::vector<Datum> &values, std::vector<bool> &is_nulls,
                   std::vector<jbvType> &types)
{
    Jsonb       *jb = DatumGetJsonbP(attr_value);
    Datum       *cols;
    Datum       *col_vals;
    jbvType     *col_types;
    bool        *col_isnulls;
    size_t      len;

    parquet_parse_jsonb(&jb->root, &cols, &col_vals, &col_types, &col_isnulls, &len);

    for (size_t col_idx = 0; col_idx < len; col_idx++)
    {
        bytea *bytea_val = DatumGetByteaP(cols[col_idx]);
        size_t str_len = VARSIZE(bytea_val) - VARHDRSZ;
        char *str = (char *)palloc0(sizeof(char) * (str_len + 1));

        memcpy(str, VARDATA(bytea_val), str_len);
        names.push_back(str);
        values.push_back(col_vals[col_idx]);
        if (col_types[col_idx] == jbvNull)
            is_nulls.push_back(true);
        else
            is_nulls.push_back(false);

        types.push_back(col_types[col_idx]);
    }
}

/**
 * @brief for schemaless mode only
 *      - Create base on column of inserted record and existed columns.
 *      - If column is not exist on any file, create schema by mapping type.
 *
 * @param slot tuple table slot
 * @return std::shared_ptr<arrow::Schema> new file schema
 */
std::shared_ptr<arrow::Schema>
ParquetS3FdwModifyState::schemaless_create_new_file_schema(TupleTableSlot *slot)
{
    arrow::FieldVector          fields;
    std::vector<std::string>    column_names;
    std::vector<Datum>          values;
    std::vector<bool>           is_nulls;
    std::vector<jbvType>        types;

    /* Get jsonb column */
    for (int i = 0; i < this->tuple_desc->natts; i++)
    {
        Form_pg_attribute   att = TupleDescAttr(this->tuple_desc, i);
        Datum               att_val;
        bool                att_isnull;

        if (att->attisdropped || att->atttypid != JSONBOID)
            continue;

        att_val = slot_getattr(slot, att->attnum, &att_isnull);

        /* try to get not null jsonb col */
        if (att_isnull)
            continue;

        parse_jsonb_column(att_val, column_names, values, is_nulls, types);
    }

    if (column_names.size() == 0)
        elog(ERROR, "parquet_s3_fdw: can not find any record for schemaless mode.");

    bool *founds = (bool *)palloc0(sizeof(bool) * column_names.size());

    /* try to get existed column info */
    for (size_t i = 0; i < column_names.size(); i++)
    {
        if (founds[i] == true)
            continue;

        for (auto reader : readers)
        {
            auto schema = reader->get_file_schema();
            char            pg_colname[NAMEDATALEN];

            tolowercase(column_names[i].c_str(), pg_colname);
            auto field = schema->GetFieldByName(pg_colname);

            if (field != nullptr)
            {
                founds[i] = true;
                fields.push_back(field);
                break;
            }
        }
    }

    /* column can not be found in any file */
    for (size_t i = 0; i < column_names.size(); i++)
    {
        if (founds[i] == true)
            continue;

        switch (types[i])
        {
            case jbvNumeric:
                fields.push_back(arrow::field(column_names[i].c_str(), arrow::float64()));
                break;
            case jbvBool:
                fields.push_back(arrow::field(column_names[i].c_str(), arrow::boolean()));
                break;
            case jbvNull:
            case jbvString:
                fields.push_back(arrow::field(column_names[i].c_str(), arrow::utf8()));
                break;
            case jbvArray:
            case jbvObject:
            case jbvBinary:
            {
                Jsonb *jb = DatumGetJsonbP(values[i]);
                Datum       *cols;
                Datum       *col_vals;
                jbvType     *col_types;
                bool        *col_isnulls;
                size_t      len;

                parquet_parse_jsonb(&jb->root, &cols, &col_vals, &col_types, &col_isnulls, &len);

                if (JsonContainerIsArray(&jb->root))
                {
                    /* get type of first element only */
                    fields.push_back(arrow::field(column_names[i].c_str(), arrow::list(jbvType_to_primitive_DataType(col_types[0]))));
                }
                else if (JsonContainerIsObject(&jb->root))
                {
                    fields.push_back(arrow::field(column_names[i].c_str(), arrow::map(arrow::utf8(), jbvType_to_primitive_DataType(col_types[0]))));
                }
                else
                    elog(ERROR, "parquet_s3_fdw: can not create parquet mapping type for jsonb type: %d.", types[i]);
                break;
            }
            default:
                elog(ERROR, "parquet_s3_fdw: can not create parquet mapping type for jsonb type: %d.", types[i]);
                break;
        }
    }
    return arrow::schema(fields);
}

/**
 * @brief Create base on column of inserted record and existed columns.
 *        If column is not exist on any file, create schema by mapping type.
 *
 * @param slot tuple table slot
 * @return std::shared_ptr<arrow::Schema> new file schema
 */
std::shared_ptr<arrow::Schema>
ParquetS3FdwModifyState::create_new_file_schema(TupleTableSlot *slot)
{
    arrow::FieldVector fields;
    int         natts = this->tuple_desc->natts;
    bool       *founds = (bool *)palloc0(sizeof(bool) * natts);

    memset(founds, false, natts);
    for (auto reader : readers)
    {
        auto schema = reader->get_file_schema();
        for (int i = 0; i < natts; i++)
        {
            char            pg_colname[NAMEDATALEN];
            Form_pg_attribute att = TupleDescAttr(this->tuple_desc, i);

            if (founds[i] == true || att->attisdropped)
                continue;

            tolowercase(NameStr(att->attname), pg_colname);
            auto field = schema->GetFieldByName(pg_colname);

            if (field != nullptr)
            {
                founds[i] = true;
                fields.push_back(field);
            }
        }
    }

    for (int i = 0; i < natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(this->tuple_desc, i);
        arrow::Type::type type_id;

        if (att->attisdropped || founds[i] == true)
            continue;

        type_id = postgres_to_arrow_type(att->atttypid);
        if (type_id != arrow::Type::NA)
            fields.push_back(arrow::field(att->attname.data, to_primitive_DataType(type_id)));
        else if (att->atttypid == JSONBOID)
        {
            bool        att_isnull;
            Datum      *cols;
            Datum      *col_vals;
            jbvType    *col_types;
            bool       *col_isnulls;
            size_t      len;
            Datum       att_val = slot_getattr(slot, att->attnum, &att_isnull);

            if (att_isnull)
            {
                /* we has no information for MAP column => use text instead */
                fields.push_back(arrow::field(att->attname.data,arrow::map(arrow::utf8(), arrow::utf8())));
            }
            else
            {
                Jsonb      *jb = DatumGetJsonbP(att_val);

                parquet_parse_jsonb(&jb->root, &cols, &col_vals, &col_types, &col_isnulls, &len);

                if (JsonContainerIsObject(&jb->root))
                {
                    if (len > 0)
                        fields.push_back(arrow::field(att->attname.data, arrow::map(arrow::utf8(), jbvType_to_primitive_DataType(col_types[0]))));
                    else
                        /* we has no information for MAP column => use text => text instead */
                        fields.push_back(arrow::field(att->attname.data,arrow::map(arrow::utf8(), arrow::utf8())));
                }
                else
                    elog(ERROR, "parquet_s3_fdw: can not create parquet mapping type for jsonb column: %s.", att->attname.data);
            }
        }
        else
        {
            Oid elemtyp = get_element_type(att->atttypid);
            if (elemtyp != InvalidOid)
            {
                arrow::Type::type elem_type_id = postgres_to_arrow_type(elemtyp);
                if (elem_type_id != arrow::Type::NA)
                    fields.push_back(arrow::field(att->attname.data, arrow::list(to_primitive_DataType(elem_type_id))));
            }
            else
            {
                elog(ERROR, "parquet_s3_fdw: Can not create parquet mapping type for type OID: %d, column: %s.", att->atttypid, att->attname.data);
            }
        }

    }

    return arrow::schema(fields);
}
