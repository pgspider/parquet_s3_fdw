/*-------------------------------------------------------------------------
 *
 * modify_reader.hpp
 *        FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2022, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        contrib/parquet_s3_fdw/src/modify_reader.hpp
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "reader.hpp"
#include "common.hpp"

/*
 * ModifyParquetReader
 *      - Read parquet file and cache this value
 *      - Overwrite parquet file by cache data
 *      - Create new file from given file schema
 */
class ModifyParquetReader: ParquetReader
{
private:
    /* represent a MAP */
    typedef struct parquet_map_value
    {
        /* Key values list */
        void      **keys;
        /* true if key is NULL (does not happend) */
        bool       *key_nulls;
        /* Item values list */
        void      **items;
        /* true if item value is NULL */
        bool       *item_nulls;
        /* len of keys list and items list */
        size_t      len;
    } parquet_map_value;

    /* represent a nested LIST */
    typedef struct parquet_list_value
    {
        /* nested list value */
        void      **listValues;
        /* true if value is NULL */
        bool       *listIsNULL;
        /* len of nested list */
        size_t      len;
    } parquet_list_value;

    /* Cache data structure */
    typedef struct parquet_file_info
    {
        /* Parquet file data in column array */
        void                     ***columnsValue;
        /* true if a value is NULL */
        bool                      **columnsNulls;
        /* parquet column names list */
        std::vector<std::string>    columnNames;
        /* column num */
        size_t                      column_num;
        /* num of row (column len) */
        size_t                      row_num;
    } parquet_file_info;

private:
    /* true if cache data has been modified */
    bool    modified;
    /* cache data of target parquet file, it will be whole schema if parquet file does not exist */
    parquet_file_info *cache_data;
    /* file data is cached or not */
    bool    data_is_cached;
    /* schema of target file */
    std::shared_ptr<arrow::Schema>  file_schema;
    /* mapping of column name and column idx (use for schemaless mode only) */
    std::map<std::string, int>      column_name_map;
    /* list key column names */
    std::set<std::string>           keycol_names;
    /* true if taget file is new file (does not existed) */
    bool                            is_new_file;

private:
    /* read target file and cache data to cache_data */
    void cache_parquet_file_data();
    /* read arrow primitive type and convert to `void *` value */
    void *read_primitive_type_raw(arrow::Array *array, TypeInfo *type_info, int64_t i);
    /* read an arrow column form table in `void **` type (column of `void *` data)  */
    void read_column(std::shared_ptr<arrow::Table> table, int col, TypeInfo *type_info,
                     void ***data, bool **is_null);
    /* return corresponding C type size of arrow type */
    size_t get_arrow_type_size(arrow::Type::type type_id);
    /* get element type information from postgres array type */
    void get_element_type_info(Oid type, const char *colname, TypeInfo &elem);

    /* CACHE PARQUET FILE HELPER FUNCTIONS */
    /* get arrow array builder instance from arrow type */
    std::shared_ptr<arrow::ArrayBuilder> typeid_get_builder(const TypeInfo& typeinfo);
    /* append primitive value to array builder */
    std::shared_ptr<arrow::Array> builder_append_primitive_type(arrow::ArrayBuilder *builder, TypeInfo &type_info,
                                                                void **column_values, bool *isnulls, size_t len,
                                                                bool need_finished);
    /* create arrow table from cache data */
    std::shared_ptr<arrow::Table> create_arrow_table();
    /* write arrow table to a parqet file */
    void parquet_write_file(const char *dirname, Aws::S3::S3Client *s3_client, const arrow::Table& table);

    /* CAST HELPER FUNCTIONS */
    /* init cast from postgres type to mapped parquet file */
    void initialize_postgres_to_parquet_cast(TypeInfo &typinfo, const char *attname);
    void exec_cast(std::vector<int> column_name, std::vector<Datum> &row_value, std::vector<bool> is_nulls);

    /* INSERT HELPER FUNCTIONS */
    /* insert value to a column */
    void add_value_to_column(void ***column, bool **isnulls, size_t column_len, TypeInfo &column_type,
                             Datum value, bool value_null, size_t idx);
    /* Get value from Datum and convert to void pointer */
    void postgres_val_to_voidp(const TypeInfo &type_info, Datum value, void **res);
    /* convert jsonb value to internal MAP data */
    parquet_map_value Jsonb_to_MAP(Jsonb *jb, TypeInfo &column_type);
    /* convert jsonb value to internal nested LIST data */
    parquet_list_value Jsonb_to_LIST(Jsonb *jb, TypeInfo &column_type);
    /* convert array value to internal nested LIST data */
    parquet_list_value Array_to_LIST(ArrayType *arr, TypeInfo &column_type);
    /* true if sorted columns are bigger (or eq) than which in `row_idx - 1` and smaller than which in (or eq) `row_idx` */
    bool is_right_position_in_sorted_column_datum(std::vector<int> attrs, std::vector<Datum> row_values,
                                           std::vector<bool> is_nulls, size_t row_idx);
    bool is_right_position_in_sorted_column_voidp(void **row, bool *is_nulls, size_t row_len, size_t row_idx);

    /* UPDATE/DELETE HELPER FUNCTION */
    /* true if row in idx match all key */
    bool is_modify_row(std::vector<int> key_attrs, std::vector<Datum> key_values, size_t row_idx);
    /* remove row in idx in cache data */
    void remove_row(size_t idx);
    /* update row in idx in cache data */
    void update_row(size_t row_idx, std::vector<int> attrs, std::vector<Datum> values, std::vector<bool> is_nulls);
    void reorder_row(size_t row_idx);

    /* get all parquet column type */
    void get_columns_type(std::shared_ptr<arrow::Schema> schema);

    /* schemaless support function */
    void schemaless_create_column_mapping(std::shared_ptr<arrow::Schema> schema);
    bool schemaless_parse_column(Datum attr_value, std::vector<int> *attrs,
                                 std::vector<Datum> *values,
                                 std::vector<bool> *is_nulls,
                                 std::vector<int> *key_attrs = nullptr,
                                 std::vector<Datum> *key_values = nullptr,
                                 bool key_check = false);

public:
    ModifyParquetReader(const char* filename,
                        MemoryContext cxt,
                        std::shared_ptr<arrow::Schema> schema = nullptr,
                        bool is_new_file = false,
                        int reader_id = -1);
    ~ModifyParquetReader();

    /* execute insert a postgres slot */
    bool exec_insert(std::vector<int> attrs, std::vector<Datum> values, std::vector<bool> is_nulls);

    /* execute update */
    bool exec_update(std::vector<int> key_attrs, std::vector<Datum> key_values,
                     std::vector<int> attrs, std::vector<Datum>values, std::vector<bool>is_nulls);

    /* delete a record by key column values */
    bool exec_delete(std::vector<int> key_attrs, std::vector<Datum> key_values);


    /* create new parquet file and overwrite to storage system */
    void upload(const char *dirname, Aws::S3::S3Client *s3_client);

    /* check that all column in target_attr are existed in parquet file */
    bool schema_check(std::vector<int> attrs, std::vector<bool> is_nulls);

    /* compare with target file name */
    bool compare_filename(char *filename);

    void set_sorted_col_list(std::set<std::string> sorted_cols);
    void set_schemaless(bool schemaless);
    void set_keycol_names(std::set<std::string> keycol_names);
    void create_column_mapping(TupleDesc tupleDesc, const std::set<int> &attrs_used);
    std::shared_ptr<arrow::Schema> get_file_schema();
    void create_new_file_temp_cache();

    /* override parent's functions */
    void open() override;
    void open(const char *dirname, Aws::S3::S3Client *s3_client) override;
    void close() override;
    void set_options(bool use_threads, bool use_mmap);
    ReadStatus next(TupleTableSlot *slot, bool fake=false) override
    {
        elog(ERROR, "parquet_s3_fdw: ModifyParquetReader does not support next() function");
    }
    void rescan() override
    {
        elog(ERROR, "parquet_s3_fdw: ModifyParquetReader does not support rescan() function");
    }
};

ModifyParquetReader *create_modify_parquet_reader(const char *filename,
                                                  MemoryContext cxt,
                                                  std::shared_ptr<arrow::Schema> schema = nullptr,
                                                  bool is_new_file = false,
                                                  int reader_id = -1);
