/*-------------------------------------------------------------------------
 *
 * modify_state.hpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2022, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/modify_state.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARQUET_FDW_MODIFY_STATE_HPP
#define PARQUET_FDW_MODIFY_STATE_HPP

#include <list>
#include <set>
#include <vector>

#include "modify_reader.hpp"

extern "C"
{
#include "postgres.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
}

class ParquetS3FdwModifyState
{
private:
    /* list parquet reader of target files */
    std::vector<ModifyParquetReader *> readers;
    /* memory context of reader */
    MemoryContext       cxt;
    /* target directory name */
    const char         *dirname;
    /* S3 system client */
    Aws::S3::S3Client  *s3_client;
    /* foreign table desc */
    TupleDesc           tuple_desc;
    /* list attnum of needed modify attributes */
    std::set<int>       target_attrs;
    /* list column key names */
    std::set<std::string> key_names;
    /* List of junk attributes */
    AttrNumber         *junk_idx;
    /* parquet reader option */
    bool                use_threads;
    bool                use_mmap;
    /* schemaless mode flag */
    bool                schemaless;
    /* sorted column list */
    std::set<std::string> sorted_cols;
    /* insert_file_selector function name */
    char               *user_defined_func;
    /* foreign table name */
    char               *rel_name;

public:
    MemoryContext       fmstate_cxt;

protected:
    /* true if `name` is the name of a key column */
    bool is_key_column(std::string name);

public:
    ParquetS3FdwModifyState(MemoryContext reader_cxt,
                            const char *dirname,
                            Aws::S3::S3Client *s3_client,
                            TupleDesc tuple_desc,
                            std::set<int> target_attrs,
                            std::set<std::string> key_attrs,
                            AttrNumber *junk_idx,
                            bool use_threads,
                            bool use_mmap,
                            bool schemaless,
                            std::set<std::string> sorted_cols);
    ~ParquetS3FdwModifyState();

    /* create reader for `filename` and add to list file */
    void add_file(const char *filename);
    /* create new file and its temporary cache data */
    ModifyParquetReader * add_new_file(const char *filename, TupleTableSlot *slot);
    /* execute insert `*slot` to cache data */
    bool exec_insert(TupleTableSlot *slot);
    /* execute update */
    bool exec_update(TupleTableSlot *slot, TupleTableSlot *planSlot);
    /* execute delete */
    bool exec_delete(TupleTableSlot *slot, TupleTableSlot *planSlot);
    /* upload modified parquet file to storage system (local/S3) */
    void upload();
    /* true if s3_client is set */
    bool has_s3_client();

    /* create schema for new file */
    std::shared_ptr<arrow::Schema> create_new_file_schema(TupleTableSlot *slot);
    std::shared_ptr<arrow::Schema> schemaless_create_new_file_schema(TupleTableSlot *slot);

    void set_user_defined_func(char *func_name);
    void set_rel_name(char *name);
};

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
                                                     std::set<std::string> sorted_cols);

#endif
