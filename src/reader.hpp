/*-------------------------------------------------------------------------
 *
 * reader.hpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/reader.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARQUET_FDW_READER_HPP
#define PARQUET_FDW_READER_HPP

#include <memory>
#include <mutex>
#include <set>
#include <vector>
#include <math.h>

#include "parquet_s3_fdw.hpp"

#include "arrow/api.h"
#include "parquet/arrow/reader.h"

extern "C"
{
#include "postgres.h"
#include "fmgr.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/pg_list.h"
#include "storage/spin.h"
}

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <parquet/arrow/reader.h>

struct ParallelCoordinator
{
    slock_t     lock;
    int32       next_reader;
    int32       next_rowgroup;
};

class FastAllocatorS3;

enum ReadStatus
{
    RS_SUCCESS = 0,
    RS_INACTIVE = 1,
    RS_EOF = 2
};

class ParquetReader
{
protected:

    struct TypeInfo
    {
        struct
        {
            arrow::Type::type   type_id;
            std::string         type_name;
        } arrow;

        struct
        {
            Oid         oid;
            int16       len;    /*                         */
            bool        byval;  /* Only for array elements */
            char        align;  /*                         */
        } pg;

        /*
         * Cast functions from dafult postgres type defined in `to_postgres_type`
         * to actual table column type.
         */
        FmgrInfo       *castfunc;
        FmgrInfo       *outfunc;

        /* Underlying types for complex types like list and map */
        std::vector<TypeInfo> children;

        /*
         * Column index in parquet schema. For complex types and children
         * index is equal -1. Currently only used for checking column
         * statistics.
         */
        int             index;

        TypeInfo()
            : arrow{}, pg{}, castfunc(nullptr), index(-1)
        {}

        TypeInfo(TypeInfo &&ti)
            : arrow(ti.arrow), pg(ti.pg), castfunc(nullptr),
              children(std::move(ti.children)), index(-1)
        {}

        TypeInfo(std::shared_ptr<arrow::DataType> arrow_type)
            : arrow{arrow_type->id(), arrow_type->name()}, pg{},
              castfunc(nullptr), index(-1)
        {}

        TypeInfo(std::shared_ptr<arrow::DataType> arrow_type, Oid typid,
                 FmgrInfo *castfunc)
            : arrow{arrow_type->id(), arrow_type->name()}, pg{typid, 0, false, 0},
              castfunc(castfunc), index(-1)
        {}
    };

protected:
    std::string                     filename;

    /* The reader identifier needed for parallel execution */
    int32_t                         reader_id;

    std::unique_ptr<parquet::arrow::FileReader> reader;

    /* Arrow column indices that are used in query */
    std::vector<int>                indices;

    /*
     * Mapping between slot attributes and arrow result set columns.
     * Corresponds to 'indices' vector.
     */
    std::vector<int>                map;

    /*
     * Cast functions from dafult postgres type defined in `to_postgres_type`
     * to actual table column type.
     */
    std::vector<FmgrInfo *>         castfuncs;

    std::vector<std::string>        column_names;
    std::vector<TypeInfo>           types;

    /* Coordinator for parallel query execution */
    ParallelCoordinator            *coordinator;

    /*
     * List of row group indexes to scan
     */
    std::vector<int>                rowgroups;

    std::unique_ptr<FastAllocatorS3>  allocator;
    ReaderCacheEntry               *reader_entry;

    /*
     * libparquet options
     */
    bool    use_threads;
    bool    use_mmap;

    /* Wether object is properly initialized */
    bool    initialized;

protected:
    Datum read_primitive_type(arrow::Array *array, const TypeInfo &typinfo,
                              int64_t i);
    Datum nested_list_to_datum(arrow::ListArray *larray, int pos, const TypeInfo &typinfo);
    Datum map_to_datum(arrow::MapArray *maparray, int pos, const TypeInfo &typinfo);
    FmgrInfo *find_castfunc(arrow::Type::type src_type, Oid dst_type,
                            const char *attname);
    FmgrInfo *find_outfunc(Oid typoid);
    template<typename T> inline void copy_to_c_array(T *values,
                                                     const arrow::Array *array,
                                                     int elem_size);
    template <typename T> inline const T* GetPrimitiveValues(const arrow::Array& arr);

public:
    ParquetReader(MemoryContext cxt);
    virtual ~ParquetReader() = 0;
    virtual ReadStatus next(TupleTableSlot *slot, bool fake=false) = 0;
    virtual void rescan() = 0;
    virtual void open() = 0;
    virtual void open(const char *dirname,
              Aws::S3::S3Client *s3_client) = 0;
    virtual void close() = 0;

    int32_t id();
    void create_column_mapping(TupleDesc tupleDesc, const std::set<int> &attrs_used);
    void set_rowgroups_list(const std::vector<int> &rowgroups);
    void set_options(bool use_threads, bool use_mmap);
    void set_coordinator(ParallelCoordinator *coord);
};

ParquetReader *create_parquet_reader(const char *filename,
                                     MemoryContext cxt,
                                     int reader_id = -1,
                                     bool caching = false);

#endif
