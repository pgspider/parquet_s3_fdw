/*-------------------------------------------------------------------------
 *
 * exec_state.cpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/exec_state.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "exec_state.hpp"
#include "heap.hpp"

#include <sys/time.h>
#include <functional>
#include <list>


#if PG_VERSION_NUM < 110000
#define MakeTupleTableSlotCompat(tupleDesc) MakeSingleTupleTableSlot(tupleDesc)
#elif PG_VERSION_NUM < 120000
#define MakeTupleTableSlotCompat(tupleDesc) MakeTupleTableSlot(tupleDesc)
#else
#define MakeTupleTableSlotCompat(tupleDesc) MakeTupleTableSlot(tupleDesc, &TTSOpsVirtual)
#endif

/*
 * More compact form of common PG_TRY/PG_CATCH block which throws a c++
 * exception in case of errors.
 */
#define PG_TRY_INLINE(code_block, err) \
    do { \
        bool error = false; \
        PG_TRY(); \
        code_block \
        PG_CATCH(); { error = true; } \
        PG_END_TRY(); \
        if (error) { throw std::runtime_error(err); } \
    } while(0)

class SingleFileExecutionStateS3 : public ParquetS3FdwExecutionState
{
private:
    ParquetReader      *reader;
    MemoryContext       cxt;
    TupleDesc           tuple_desc;
    std::set<int>       attrs_used;
    bool                use_mmap;
    bool                use_threads;
    const char *dirname;
    Aws::S3::S3Client *s3_client;

public:
    MemoryContext       estate_cxt;

    SingleFileExecutionStateS3(MemoryContext cxt,
                             const char *dirname,
                             Aws::S3::S3Client *s3_client,
                             TupleDesc tuple_desc,
                             std::set<int> attrs_used,
                             bool use_threads,
                             bool use_mmap)
        : cxt(cxt), tuple_desc(tuple_desc), attrs_used(attrs_used),
          use_mmap(use_mmap), use_threads(use_threads),
          dirname(dirname), s3_client(s3_client)
    { }

    ~SingleFileExecutionStateS3()
    {
        if (reader)
            delete reader;
    }

    bool next(TupleTableSlot *slot, bool fake)
    {
        bool res;

        if ((res = reader->next(slot, fake)) == RS_SUCCESS)
            ExecStoreVirtualTuple(slot);

        return res;
    }

    void rescan(void)
    {
        reader->rescan();
    }

    void add_file(const char *filename, List *rowgroups)
    {
        ListCell           *lc;
        std::vector<int>    rg;

        foreach (lc, rowgroups)
            rg.push_back(lfirst_int(lc));

        reader = create_parquet_reader(filename, cxt);
        reader->set_options(use_threads, use_mmap);
        reader->set_rowgroups_list(rg);
        if (s3_client)
            reader->open(dirname, s3_client);
        else
            reader->open();
        reader->create_column_mapping(tuple_desc, attrs_used);
    }

    void set_coordinator(ParallelCoordinator *coord)
    {
        if (reader)
            reader->set_coordinator(coord);
    }
};

class MultifileExecutionStateS3 : public ParquetS3FdwExecutionState
{
private:
    struct FileRowgroups
    {
        std::string         filename;
        std::vector<int>    rowgroups;
    };
private:
    ParquetReader       *reader;

    std::vector<FileRowgroups> files;
    uint64_t                cur_reader;

    MemoryContext           cxt;
    TupleDesc               tuple_desc;
    std::set<int>           attrs_used;
    bool                    use_threads;
    bool                    use_mmap;

    ParallelCoordinator    *coord;
    const char *dirname;
    Aws::S3::S3Client *s3_client;

private:
    ParquetReader *get_next_reader()
    {
        ParquetReader *r;

        if (coord)
        {
            int32 reader_id;

            SpinLockAcquire(&coord->lock);

            /* 
             * First let's check if the file other workers are reading has more
             * rowgroups to read
             */
            reader_id = coord->next_reader - 1;
            if (reader_id >= 0 && reader_id < (int) files.size()
                && (int) files[reader_id].rowgroups.size() > coord->next_rowgroup) {
                /* yep */;
            } else {
                /* If that's not the case then open the next file */
                reader_id = coord->next_reader++;
                coord->next_rowgroup = 0;
            }
            this->cur_reader = reader_id;
            SpinLockRelease(&coord->lock);
        }

        if (cur_reader >= files.size())
            return NULL;

        r = create_parquet_reader(files[cur_reader].filename.c_str(), cxt, cur_reader);
        r->set_rowgroups_list(files[cur_reader].rowgroups);
        r->set_options(use_threads, use_mmap);
        r->set_coordinator(coord);
        if (s3_client)
            r->open(dirname, s3_client);
        else
            r->open();
        r->create_column_mapping(tuple_desc, attrs_used);

        cur_reader++;

        return r;
    }

public:
    MultifileExecutionStateS3(MemoryContext cxt,
                            const char *dirname,
                            Aws::S3::S3Client *s3_client,
                            TupleDesc tuple_desc,
                            std::set<int> attrs_used,
                            bool use_threads,
                            bool use_mmap)
        : reader(NULL), cur_reader(0), cxt(cxt), tuple_desc(tuple_desc),
          attrs_used(attrs_used), use_threads(use_threads), use_mmap(use_mmap),
          coord(NULL), dirname(dirname), s3_client(s3_client)
    { }

    ~MultifileExecutionStateS3()
    {
        if (reader)
            delete reader;
    }

    bool next(TupleTableSlot *slot, bool fake=false)
    {
        ReadStatus  res;

        if (unlikely(reader == NULL))
        {
            if ((reader = this->get_next_reader()) == NULL)
                return false;
        }

        res = reader->next(slot, fake);

        /* Finished reading current reader? Proceed to the next one */
        if (unlikely(res != RS_SUCCESS))
        {
            while (true)
            {
                if (reader)
                    delete reader;

                reader = this->get_next_reader();
                if (!reader)
                    return false;
                res = reader->next(slot, fake);
                if (res == RS_SUCCESS)
                    break;
            }
        }

        if (res == RS_SUCCESS)
        {
            /*
             * ExecStoreVirtualTuple doesn't throw postgres exceptions thus no
             * need to wrap it into PG_TRY / PG_CATCH
             */
            ExecStoreVirtualTuple(slot);
        }

        return res;
    }

    void rescan(void)
    {
        reader->rescan();
    }

    void add_file(const char *filename, List *rowgroups)
    {
        FileRowgroups   fr;
        ListCell       *lc;

        fr.filename = filename;
        foreach (lc, rowgroups)
            fr.rowgroups.push_back(lfirst_int(lc));
        files.push_back(fr);
    }

    void set_coordinator(ParallelCoordinator *coord)
    {
        this->coord = coord;
    }
};

class MultifileMergeExecutionStateBaseS3 : public ParquetS3FdwExecutionState
{
protected:
    struct ReaderSlot
    {
        int             reader_id;
        TupleTableSlot *slot;
    };

protected:
    std::vector<ParquetReader *> readers;

    MemoryContext       cxt;
    TupleDesc           tuple_desc;
    std::set<int>       attrs_used;
    std::list<SortSupportData> sort_keys;
    bool                use_threads;
    bool                use_mmap;

    /*
     * Heap is used to store tuples in prioritized manner along with file
     * number. Priority is given to the tuples with minimal key. Once next
     * tuple is requested it is being taken from the top of the heap and a new
     * tuple from the same file is read and inserted back into the heap. Then
     * heap is rebuilt to sustain its properties. The idea is taken from
     * nodeGatherMerge.c in PostgreSQL but reimplemented using STL.
     */
    Heap<ReaderSlot>    slots;
    bool                slots_initialized;
    const char         *dirname;
    Aws::S3::S3Client  *s3_client;

protected:
    /*
     * compare_slots
     *      Compares two slots according to sort keys. Returns true if a > b,
     *      false otherwise. The function is stolen from nodeGatherMerge.c
     *      (postgres) and adapted.
     */
    bool compare_slots(const ReaderSlot &a, const ReaderSlot &b)
    {
        TupleTableSlot *s1 = a.slot;
        TupleTableSlot *s2 = b.slot;

        Assert(!TupIsNull(s1));
        Assert(!TupIsNull(s2));

        for (auto sort_key: sort_keys)
        {
            AttrNumber  attno = sort_key.ssup_attno;
            Datum       datum1,
                        datum2;
            bool        isNull1,
                        isNull2;
            int         compare;

            datum1 = slot_getattr(s1, attno, &isNull1);
            datum2 = slot_getattr(s2, attno, &isNull2);

            compare = ApplySortComparator(datum1, isNull1,
                                          datum2, isNull2,
                                          &sort_key);
            if (compare != 0)
                return (compare > 0);
        }

        return false;
    }
};

class MultifileMergeExecutionStateS3 : public MultifileMergeExecutionStateBaseS3
{
private:
    /*
     * initialize_slots
     *      Initialize slots binary heap on the first run.
     */
    void initialize_slots()
    {
        std::function<bool(const ReaderSlot &, const ReaderSlot &)> cmp =
            [this] (const ReaderSlot &a, const ReaderSlot &b) { return compare_slots(a, b); };
        int i = 0;

        slots.init(readers.size(), cmp);
        for (auto reader: readers)
        {
            ReaderSlot    rs;

            PG_TRY_INLINE(
                {
                    MemoryContext oldcxt;

                    oldcxt = MemoryContextSwitchTo(cxt);
                    rs.slot = MakeTupleTableSlotCompat(tuple_desc);
                    MemoryContextSwitchTo(oldcxt);
                }, "failed to create a TupleTableSlot"
            );

            if (reader->next(rs.slot) == RS_SUCCESS)
            {
                ExecStoreVirtualTuple(rs.slot);
                rs.reader_id = i;
                slots.append(rs);
            }
            ++i;
        }
        PG_TRY_INLINE({ slots.heapify(); }, "heapify failed");
        slots_initialized = true;
    }

public:
    MultifileMergeExecutionStateS3(MemoryContext cxt,
                                 const char *dirname,
                                 Aws::S3::S3Client *s3_client,
                                 TupleDesc tuple_desc,
                                 std::set<int> attrs_used,
                                 std::list<SortSupportData> sort_keys,
                                 bool use_threads,
                                 bool use_mmap)
    {
        this->cxt = cxt;
        this->tuple_desc = tuple_desc;
        this->dirname = dirname;
        this->s3_client = s3_client;
        this->attrs_used = attrs_used;
        this->sort_keys = sort_keys;
        this->use_threads = use_threads;
        this->use_mmap = use_mmap;
        this->slots_initialized = false;
    }

    ~MultifileMergeExecutionStateS3()
    {
#if PG_VERSION_NUM < 110000
        /* Destroy tuple slots if any */
        for (int i = 0; i < slots.size(); i++)
            ExecDropSingleTupleTableSlot(slots[i].slot);
#endif

        for (auto it: readers)
            delete it;
    }

    bool next(TupleTableSlot *slot, bool /* fake=false */)
    {
        if (unlikely(!slots_initialized))
            initialize_slots();

        if (unlikely(slots.empty()))
            return false;

        /* Copy slot with the smallest key into the resulting slot */
        const ReaderSlot &head = slots.head();
        PG_TRY_INLINE(
            {
                ExecCopySlot(slot, head.slot);
                ExecClearTuple(head.slot);
            }, "failed to copy a virtual tuple slot"
        );

        /*
         * Try to read another record from the same reader as in the head slot.
         * In case of success the new record makes it into the heap and the
         * heap gets reheapified. Else if there are no more records in the
         * reader then current head is removed from the heap and heap gets
         * reheapified.
         */
        if (readers[head.reader_id]->next(head.slot) == RS_SUCCESS)
        {
            ExecStoreVirtualTuple(head.slot);
            PG_TRY_INLINE({ slots.heapify_head(); }, "heapify failed");
        }
        else
        {
#if PG_VERSION_NUM < 110000
            /* Release slot resources */
            PG_TRY_INLINE(
                {
                    ExecDropSingleTupleTableSlot(head.slot);
                }, "failed to drop a tuple slot"
            );
#endif
            slots.pop();
        }
        return true;
    }

    void rescan(void)
    {
        /* TODO: clean binheap */
        for (auto reader: readers)
            reader->rescan();
        slots.clear();
        slots_initialized = false;
    }

    void add_file(const char *filename, List *rowgroups)
    {
        ParquetReader *r;
        ListCell           *lc;
        std::vector<int>    rg;

        foreach (lc, rowgroups)
            rg.push_back(lfirst_int(lc));

        r = create_parquet_reader(filename, cxt);
        r->set_rowgroups_list(rg);
        r->set_options(use_threads, use_mmap);
        if (s3_client)
            r->open(dirname, s3_client);
        else
            r->open();
        r->create_column_mapping(tuple_desc, attrs_used);
        readers.push_back(r);
    }

    void set_coordinator(ParallelCoordinator * /* coord */)
    {
        Assert(true);   /* not supported, should never happen */
    }
};

/*
 * CachingMultifileMergeExecutionStateS3
 *      This is a specialized version of MultifileMergeExecutionState that is
 *      capable of merging large amount of files without keeping all of them
 *      open at the same time. For that it utilizes CachingParquetReader which
 *      stores all read data in the internal buffers.
 */
class CachingMultifileMergeExecutionStateS3 : public MultifileMergeExecutionStateBaseS3
{
private:
    /* Per-reader activation timestamps */
    std::vector<uint64_t>   ts_active;

    int                     num_active_readers;

    int                     max_open_files;

private:
    /*
     * initialize_slots
     *      Initialize slots binary heap on the first run.
     */
    void initialize_slots()
    {
        std::function<bool(const ReaderSlot &, const ReaderSlot &)> cmp =
            [this] (const ReaderSlot &a, const ReaderSlot &b) { return compare_slots(a, b); };
        int i = 0;

        this->ts_active.resize(readers.size(), 0);

        slots.init(readers.size(), cmp);
        for (auto reader: readers)
        {
            ReaderSlot    rs;

            PG_TRY_INLINE(
                {
                    MemoryContext oldcxt;

                    oldcxt = MemoryContextSwitchTo(cxt);
                    rs.slot = MakeTupleTableSlotCompat(tuple_desc);
                    MemoryContextSwitchTo(oldcxt);
                }, "failed to create a TupleTableSlot"
            );

            activate_reader(reader);
            reader->create_column_mapping(tuple_desc, attrs_used);

            if (reader->next(rs.slot) == RS_SUCCESS)
            {
                ExecStoreVirtualTuple(rs.slot);
                rs.reader_id = i;
                slots.append(rs);
            }
            ++i;
        }
        PG_TRY_INLINE({ slots.heapify(); }, "heapify failed");
        slots_initialized = true;
    }

    /*
     * activate_reader
     *      Opens reader if it's not already active. If the number of active
     *      readers exceeds the limit, function closes the least recently used
     *      one.
     */
    ParquetReader *activate_reader(ParquetReader *reader)
    {
        struct timeval tv;

        Assert(readers.size() > 0);

        /* If reader's already active then we're done here */
        if (ts_active[reader->id()] > 0)
            return reader;

        /* Does the number of active readers exceeds limit? */
        if (max_open_files > 0 && num_active_readers >= max_open_files)
        {
            uint64_t    ts_min = -1;  /* initialize with max uint64_t */
            int         idx_min = -1;

            /* Find the least recently used reader */
            for (std::vector<ParquetReader *>::size_type i = 0; i < readers.size(); ++i) {
                if (ts_active[i] > 0 && ts_active[i] < ts_min) {
                    ts_min = ts_active[i];
                    idx_min = i;
                }
            }

            if (idx_min < 0)
                throw std::runtime_error("failed to find a reader to deactivate");
            readers[idx_min]->close();
            ts_active[idx_min] = 0;
            num_active_readers--;
        }

        /* Reopen the reader and update timestamp */
        gettimeofday(&tv, NULL);
        ts_active[reader->id()] = tv.tv_sec*1000LL + tv.tv_usec/1000;
        if (s3_client)
            reader->open(dirname, s3_client);
        else
            reader->open();
        num_active_readers++;

        return reader;
    }

public:
    CachingMultifileMergeExecutionStateS3(MemoryContext cxt,
                                        const char *dirname,
                                        Aws::S3::S3Client *s3_client,
                                        TupleDesc tuple_desc,
                                        std::set<int> attrs_used,
                                        std::list<SortSupportData> sort_keys,
                                        bool use_threads,
                                        bool use_mmap,
                                        int max_open_files)
        : num_active_readers(0), max_open_files(max_open_files)
    {
        this->cxt = cxt;
        this->dirname = dirname;
        this->s3_client = s3_client;
        this->tuple_desc = tuple_desc;
        this->attrs_used = attrs_used;
        this->sort_keys = sort_keys;
        this->use_threads = use_threads;
        this->use_mmap = use_mmap;
        this->slots_initialized = false;
    }

    ~CachingMultifileMergeExecutionStateS3()
    {
#if PG_VERSION_NUM < 110000
        /* Destroy tuple slots if any */
        for (int i = 0; i < slots.size(); i++)
            ExecDropSingleTupleTableSlot(slots[i].slot);
#endif

        for (auto it: readers)
            delete it;
    }

    bool next(TupleTableSlot *slot, bool /* fake=false */)
    {
        if (unlikely(!slots_initialized))
            initialize_slots();

        if (unlikely(slots.empty()))
            return false;

        /* Copy slot with the smallest key into the resulting slot */
        const ReaderSlot &head = slots.head();
        PG_TRY_INLINE(
            {
                ExecCopySlot(slot, head.slot);
                ExecClearTuple(head.slot);
            }, "failed to copy a virtual tuple slot"
        );

        /*
         * Try to read another record from the same reader as in the head slot.
         * In case of success the new record makes it into the heap and the
         * heap gets reheapified. If next() returns RS_INACTIVE try to reopen
         * reader and retry. If there are no more records in the reader then
         * current head is removed from the heap and heap gets reheapified.
         */
        while (true) {
            ReadStatus status = readers[head.reader_id]->next(head.slot);

            switch(status)
            {
                case RS_SUCCESS:
                    ExecStoreVirtualTuple(head.slot);
                    PG_TRY_INLINE({ slots.heapify_head(); }, "heapify failed");
                    return true;

                case RS_INACTIVE:
                    /* Reactivate reader and retry */
                    activate_reader(readers[head.reader_id]);
                    break;

                case RS_EOF:
#if PG_VERSION_NUM < 110000
                    /* Release slot resources */
                    PG_TRY_INLINE(
                        {
                            ExecDropSingleTupleTableSlot(head.slot);
                        }, "failed to drop a tuple slot"
                    );
#endif
                    slots.pop();
                    return true;
            }
        }
    }

    void rescan(void)
    {
        /* TODO: clean binheap */
        for (auto reader: readers)
            reader->rescan();
        slots.clear();
        slots_initialized = false;
    }

    void add_file(const char *filename, List *rowgroups)
    {
        ParquetReader      *r;
        ListCell           *lc;
        std::vector<int>    rg;
        int32_t             reader_id = readers.size();

        foreach (lc, rowgroups)
            rg.push_back(lfirst_int(lc));

        r = create_parquet_reader(filename, cxt, reader_id, true);
        r->set_rowgroups_list(rg);
        r->set_options(use_threads, use_mmap);
        readers.push_back(r);
    }

    void set_coordinator(ParallelCoordinator * /* coord */)
    {
        Assert(true);   /* not supported, should never happen */
    }
};

ParquetS3FdwExecutionState *create_parquet_execution_state(ReaderType reader_type,
                                                         MemoryContext reader_cxt,
                                                         const char *dirname,
                                                         Aws::S3::S3Client *s3_client,
                                                         TupleDesc tuple_desc,
                                                         std::set<int> &attrs_used,
                                                         std::list<SortSupportData> sort_keys,
                                                         bool use_threads,
                                                         bool use_mmap,
                                                         int32_t max_open_files)
{
    switch (reader_type)
    {
        case RT_SINGLE:
            return new SingleFileExecutionStateS3(reader_cxt, dirname, s3_client, tuple_desc,
                                                         attrs_used, use_threads,
                                                         use_mmap);
        case RT_MULTI:
            return new MultifileExecutionStateS3(reader_cxt, dirname, s3_client, tuple_desc,
                                                        attrs_used, use_threads,
                                                        use_mmap);
        case RT_MULTI_MERGE:
            return new MultifileMergeExecutionStateS3(reader_cxt, dirname, s3_client, tuple_desc,
                                                             attrs_used, sort_keys, 
                                                             use_threads, use_mmap);
        case RT_CACHING_MULTI_MERGE:
            return new CachingMultifileMergeExecutionStateS3(reader_cxt, dirname, s3_client, tuple_desc,
                                                           attrs_used, sort_keys, 
                                                           use_threads, use_mmap,
                                                           max_open_files);
        default:
            throw std::runtime_error("unknown reader type");
    }
}
