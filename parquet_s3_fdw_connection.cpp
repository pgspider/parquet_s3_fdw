/*-------------------------------------------------------------------------
 *
 * parquet_s3_fdw_connection.c
 *		  Connection management functions for parquet_s3_fdw
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/parquet_s3_fdw_connection.c
 *
 *-------------------------------------------------------------------------
 */
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <dirent.h>
#include <sys/stat.h>
#include "parquet_s3_fdw.hpp"

extern "C"
{
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/latch.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
}

static Aws::SDKOptions *aws_sdk_options;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *FileReaderHash = NULL;

struct Error : std::exception
{
    char text[1000];

    Error(char const* fmt, ...) __attribute__((format(printf,2,3))) {
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(text, sizeof text, fmt, ap);
        va_end(ap);
    }

    char const* what() const throw() { return text; }
};

/*
 * It seems that "Aws::Client::ClientConfiguration clientConfig;" is not
 * thread safe. We use the mutex for it because PGSpider accesses this
 * FDW in parallel.
 */
pthread_mutex_t cred_mtx = PTHREAD_MUTEX_INITIALIZER;

/*
 * Connection cache hash table entry
 *
 * The lookup key in this hash table is the user mapping OID. We use just one
 * connection per user mapping ID, which ensures that all the scans use the
 * same snapshot during a query.  Using the user mapping OID rather than
 * the foreign server OID + user OID avoids creating multiple connections when
 * the public user mapping applies to all user OIDs.
 *
 * The "conn" pointer can be NULL if we don't currently have a live connection.
 * When we do have a connection, xact_depth tracks the current depth of
 * transactions and subtransactions open on the remote side.  We need to issue
 * commands at the same nesting depth on the remote as we're executing at
 * ourselves, so that rolling back a subtransaction will kill the right
 * queries and not the wrong ones.
 */
typedef Oid ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	Aws::S3::S3Client *conn;			/* connection to foreign server, or NULL */
	/* Remaining fields are invalid when conn is NULL: */
	bool		have_error;		/* have any subxacts aborted in this xact? */
	bool		invalidated;	/* true if reconnect is pending */
	uint32		server_hashvalue;	/* hash value of foreign server OID */
	uint32		mapping_hashvalue;	/* hash value of user mapping OID */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

/* prototypes of private functions */
static Aws::S3::S3Client *create_s3_connection(ForeignServer *server, UserMapping *user, bool use_minio);
static void close_s3_connection(ConnCacheEntry *entry);
static void check_conn_params(const char **keywords, const char **values, UserMapping *user);
static void parquet_fdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue);
static Aws::S3::S3Client* s3_client_open(const char *user, const char *password, bool use_minio);
static void s3_client_close(Aws::S3::S3Client *s3_client);

extern "C" void
parquet_s3_init()
{
	aws_sdk_options = new Aws::SDKOptions();
	Aws::InitAPI(*aws_sdk_options);
}

extern "C" void
parquet_s3_shutdown()
{
	Aws::ShutdownAPI(*aws_sdk_options);
    aws_sdk_options = NULL;
}

/*
 * Get a S3 handle which can be used to get objects on AWS S3
 * with the user's authorization.  A new connection is established
 * if we don't already have a suitable one.
 */
Aws::S3::S3Client *
parquetGetConnection(UserMapping *user, bool use_minio)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	/* First time through, initialize connection cache hashtable */
	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("parquet_fdw connections", 8,
									 &ctl,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
									  parquet_fdw_inval_callback, (Datum) 0);
		CacheRegisterSyscacheCallback(USERMAPPINGOID,
									  parquet_fdw_inval_callback, (Datum) 0);
	}

	/* Create hash key for the entry.  Assume no pad bytes in key struct */
	key = user->umid;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = (ConnCacheEntry *) hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/*
		 * We need only clear "conn" here; remaining fields will be filled
		 * later when "conn" is set.
		 */
		entry->conn = NULL;
	}

	/*
	 * If the connection needs to be remade due to invalidation, disconnect as
	 * soon as we're out of all transactions.
	 */
	if (entry->conn != NULL && entry->invalidated)
	{
		elog(DEBUG3, "closing handle %p for option changes to take effect",
			 entry->conn);
		close_s3_connection(entry);
	}

	/*
	 * We don't check the health of cached connection here, because it would
	 * require some overhead.  Broken connection will be detected when the
	 * connection is actually used.
	 */

	/*
	 * If cache entry doesn't have a connection, we have to establish a new
	 * connection.  (If connect_ps3 throws an error, the cache entry
	 * will remain in a valid empty state, ie conn == NULL.)
	 */
	if (entry->conn == NULL)
	{
		ForeignServer *server = GetForeignServer(user->serverid);

		/* Reset all transient state fields, to be sure all are clean */
		entry->have_error = false;
		entry->invalidated = false;
		entry->server_hashvalue =
			GetSysCacheHashValue1(FOREIGNSERVEROID,
								  ObjectIdGetDatum(server->serverid));
		entry->mapping_hashvalue =
			GetSysCacheHashValue1(USERMAPPINGOID,
								  ObjectIdGetDatum(user->umid));

		/* Now try to make the handle */
		entry->conn = create_s3_connection(server, user, use_minio);

		elog(DEBUG3, "new parquet_fdw handle %p for server \"%s\" (user mapping oid %u, userid %u)",
			 entry->conn, server->servername, user->umid, user->userid);
	}

	return entry->conn;
}

/*
 * Generate key-value arrays from the given list. Caller must have
 * allocated large-enough arrays.  Returns number of options found.
 */
static int
ExtractConnectionOptions(List *defelems, const char **keywords,
						 const char **values)
{
	ListCell   *lc;
	int			i;

	i = 0;
	foreach(lc, defelems)
	{
		DefElem    *d = (DefElem *) lfirst(lc);

		keywords[i] = d->defname;
		values[i] = defGetString(d);
		i++;
	}
	return i;
}

/*
 * Connect to remote server using specified server and user mapping properties.
 */
static Aws::S3::S3Client *
create_s3_connection(ForeignServer *server, UserMapping *user, bool use_minio)
{
	Aws::S3::S3Client	   *volatile conn = NULL;

	/*
	 * Use PG_TRY block to ensure closing connection on error.
	 */
	PG_TRY();
	{
		const char **keywords;
		const char **values;
		int			n;
		char *id = NULL;
		char *password = NULL;
		ListCell   *lc;

		n = list_length(user->options) + 1;
		keywords = (const char **) palloc(n * sizeof(char *));
		values = (const char **) palloc(n * sizeof(char *));

		n = ExtractConnectionOptions(user->options,
									  keywords, values);
		keywords[n] = values[n] = NULL;

		/* verify connection parameters and make connection */
		check_conn_params(keywords, values, user);

		/* get id and password from user option */
		foreach(lc, user->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "user") == 0)
				id = defGetString(def);

			if (strcmp(def->defname, "password") == 0)
				password = defGetString(def);
		}

		conn = s3_client_open(id, password, use_minio);
		if (!conn)
			ereport(ERROR,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("could not connect to S3 \"%s\"",
							server->servername)));

		pfree(keywords);
		pfree(values);
	}
	PG_CATCH();
	{
		/* Close S3 handle if we managed to create one */
		if (conn)
			s3_client_close(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return conn;
}

/*
 * Close any open handle for a connection cache entry.
 */
static void
close_s3_connection(ConnCacheEntry *entry)
{
	if (entry->conn != NULL)
	{
		s3_client_close(entry->conn);
		entry->conn = NULL;
	}
}

/*
 * Password is required to connect to S3.
 */
static void
check_conn_params(const char **keywords, const char **values, UserMapping *user)
{
	int			i;

	/* ok if params contain a non-empty password */
	for (i = 0; keywords[i] != NULL; i++)
	{
		if (strcmp(keywords[i], "password") == 0 && values[i][0] != '\0')
			return;
	}

	ereport(ERROR,
			(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
			 errmsg("password is required"),
			 errdetail("Non-superusers must provide a password in the user mapping.")));
}

/*
 * Release connection reference count created by calling GetConnection.
 */
void
parquetReleaseConnection(Aws::S3::S3Client *conn)
{
	/*
	 * Currently, we don't actually track connection references because all
	 * cleanup is managed on a transaction or subtransaction basis instead. So
	 * there's nothing to do here.
	 */
}

/*
 * Connection invalidation callback function
 *
 * After a change to a pg_foreign_server or pg_user_mapping catalog entry,
 * mark connections depending on that entry as needing to be remade.
 * We can't immediately destroy them, since they might be in the midst of
 * a transaction, but we'll remake them at the next opportunity.
 *
 * Although most cache invalidation callbacks blow away all the related stuff
 * regardless of the given hashvalue, connections are expensive enough that
 * it's worth trying to avoid that.
 *
 * NB: We could avoid unnecessary disconnection more strictly by examining
 * individual option values, but it seems too much effort for the gain.
 */
static void
parquet_fdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

	/* ConnectionHash must exist already, if we're registered */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore invalid entries */
		if (entry->conn == NULL)
			continue;

		/* hashvalue == 0 means a cache reset, must clear all state */
		if (hashvalue == 0 ||
			(cacheid == FOREIGNSERVEROID &&
			 entry->server_hashvalue == hashvalue) ||
			(cacheid == USERMAPPINGOID &&
			 entry->mapping_hashvalue == hashvalue))
			entry->invalidated = true;
	}
}

/*
 * Create S3 handle.
 */
static Aws::S3::S3Client*
s3_client_open(const char *user, const char *password, bool use_minio)
{
    const Aws::String access_key_id = user;
    const Aws::String secret_access_key = password;
	Aws::Auth::AWSCredentials cred = Aws::Auth::AWSCredentials(access_key_id, secret_access_key);
	Aws::S3::S3Client *s3_client;

	pthread_mutex_lock(&cred_mtx);
	Aws::Client::ClientConfiguration clientConfig;
	pthread_mutex_unlock(&cred_mtx);

	if (use_minio)
	{
		const Aws::String endpoint = "127.0.0.1:9000";
		clientConfig.scheme = Aws::Http::Scheme::HTTP;
		clientConfig.endpointOverride = endpoint;
		s3_client = new Aws::S3::S3Client(cred, clientConfig,
				Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
	}
	else
	{
		clientConfig.scheme = Aws::Http::Scheme::HTTPS;
		clientConfig.region = Aws::Region::AP_NORTHEAST_1;
		s3_client = new Aws::S3::S3Client(cred, clientConfig);
	}

	return s3_client;
}

/*
 * Close S3 handle.
 */
static void
s3_client_close(Aws::S3::S3Client *s3_client)
{
	delete s3_client;
}

/*
 * Get S3 handle by foreign table id from connection cache.
 */
Aws::S3::S3Client*
parquetGetConnectionByTableid(Oid foreigntableid)
{
    Aws::S3::S3Client *s3client = NULL;

    if (foreigntableid != 0)
    {
        ForeignTable  *ftable = GetForeignTable(foreigntableid);
        ForeignServer *fserver = GetForeignServer(ftable->serverid);
        UserMapping   *user = GetUserMapping(GetUserId(), fserver->serverid);
        parquet_s3_server_opt *options = parquet_s3_get_options(foreigntableid);

        s3client = parquetGetConnection(user, options->use_minio);
    }
    return s3client;
}

/*
 * Get file names in S3 directory. Retuned file names are path from s3path.
 */
List*
parquetGetS3ObjectList(Aws::S3::S3Client *s3_cli, const char *s3path)
{
    List *objectlist = NIL;
	Aws::S3::S3Client s3_client = *s3_cli;
	Aws::S3::Model::ListObjectsRequest request;

    if (s3path == NULL)
        return NIL;

    /* Calculate bucket name and directory name from S3 path. */
    const char *bucket = s3path + 5; /* Remove "s3://" */
    const char *dir = strchr(bucket, '/'); /* Search the 1st '/' after "s3://". */
    const Aws::String& bucketName = bucket;
    size_t len;
    if (dir)
    {
        len = dir - bucket;
        dir++; /* Remove '/' */
    }
    else
    {
        len = bucketName.length();
    }
    request.WithBucket(bucketName.substr(0, len));
    
	auto outcome = s3_client.ListObjects(request);

	if (!outcome.IsSuccess())
		elog(ERROR, "parquet_fdw: failed to get object list on %s. %s", bucketName.substr(0, len).c_str(), outcome.GetError().GetMessage().c_str());

	Aws::Vector<Aws::S3::Model::Object> objects =
		outcome.GetResult().GetContents();
	for (Aws::S3::Model::Object& object : objects)
	{
        Aws::String key = object.GetKey();
        if (!dir)
        {
		    objectlist = lappend(objectlist, makeString(pstrdup((char*)key.c_str())));
            elog(DEBUG1, "parquet_fdw: accessing %s%s", s3path, key.c_str());
        }
        else if (strncmp(key.c_str(), dir, strlen(dir)) == 0)
        {
            char *file = pstrdup((char*) key.substr(strlen(dir)).c_str());
            /* Don't register if the object is directory. */
            if (key.at(key.length()-1) != '/' && strcmp(file, "/") != 0)
            {
                objectlist = lappend(objectlist, makeString(file));
                elog(DEBUG1, "parquet_fdw: accessing %s%s", s3path, key.substr(strlen(dir)).c_str());
            }
			else
				pfree(file);
        }
        else
            elog(DEBUG1, "parquet_fdw: skipping s3://%s/%s", bucketName.substr(0, len).c_str(), key.c_str());
	}

	return objectlist;
}

/*
 * Check if the file name can be aceptable.
 * Either local file or S3 file can be specified at a time. It cannot be mixed.
 * The 2nd argument 'loc' indicates which location is used currently.
 */
FileLocation
parquetFilenamesValidator(const char *filename, FileLocation loc)
{
    if (IS_S3_PATH(filename))
    {
        if (loc == LOC_LOCAL)
            elog(ERROR, "Cannot specify the mix of local file and S3 file");
        return LOC_S3;
    }
    else
    {
        if (loc == LOC_S3)
            elog(ERROR, "Cannot specify the mix of local file and S3 file");
        return LOC_LOCAL;
    }
}

/*
 * Return true if the 1st filename in the list is S3's URL.
 */
bool
parquetIsS3Filenames(List *filenames)
{
    char *name;

    if (filenames == NIL)
        return false;

    name = strVal(list_nth(filenames, 0));
    return IS_S3_PATH(name);
}

/*
 * Split s3 path into bucket name and file path.
 * If foreign table option 'dirname' is specified, dirname starts by 
 * "s3://". And filename is already set by get_filenames_in_dir().
 * On the other hand, if foreign table option 'filename' is specified,
 * dirname is NULL and filename is set as fullpath started by "s3://".
 */
void
parquetSplitS3Path(const char *dirname, const char *filename, char **bucket, char **filepath)
{
    if (dirname)
    {
        *bucket = pstrdup(dirname + 5); /* Remove "s3://" */
        Assert (filename);
        *filepath = pstrdup(filename);
    }
    else
    {
        char *copied = pstrdup(filename + 5); /* Remove "s3://" */
        char *sep;
        *bucket = copied;
        sep = strchr(copied, '/');
        *sep = '\0';
        *filepath = pstrdup(sep + 1);
    }
}

/*
 * Get file names in local directory.
 */
List*
parquetGetDirFileList(List *filelist, const char *path)
{
    int ret;
    struct stat st;
    DIR *dp;
    struct dirent *entry;

    ret = stat(path, &st);
    if (ret != 0)
        elog(ERROR, "parquet_fdw: cannot stat %s", path);
    
    if ((st.st_mode & S_IFMT) == S_IFREG)
    {
        filelist = lappend(filelist, makeString(pstrdup(path)));
        elog(DEBUG1, "parquet_fdw: file = %s", path);
        return filelist;
    }

    /* Do nothing if it is not file and not directory. */
    if ((st.st_mode & S_IFMT) != S_IFDIR)
        return filelist;

    dp = opendir(path);
    if (!dp)
        elog(ERROR, "parquet_fdw: cannot open %s", path);

    entry = readdir(dp);
    while (entry != NULL) {
        char *newpath;
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0)
        {
            newpath = psprintf("%s/%s", path, entry->d_name);
            filelist = parquetGetDirFileList(filelist, newpath);
            pfree(newpath);
        }
        entry = readdir(dp);
    }
    closedir(dp);

	return filelist;
}

/*
 * Implementation of ImportForeignSchema for S3 URL.
 */
List *
parquetImportForeignSchemaS3(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    List           *cmds = NIL;
    Aws::S3::S3Client *s3client; 
    List *objects;
    ListCell *cell;

    ForeignServer *fserver = GetForeignServer(serverOid);
    UserMapping   *user = GetUserMapping(GetUserId(), fserver->serverid);
    parquet_s3_server_opt *options = parquet_s3_get_server_options(serverOid);
    s3client = parquetGetConnection(user, options->use_minio);

    objects = parquetGetS3ObjectList(s3client, stmt->remote_schema);

    foreach(cell, objects)
    {
        ListCell   *lc;
        bool        skip = false;
        List       *fields;
        char       *path = strVal((Value *) lfirst(cell));
        char       *filename = pstrdup(path);
        char       *query;
        char       *fullpath;

        char *ext = strrchr(filename, '.');

        if (ext && strcmp(ext + 1, "parquet") != 0)
            continue;

        if (filename[0] == '/')
            filename++;

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

        fields = extract_parquet_fields(path, stmt->remote_schema, s3client);
        if (stmt->remote_schema[strlen(stmt->remote_schema)-1] == '/')
            fullpath = psprintf("%s%s", stmt->remote_schema, path);
        else
            fullpath = psprintf("%s/%s", stmt->remote_schema, path);
        query = create_foreign_table_query(filename, stmt->local_schema,
                                           stmt->server_name, &fullpath, 1,
                                           fields, stmt->options);
        cmds = lappend(cmds, query);
        elog(DEBUG1, "parquet_fdw: %s", query);
    }

    return cmds;
}

/*
 * Wrapper of extract_parquet_fields(). If the path is S3 URL, we give s3 handle to
 * extract_parquet_fields().
 */
List *
parquetExtractParquetFields(List *fields, char **paths, const char *servername) noexcept
{
    if (!fields)
    {
        if (IS_S3_PATH(paths[0]))
        {                
            ForeignServer *fserver = GetForeignServerByName(servername, false);
            UserMapping   *user = GetUserMapping(GetUserId(), fserver->serverid);
            parquet_s3_server_opt *options = parquet_s3_get_server_options(fserver->serverid);
            Aws::S3::S3Client *s3client = parquetGetConnection(user, options->use_minio);

            fields = extract_parquet_fields(paths[0], NULL, s3client);
        }
        else
            fields = extract_parquet_fields(paths[0], NULL, NULL);
    }
    return fields;
}

/*
 * Get a S3 handle which can be used to get objects on AWS S3
 * with the user's authorization.  A new connection is established
 * if we don't already have a suitable one.
 */
ReaderCacheEntry *
parquetGetFileReader(Aws::S3::S3Client *s3client, char *dname, char *fname)
{
	bool		found;
	ReaderCacheEntry *entry;
	ReaderCacheKey key = {0};

	/* First time through, initialize connection cache hashtable */
	if (FileReaderHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ReaderCacheKey);
		ctl.entrysize = sizeof(ReaderCacheEntry);
		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		FileReaderHash = hash_create("parquet_fdw file reader cache", 8,
									 &ctl,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
									  parquet_fdw_inval_callback, (Datum) 0);
		CacheRegisterSyscacheCallback(USERMAPPINGOID,
									  parquet_fdw_inval_callback, (Datum) 0);
	}

	/* Create hash key for the entry.  Assume no pad bytes in key struct */
	strcpy(key.dname, dname);
	strcpy(key.fname, fname);

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = (ReaderCacheEntry *) hash_search(FileReaderHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/*
		 * We need only clear "file_reader" here; remaining fields will be filled
		 * later when "file_reader" is set.
		 */
		entry->file_reader = NULL;
	}

	/*
	 * If cache entry doesn't have a reader, we have to establish a new
	 * reader.
	 */
	if (entry->file_reader == NULL || entry->file_reader->reader == nullptr)
	{
		std::unique_ptr<parquet::arrow::FileReader> reader;
		entry->pool = arrow::default_memory_pool();
		std::shared_ptr<arrow::io::RandomAccessFile> input(new S3RandomAccessFile(s3client, dname, fname));
		arrow::Status status = parquet::arrow::OpenFile(input, entry->pool, &reader);

        if (!status.ok())
            throw Error("failed to open Parquet file %s",
                             status.message().c_str());

		if (!entry->file_reader)
			entry->file_reader = new FileReaderCache();
		entry->file_reader->reader = std::move(reader);
		elog(DEBUG3, "new parquet file reader for s3handle %p %s/%s",
			 s3client, dname, fname);
	}

	return entry;
}

