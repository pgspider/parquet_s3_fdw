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
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fstream>
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
#include "funcapi.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/latch.h"
#include "utils/builtins.h"
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
	ConnCacheKey key;				/* hash key (must be first) */
	Aws::S3::S3Client *conn;		/* connection to foreign server, or NULL */
	/* Remaining fields are invalid when conn is NULL: */
	bool		invalidated;		/* true if reconnect is pending */
	Oid 		serverid;			/* foreign server OID used to get server name */
	uint32		server_hashvalue;	/* hash value of foreign server OID */
	uint32		mapping_hashvalue;	/* hash value of user mapping OID */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

/*
 * SQL functions
 */
extern "C"
{
PG_FUNCTION_INFO_V1(parquet_s3_fdw_get_connections);
PG_FUNCTION_INFO_V1(parquet_s3_fdw_disconnect);
PG_FUNCTION_INFO_V1(parquet_s3_fdw_disconnect_all);
}

/* prototypes of private functions */
static void make_new_connection(ConnCacheEntry *entry, UserMapping *user, bool use_minio);
static bool disconnect_cached_connections(Oid serverid);
static Aws::S3::S3Client *create_s3_connection(ForeignServer *server, UserMapping *user, bool use_minio);
static void close_s3_connection(ConnCacheEntry *entry);
static void check_conn_params(const char **keywords, const char **values, UserMapping *user);
static void parquet_fdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue);
static Aws::S3::S3Client* s3_client_open(const char *user, const char *password, bool use_minio, const char *endpoint, const char *awsRegion);
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
		ConnectionHash = hash_create("parquet_s3_fdw connections", 8,
									 &ctl,
#if (PG_VERSION_NUM >= 140000)
									 HASH_ELEM | HASH_BLOBS);
#else
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
#endif

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
		elog(DEBUG3, "parquet_s3_fdw: closing handle %p for option changes to take effect",
			 entry->conn);
		close_s3_connection(entry);
	}

	/*
	 * If cache entry doesn't have a connection, we have to establish a new
	 * connection.  (If connect_ps3 throws an error, the cache entry
	 * will remain in a valid empty state, ie conn == NULL.)
	 */
	if (entry->conn == NULL)
		make_new_connection(entry, user, use_minio);

	return entry->conn;
}

/*
 * Reset all transient state fields in the cached connection entry and
 * establish new connection to the remote server.
 */
static void
make_new_connection(ConnCacheEntry *entry, UserMapping *user, bool use_minio)
{
	ForeignServer *server = GetForeignServer(user->serverid);

	Assert(entry->conn == NULL);

	/* Reset all transient state fields, to be sure all are clean */
	entry->invalidated = false;
	entry->serverid = server->serverid;
	entry->server_hashvalue =
		GetSysCacheHashValue1(FOREIGNSERVEROID,
								ObjectIdGetDatum(server->serverid));
	entry->mapping_hashvalue =
		GetSysCacheHashValue1(USERMAPPINGOID,
								ObjectIdGetDatum(user->umid));

	/* Now try to make the handle */
	entry->conn = create_s3_connection(server, user, use_minio);

	elog(DEBUG3, "parquet_s3_fdw: new parquet_fdw handle %p for server \"%s\" (user mapping oid %u, userid %u)",
			entry->conn, server->servername, user->umid, user->userid);
}

/*
 * Workhorse to disconnect cached connections.
 *
 * This function scans all the connection cache entries and disconnects
 * the open connections whose foreign server OID matches with
 * the specified one. If InvalidOid is specified, it disconnects all
 * the cached connections.
 *
 * This function emits a warning for each connection that's used in
 * the current transaction and doesn't close it. It returns true if
 * it disconnects at least one connection, otherwise false.
 *
 * Note that this function disconnects even the connections that are
 * established by other users in the same local session using different
 * user mappings. This leads even non-superuser to be able to close
 * the connections established by superusers in the same local session.
 *
 * XXX As of now we don't see any security risk doing this. But we should
 * set some restrictions on that, for example, prevent non-superuser
 * from closing the connections established by superusers even
 * in the same session?
 */
static bool
disconnect_cached_connections(Oid serverid)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	bool		all = !OidIsValid(serverid);
	bool		result = false;

	/*
	 * Connection cache hashtable has not been initialized yet in this
	 * session, so return false.
	 */
	if (!ConnectionHash)
		return false;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore cache entry if no open connection right now. */
		if (!entry->conn)
			continue;

		if (all || entry->serverid == serverid)
		{
			elog(DEBUG3, "parquet_s3_fdw: discarding connection %p", entry->conn);
			close_s3_connection(entry);
			result = true;
		}
	}

	return result;
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
		char *awsRegion = NULL;
		char *endpoint = NULL;
		ListCell   *lc;
		List *lst_options = NULL;

		lst_options = list_concat(lst_options, user->options);
		lst_options = list_concat(lst_options, server->options);
		n = list_length(lst_options) + 1;
		keywords = (const char **) palloc(n * sizeof(char *));
		values = (const char **) palloc(n * sizeof(char *));
		
		n = ExtractConnectionOptions( lst_options,
									  keywords, values);
		keywords[n] = values[n] = NULL;

		/* verify connection parameters and make connection */
		check_conn_params(keywords, values, user);

		/* get id, password, region and endpoint from user and server options */
		foreach(lc, lst_options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "user") == 0)
				id = defGetString(def);

			if (strcmp(def->defname, "password") == 0)
				password = defGetString(def);

			if (strcmp(def->defname, "region") == 0)
				awsRegion = defGetString(def);

			if (strcmp(def->defname, "endpoint") == 0)
				endpoint = defGetString(def);
		}

		conn = s3_client_open(id, password, use_minio, endpoint, awsRegion);
		if (!conn)
			ereport(ERROR,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("parquet_s3_fdw: could not connect to S3 \"%s\"",
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
			 errmsg("parquet_s3_fdw: password is required"),
			 errdetail("parquet_s3_fdw: Non-superusers must provide a password in the user mapping.")));
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
s3_client_open(const char *user, const char *password, bool use_minio, const char *endpoint, const char * awsRegion)
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
		const Aws::String defaultEndpoint = "127.0.0.1:9000";
		clientConfig.scheme = Aws::Http::Scheme::HTTP;
		clientConfig.endpointOverride = endpoint ? (Aws::String) endpoint : defaultEndpoint;
		s3_client = new Aws::S3::S3Client(cred, clientConfig,
				Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
	}
	else
	{
		const Aws::String defaultRegion = "ap-northeast-1";
		clientConfig.scheme = Aws::Http::Scheme::HTTPS;
		clientConfig.region = awsRegion ? (Aws::String) awsRegion : defaultRegion;
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
		elog(ERROR, "parquet_s3_fdw: failed to get object list on %s. %s", bucketName.substr(0, len).c_str(), outcome.GetError().GetMessage().c_str());

	Aws::Vector<Aws::S3::Model::Object> objects =
		outcome.GetResult().GetContents();
	for (Aws::S3::Model::Object& object : objects)
	{
        Aws::String key = object.GetKey();
        if (!dir)
        {
		    objectlist = lappend(objectlist, makeString(pstrdup((char*)key.c_str())));
            elog(DEBUG1, "parquet_s3_fdw: accessing %s%s", s3path, key.c_str());
        }
        else if (strncmp(key.c_str(), dir, strlen(dir)) == 0)
        {
            char *file = pstrdup((char*) key.substr(strlen(dir)).c_str());
            /* Don't register if the object is directory. */
            if (key.at(key.length()-1) != '/' && strcmp(file, "/") != 0)
            {
                objectlist = lappend(objectlist, makeString(file));
                elog(DEBUG1, "parquet_s3_fdw: accessing %s%s", s3path, key.substr(strlen(dir)).c_str());
            }
			else
				pfree(file);
        }
        else
            elog(DEBUG1, "parquet_s3_fdw: skipping s3://%s/%s", bucketName.substr(0, len).c_str(), key.c_str());
	}

	return objectlist;
}

/*
 * If the keep_connections option of its server is disabled,
 * then discard it to recover. Next parquetGetConnection 
 * will open a new connection.
 */
void
parquet_disconnect_s3_server()
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	HASH_SEQ_STATUS scan_reader;
	ReaderCacheEntry *entry_reader;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore cache entry if no open connection right now */
		if (entry->conn == NULL)
			continue;

		elog(DEBUG3, "parquet_s3_fdw: discarding connection %p", entry->conn);
		close_s3_connection(entry);
	}

	hash_seq_init(&scan_reader, FileReaderHash);
	while ((entry_reader = (ReaderCacheEntry *) hash_seq_search(&scan_reader)))
	{
		/* Ignore cache entry if no open connection right now */
		if (entry_reader->file_reader == NULL)
			continue;

		elog(DEBUG3, "parquet_s3_fdw: discarding reader connection %p", entry_reader->file_reader);
		entry_reader->file_reader->reader.release();
		delete(entry_reader->file_reader);
		entry_reader->file_reader = NULL;
	}
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
            elog(ERROR, "parquet_s3_fdw: Cannot specify the mix of local file and S3 file");
        return LOC_S3;
    }
    else
    {
        if (loc == LOC_S3)
            elog(ERROR, "parquet_s3_fdw: Cannot specify the mix of local file and S3 file");
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
 * dirname is NULL (Or empty string when ANALYZE was executed)
 * and filename is set as fullpath started by "s3://".
 */
void
parquetSplitS3Path(const char *dirname, const char *filename, char **bucket, char **filepath)
{
    if (dirname != NULL && dirname[0] != '\0')
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
    int         ret;
    struct stat st;
    DIR        *dp;
    struct dirent *entry;
    char       *dirname = pstrdup(path);
    char       *back;

    ret = stat(path, &st);
    if (ret != 0)
        elog(ERROR, "parquet_s3_fdw: cannot stat %s", path);

    if ((st.st_mode & S_IFMT) == S_IFREG)
    {
        filelist = lappend(filelist, makeString(pstrdup(path)));
        elog(DEBUG1, "parquet_s3_fdw: file = %s", path);
        return filelist;
    }

    /* Do nothing if it is not file and not directory. */
    if ((st.st_mode & S_IFMT) != S_IFDIR)
        return filelist;

    dp = opendir(path);
    if (!dp)
        elog(ERROR, "parquet_s3_fdw: cannot open %s", path);

    /* remove redundant slash */
    back = dirname + strlen(dirname);
    while (*--back == '/')
    {
        *back = '\0';
    }

    entry = readdir(dp);
    while (entry != NULL) {
        char *newpath;
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0)
        {
            newpath = psprintf("%s/%s", dirname, entry->d_name);
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
        char       *path = strVal((Node *) lfirst(cell));
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
        elog(DEBUG1, "parquet_s3_fdw: %s", query);
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
		FileReaderHash = hash_create("parquet_s3_fdw file reader cache", 8,
									 &ctl,
#if (PG_VERSION_NUM >= 140000)
									 HASH_ELEM | HASH_BLOBS);
#else
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
#endif

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
		elog(DEBUG3, "parquet_s3_fdw: new parquet file reader for s3handle %p %s/%s",
			 s3client, dname, fname);
	}

	return entry;
}

/*
 * List active foreign server connections.
 *
 * This function takes no input parameter and returns setof record made of
 * following values:
 * - server_name - server name of active connection. In case the foreign server
 *   is dropped but still the connection is active, then the server name will
 *   be NULL in output.
 * - valid - true/false representing whether the connection is valid or not.
 *
 * No records are returned when there are no cached connections at all.
 */
extern "C"
{
Datum
parquet_s3_fdw_get_connections(PG_FUNCTION_ARGS)
{
#define PARQUET_S3_FDW_GET_CONNECTIONS_COLS	2
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parquet_s3_fdw: set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parquet_s3_fdw: materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "parquet_s3_fdw: return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* If cache doesn't exist, we return no records */
	if (!ConnectionHash)
	{
		/* clean up and return the tuplestore */
		tuplestore_donestoring(tupstore);

		PG_RETURN_VOID();
	}

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		ForeignServer	*server;
		Datum			values[PARQUET_S3_FDW_GET_CONNECTIONS_COLS];
		bool			nulls[PARQUET_S3_FDW_GET_CONNECTIONS_COLS];

		/* We only look for open remote connections */
		if (!entry->conn)
			continue;

		server = GetForeignServerExtended(entry->serverid, FSV_MISSING_OK);

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		/*
		 * The foreign server may have been dropped in current explicit
		 * transaction. It is not possible to drop the server from another
		 * session when the connection associated with it is in use in the
		 * current transaction, if tried so, the drop query in another session
		 * blocks until the current transaction finishes.
		 *
		 * Even though the server is dropped in the current transaction, the
		 * cache can still have associated active connection entry, say we
		 * call such connections dangling. Since we can not fetch the server
		 * name from system catalogs for dangling connections, instead we show
		 * NULL value for server name in output.
		 *
		 * We could have done better by storing the server name in the cache
		 * entry instead of server oid so that it could be used in the output.
		 * But the server name in each cache entry requires 64 bytes of
		 * memory, which is huge, when there are many cached connections and
		 * the use case i.e. dropping the foreign server within the explicit
		 * current transaction seems rare. So, we chose to show NULL value for
		 * server name in output.
		 */
		if (!server)
		{
			/*
			 * If the server has been dropped in the current explicit
			 * transaction, then this entry would have been invalidated in
			 * parquet_fdw_inval_callback at the end of drop server command.
			 * Note that this connection would not have been closed in
			 * parquet_fdw_inval_callback because it is still being used in
			 * the current explicit transaction. So, assert that here.
			 */
			Assert(entry->conn && entry->invalidated);

			/* Show null, if no server name was found */
			nulls[0] = true;
		}
		else
			values[0] = CStringGetTextDatum(server->servername);

		values[1] = BoolGetDatum(!entry->invalidated);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
}

/*
 * Disconnect the specified cached connections.
 *
 * This function discards the open connections that are established by
 * parquet_s3_fdw from the local session to the foreign server with
 * the given name. Note that there can be multiple connections to
 * the given server using different user mappings. If the connections
 * are used in the current local transaction, they are not disconnected
 * and warning messages are reported. This function returns true
 * if it disconnects at least one connection, otherwise false. If no
 * foreign server with the given name is found, an error is reported.
 */
Datum
parquet_s3_fdw_disconnect(PG_FUNCTION_ARGS)
{
	ForeignServer	*server;
	char			*servername;

	servername = text_to_cstring(PG_GETARG_TEXT_PP(0));
	server = GetForeignServerByName(servername, false);

	PG_RETURN_BOOL(disconnect_cached_connections(server->serverid));
}

/*
 * Disconnect all the cached connections.
 *
 * This function discards all the open connections that are established by
 * parquet_s3_fdw from the local session to the foreign servers.
 * If the connections are used in the current local transaction, they are
 * not disconnected and warning messages are reported. This function
 * returns true if it disconnects at least one connection, otherwise false.
 */
Datum
parquet_s3_fdw_disconnect_all(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(disconnect_cached_connections(InvalidOid));
}
}

bool
parquet_upload_file_to_s3(const char *dirname, Aws::S3::S3Client *s3_client, const char *filename, const char *local_file)
{
    char           *bucket;
    char           *filepath;
    Aws::S3::Model::PutObjectRequest request;
    std::shared_ptr<Aws::IOStream> input_data;
    Aws::S3::Model::PutObjectOutcome outcome;

    parquetSplitS3Path(dirname, filename, &bucket, &filepath);
    request.SetBucket(bucket);

    /*
     * We are using the name of the file as the key for the object in the bucket.
     */
    request.SetKey(filepath);

    /* load local file to update */
    input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
                 local_file,
                 std::ios_base::in | std::ios_base::binary);

    request.SetBody(input_data);
    outcome = s3_client->PutObject(request);

    if (outcome.IsSuccess())
    {
        elog(DEBUG1, "parquet_s3_fdw: added object '%s' to bucket '%s'.", filepath, bucket);
        return true;
    }
    else
    {
        elog(ERROR, "parquet_s3_fdw: PutObject: %s", outcome.GetError().GetMessage().c_str());
        return false;
    }
}
