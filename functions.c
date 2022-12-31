/*-------------------------------------------------------------------------
 * functions.c
 *
 * Copyright (c) 2008-2023, PostgreSQL Global Development Group
 * Copyright (c) 2020-2023, Hironobu Suzuki @ interdb.jp
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_authid.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "pgstat.h"

#include "shutdown_db.h"
#include "hashtable.h"

/*
 * extern variables
 */
extern sddbSharedState * sddb;
extern HTAB *sddb_hash;

/*
 * Function declarations
 */
Datum		startup(PG_FUNCTION_ARGS);
Datum		shutdown_transactional(PG_FUNCTION_ARGS);
Datum		shutdown_immediate(PG_FUNCTION_ARGS);
Datum		shutdown_abort(PG_FUNCTION_ARGS);
Datum		shutdown_normal(PG_FUNCTION_ARGS);
Datum		sddb_show_db(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(startup);
PG_FUNCTION_INFO_V1(shutdown_transactional);
PG_FUNCTION_INFO_V1(shutdown_immediate);
PG_FUNCTION_INFO_V1(shutdown_abort);
PG_FUNCTION_INFO_V1(shutdown_normal);
PG_FUNCTION_INFO_V1(sddb_show_db);

static bool is_allowed_role(void);
static void check_workenv(void);
static bool check_dbname(char *dbname);
static Oid	get_dbid(char *dbname, StringInfoData *buf);
static bool do_alter_database(char *dbname, const bool set, StringInfoData *buf);
static bool kill_pids(const Oid dbid, const bool idle, StringInfoData *buf);
static bool do_checkpoint(StringInfoData *buf);
static bool run_sddb_killer(const Oid dbid, StringInfoData *buf);
static bool kill_bgworker(const pid_t pid, StringInfoData *buf);

/*
 * Check privilege
 */
static bool
is_allowed_role(void)
{
	/* Superusers or members of pg_read_all_stats members are allowed */
#if PG_VERSION_NUM >= 100000
	return is_member_of_role(GetUserId(),
#if PG_VERSION_NUM >= 140000
							 ROLE_PG_READ_ALL_STATS
#else
							 DEFAULT_ROLE_READ_ALL_STATS
#endif
		);
#else
	return superuser();
#endif
}

/*
 * Check whether the hash table and the shared info exist.
 */
static void
check_workenv(void)
{
	/* hash table must exist already */
	if (!sddb || !sddb_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("shutdown_db must be loaded via shared_preload_libraries")));
}

/*
 * Check dbname NOT IN (postgres, template1, template0) and EXISTS
 */
static bool
check_dbname(char *dbname)
{
	if (pg_strcasecmp(dbname, "postgres") == 0
		|| pg_strcasecmp(dbname, "template0") == 0
		|| pg_strcasecmp(dbname, "template1") == 0)
	{
		pfree(dbname);
		return false;
	}
	return true;
}

#define terminate_procedure(dbname) \
	do {						  \
	pfree(dbname);				  \
	SPI_finish();}				  \
	while (0)

/*
 * Get dbid by database name
 */
static Oid
get_dbid(char *dbname, StringInfoData *buf)
{
	int			ret;
	Oid			dbid;
	bool		isnull;

	appendStringInfo(buf, "SELECT oid FROM pg_database WHERE datname = %s;",
					 quote_literal_cstr(dbname));

	ret = SPI_execute(buf->data, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		terminate_procedure(dbname);
		elog(FATAL, "SPI_execute failed: error code %d", ret);
	}
	if (SPI_processed == 0)
	{
		terminate_procedure(dbname);
		elog(ERROR, "Database %s not found.", dbname);
	}
	else if (SPI_processed != 1)
	{
		terminate_procedure(dbname);
		elog(ERROR, "%s is not valid name.", dbname);
	}

	dbid = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc,
									   1, &isnull));

	if (isnull)
	{
		terminate_procedure(dbname);
		elog(FATAL, "null result");
	}
	return dbid;
}

/*
 * Excute ALTER DATABASE ALLOW_CONNECTIONS command
 */
static bool
do_alter_database(char *dbname, const bool set, StringInfoData *buf)
{
	int			ret;

	appendStringInfo(buf, "ALTER DATABASE %s ALLOW_CONNECTIONS %s;",
					 dbname,
					 (set) ? "true" : "false");
	ret = SPI_execute(buf->data, false, 0);

	if (ret != SPI_OK_UTILITY)
	{
		terminate_procedure(dbname);
		elog(FATAL, "failed to alter database");
	}

	return true;
}


/*
 * Execute the sddb_kill_processes function defined in the bgworker.c
 */
static bool
kill_pids(const Oid dbid, const bool idle, StringInfoData *buf)
{
	int			ret;

	appendStringInfo(buf, "SELECT %s.sddb_kill_processes(%d, %s);",
					 SCHEMA, dbid, (idle == true) ? "true" : "false");

	ret = SPI_execute(buf->data, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		elog(FATAL, "SPI_execute failed: error code %d", ret);
	}

	return true;
}

/*
 * Execute CHECKPOINT command
 */
static bool
do_checkpoint(StringInfoData *buf)
{
	int			ret;

	appendStringInfo(buf, "CHECKPOINT;");
	ret = SPI_execute(buf->data, false, 0);

	if (ret != SPI_OK_UTILITY)
	{
		SPI_finish();
		elog(FATAL, "failed to checkpoint");
	}

	return true;
}

/*
 * Run the killer process (bgworker).
 */
static bool
run_sddb_killer(const Oid dbid, StringInfoData *buf)
{
	int			ret;

	appendStringInfo(buf,
					 "SELECT %s.sddb_killer_launch(%d);",
					 SCHEMA,
					 dbid);

	ret = SPI_execute(buf->data, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		elog(FATAL, "SPI_execute failed: error code %d", ret);
		return false;
	}
	return true;
}

/*
 * Kill the process(pid).
 */
static bool
kill_bgworker(const pid_t pid, StringInfoData *buf)
{
	int			ret;

	appendStringInfo(buf, "SELECT pg_terminate_backend(%d);", pid);

	ret = SPI_execute(buf->data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	return true;
}

/*
 * SHUTDOWN and STARTUP commands
 */
Datum
shutdown_normal(PG_FUNCTION_ARGS)
{
	Oid			dbid;
	char	   *dbname;
	StringInfoData buf;

	check_workenv();

	if (!is_allowed_role())
		elog(ERROR, "You cannot execute this function because of no-privilege.");

	/* Get database name */
	dbname = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!check_dbname(dbname))
		elog(ERROR, "%s cannot be shutdown.", dbname);

	/* Connect SPI */
	SPI_connect();

	initStringInfo(&buf);
	/* Get dbid */
	dbid = get_dbid(dbname, &buf);

	/* Check whether dbid is in the hash table */
	if (sddb_find_entry(dbid, false))
	{
		elog(WARNING, "%s is already shutdown.", dbname);
		terminate_procedure(dbname);
		return (Datum) 0;
	}

	/* ALTER DATABASE */
	resetStringInfo(&buf);
	do_alter_database(dbname, false, &buf);

	/* Logging */
	elog(LOG, "%s has been shutdown in Normal mode", dbname);

	pfree(dbname);

	/* Add dbid into hash table */
	sddb_store_entry(dbid, NORMAL, false);

	SPI_finish();

	return (Datum) 0;
}

Datum
shutdown_abort(PG_FUNCTION_ARGS)
{
	Oid			dbid;
	char	   *dbname;
	StringInfoData buf;

	check_workenv();

	if (!is_allowed_role())
		elog(ERROR, "You cannot execute this function because of no-privilege.");

	/* Get database name */
	dbname = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!check_dbname(dbname))
		elog(ERROR, "%s cannot be shutdown.", dbname);

	/* Connect SPI */
	SPI_connect();

	initStringInfo(&buf);
	/* Get dbid */
	dbid = get_dbid(dbname, &buf);

	/* Check whether dbid is in the hash table */
	if (sddb_find_entry(dbid, false))
	{
		elog(WARNING, "%s is already shutdown.", dbname);
		terminate_procedure(dbname);
		return (Datum) 0;
	}

	/* ALTER DATABASE */
	resetStringInfo(&buf);
	do_alter_database(dbname, false, &buf);

	/* Logging */
	elog(LOG, "%s has been shutdown in Abort mode", dbname);

	pfree(dbname);

	/* Add dbid into hash table */
	sddb_store_entry(dbid, ABORT, false);

	/* Kill processes corresponding to dbname */
	resetStringInfo(&buf);
	kill_pids(dbid, false, &buf);

	/* Do checkpoint */
	resetStringInfo(&buf);
	do_checkpoint(&buf);

	SPI_finish();

	return (Datum) 0;
}

Datum
shutdown_immediate(PG_FUNCTION_ARGS)
{
	Oid			dbid;
	char	   *dbname;
	StringInfoData buf;

	check_workenv();

	if (!is_allowed_role())
		elog(ERROR, "You cannot execute this function because of no-privilege.");

	/* Get database name */
	dbname = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!check_dbname(dbname))
		elog(ERROR, "%s cannot be shutdown.", dbname);

	/* Connect SPI */
	SPI_connect();

	initStringInfo(&buf);
	/* Get dbid */
	dbid = get_dbid(dbname, &buf);

	/* Check whether dbid is in the hash table */
	if (sddb_find_entry(dbid, false))
	{
		elog(WARNING, "%s is already shutdown.", dbname);
		terminate_procedure(dbname);
		return (Datum) 0;
	}

	/* ALTER DATABASE */
	resetStringInfo(&buf);
	do_alter_database(dbname, false, &buf);

	/* Logging */
	elog(LOG, "%s has been shutdown in Immediate mode", dbname);

	pfree(dbname);

	/* Add dbid into hash table */
	sddb_store_entry(dbid, IMMEDIATE, false);

	/* Kill processes corresponding to dbname */
	resetStringInfo(&buf);
	kill_pids(dbid, false, &buf);

	SPI_finish();

	return (Datum) 0;
}

Datum
shutdown_transactional(PG_FUNCTION_ARGS)
{
	Oid			dbid;
	char	   *dbname;
	StringInfoData buf;

	check_workenv();

	if (!is_allowed_role())
		elog(ERROR, "You cannot execute this function because of no-privilege.");

	/* Get database name */
	dbname = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!check_dbname(dbname))
		elog(ERROR, "%s cannot be shutdown.", dbname);

	SPI_connect();

	/* Get dbid */
	initStringInfo(&buf);
	dbid = get_dbid(dbname, &buf);

	/* Check whether dbid is in the hash table */
	if (sddb_find_entry(dbid, false))
	{
		elog(WARNING, "%s is already shutdown.", dbname);
		terminate_procedure(dbname);
		return (Datum) 0;
	}

	/* ALTER DATABASE */
	resetStringInfo(&buf);
	do_alter_database(dbname, false, &buf);

	/* Logging */
	elog(LOG, "%s has been shutdown in Transactional mode", dbname);
	pfree(dbname);

	/* Add dbid into hash table */
	if (sddb_store_entry(dbid, TRANSACTIONAL, true))
	{
		resetStringInfo(&buf);
		run_sddb_killer(dbid, &buf);
	}
	else
	{
		SPI_finish();
		elog(ERROR, "killer process (%d) could not be executed.", dbid);
	}

	SPI_finish();

	return (Datum) 0;
}

Datum
startup(PG_FUNCTION_ARGS)
{
	Oid			dbid;
	char	   *dbname;
	StringInfoData buf;
	pid_t		pid;

	check_workenv();

	if (!is_allowed_role())
		elog(ERROR, "You cannot execute this function because of no-privilege.");

	/* Get database name */
	dbname = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!check_dbname(dbname))
		elog(ERROR, "%s cannot be started up.", dbname);

	SPI_connect();

	/* Get dbid */
	initStringInfo(&buf);
	dbid = get_dbid(dbname, &buf);

	/* Check whether dbid is in the hash table */
	if (!sddb_find_entry(dbid, false))
	{
		elog(WARNING, "%s is already started.", dbname);
		terminate_procedure(dbname);
		return (Datum) 0;
	}

	/* ALTER DATABASE */
	resetStringInfo(&buf);
	do_alter_database(dbname, true, &buf);

	/* Logging */
	elog(LOG, "%s starts again.", dbname);
	pfree(dbname);

	/* Kill the killer process if running */
	if ((pid = sddb_get_pid(dbid, true)) != InvalidPid)
	{
		resetStringInfo(&buf);
		kill_bgworker(pid, &buf);
	}
	SPI_finish();

	/* Delete dbid into hash table */
	sddb_delete_entry(dbid);

	return (Datum) 0;
}

/*
 * Retrieve stored dbs in the hash table.
 */
Datum
sddb_show_db(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Oid			userid = GetUserId();
	bool		is_allowed_role = false;
	HASH_SEQ_STATUS hash_seq;
	sddbEntry  *entry;

	/* Superusers or members of pg_read_all_stats members are allowed */
#if PG_VERSION_NUM >= 100000
	is_allowed_role = is_member_of_role(userid,
#if PG_VERSION_NUM >= 140000
										ROLE_PG_READ_ALL_STATS
#else
										DEFAULT_ROLE_READ_ALL_STATS
#endif
		);
#else
	is_allowed_role = superuser();
#endif

	/* hash table must exist already */
	if (!sddb || !sddb_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("shutdown_db must be loaded via shared_preload_libraries")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (is_allowed_role)
	{
		/* Get shared lock, and iterate over the hash table entries */
		LWLockAcquire(sddb->lock, LW_SHARED);

		hash_seq_init(&hash_seq, sddb_hash);
		while ((entry = hash_seq_search(&hash_seq)) != NULL)
		{
			Datum		values[SHUTDOWN_DB_COLS];
			bool		nulls[SHUTDOWN_DB_COLS];
			int			i = 0;

			/* Set values */
			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			values[i++] = ObjectIdGetDatum(entry->key.dbid);
			values[i++] = ObjectIdGetDatum(entry->mode);
			values[i++] = ObjectIdGetDatum(entry->is_running);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}

		LWLockRelease(sddb->lock);

		tuplestore_donestoring(tupstore);
	}
	return (Datum) 0;
}
