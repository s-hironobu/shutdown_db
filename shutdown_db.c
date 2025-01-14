/*-------------------------------------------------------------------------
 * shutdown_db.c
 *
 * Copyright (c) 2008-2025, PostgreSQL Global Development Group
 * Copyright (c) 2020-2025, Hironobu Suzuki @ interdb.jp
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "postmaster/bgworker.h"

#include "access/xact.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#if PG_VERSION_NUM >= 140000
#include "storage/shmem.h"
#endif
#include "tcop/utility.h"
#include "pgstat.h"

#include "shutdown_db.h"
#include "hashtable.h"

PG_MODULE_MAGIC;

/*
 * Saved hook values in case of unload
 */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/*
 * Links to shared memory state
 */
sddbSharedState *sddb = NULL;
HTAB	   *sddb_hash = NULL;

/*
 * Static variables
 */
static int	max_db_number;
#if PG_VERSION_NUM >= 160000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif

/*
 * Function declarations
 */
void		_PG_init(void);
void		_PG_fini(void);

static void sddb_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void sddb_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
#if PG_VERSION_NUM >= 140000
								bool readOnlyTree,
#endif
								ProcessUtilityContext context,
								ParamListInfo params, QueryEnvironment *queryEnv,
#if PG_VERSION_NUM >= 130000
								DestReceiver *dest, QueryCompletion *qc);
#else
								DestReceiver *dest, char *completionTag);
#endif
static Size sddb_memsize(void);
static void sddb_shmem_startup(void);
static void sddb_shmem_shutdown(int code, Datum arg);
static bool sddb_check_ht(void);
#if PG_VERSION_NUM >= 160000
static void shutdown_db_shmem_request(void);
#endif

/*
 * Module callback
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("shutdown_db.max_db_number",
							"The maxinum number of the databases which can be shutdown.",
							NULL,
							&max_db_number,
							10240,
							1024,
							1024 * 1024,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	EmitWarningsOnPlaceholders("shutdown_db");

#if PG_VERSION_NUM >= 160000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = shutdown_db_shmem_request;
#else
	RequestAddinShmemSpace(sddb_memsize());
#endif
#if PG_VERSION_NUM < 160000
#if PG_VERSION_NUM >= 90600
	RequestNamedLWLockTranche("shutdown_db", 1);
#else
	RequestAddinLWLocks(1);
#endif
#endif

	/* Install hooks. */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = sddb_shmem_startup;

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = sddb_ExecutorStart;

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = sddb_ProcessUtility;

	/* Initialize background worker. */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

	worker.bgw_restart_time = BGW_NEVER_RESTART;

	sprintf(worker.bgw_library_name, "shutdown_db");
	sprintf(worker.bgw_function_name, "shutdown_db_init");
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_name, BGW_MAXLEN, "shutdown_db init");
	snprintf(worker.bgw_type, BGW_MAXLEN, "shutdown_db");
	worker.bgw_main_arg = Int32GetDatum(1);
	RegisterBackgroundWorker(&worker);
}

#if PG_VERSION_NUM >= 160000
static void
shutdown_db_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(sddb_memsize());
	RequestNamedLWLockTranche("shutdown_db", 1);
}
#endif

void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	ProcessUtility_hook = prev_ProcessUtility;
	ExecutorStart_hook = prev_ExecutorStart;
#if PG_VERSION_NUM >= 160000
	shmem_request_hook = prev_shmem_request_hook;
#endif
}

/*
 * shmem_startup hook: allocate or attach to shared memory.
 */
static void
sddb_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	sddb = NULL;
	sddb_hash = NULL;

	/* Create or attach to the shared memory state, including hash table */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	sddb = ShmemInitStruct("shutdown_db", sizeof(sddbSharedState), &found);

	if (!found)
	{
		/* First time through ... */
#if PG_VERSION_NUM >= 90600
		sddb->lock = &(GetNamedLWLockTranche("shutdown_db"))->lock;
#else
		sddb->lock = LWLockAssign();
#endif
		SpinLockInit(&sddb->elock);
	}

	/* Set the initial value to is_enable, num_ht and num_bgw */
	SpinLockAcquire(&sddb->elock);
	sddb->num_ht = 0;
	sddb->num_bgw = 0;
	SpinLockRelease(&sddb->elock);

	/* Be sure everyone agrees on the hash table entry size */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(sddbHashKey);
	info.entrysize = sizeof(sddbEntry);

	sddb_hash = ShmemInitHash("shutdown_db hash",
							  max_db_number, max_db_number,
							  &info,
							  HASH_ELEM);

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
		on_shmem_exit(sddb_shmem_shutdown, (Datum) 0);
}

static void
sddb_shmem_shutdown(int code, Datum arg)
{
	/* Do nothing */
	return;
}


/*
 * Estimate shared memory space needed.
 */
static Size
sddb_memsize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(sddbSharedState));
	size = add_size(size, hash_estimate_size(max_db_number, sizeof(sddbEntry)));
	return size;
}


/*
 * Check whether the accessing database is stored in the hash table and the killer process is running.
 * If yes, returns true; otherwise false.
 */
static bool
sddb_check_ht(void)
{
	SpinLockAcquire(&sddb->elock);
	if (sddb->num_ht == 0)
	{
		SpinLockRelease(&sddb->elock);
		return false;
	}
	SpinLockRelease(&sddb->elock);

	if (sddb_find_entry(MyDatabaseId, true))
		return true;

	return false;
}

/*
 * ExecutorStart hook
 */
static void
sddb_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (sddb_check_ht() && !IsTransactionBlock())
	{
		elog(WARNING, "This database has already been shutdown and this process will be killed within seconds.");
		return;
	}
}


/*
 * ProcessUtility hook
 */
static void
			sddb_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
#if PG_VERSION_NUM >= 140000
								bool readOnlyTree,
#endif
								ProcessUtilityContext context, ParamListInfo params,
								QueryEnvironment *queryEnv, DestReceiver *dest,
#if PG_VERSION_NUM >= 130000
								QueryCompletion *qc)
#else
								char *completionTag)
#endif
{
	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
							readOnlyTree,
#endif
							context,
#if PG_VERSION_NUM >= 130000
							params, queryEnv, dest, qc);
#else
							params, queryEnv, dest, completionTag);
#endif
	else
		standard_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
								readOnlyTree,
#endif
								context,
#if PG_VERSION_NUM >= 130000
								params, queryEnv, dest, qc);
#else
								params, queryEnv, dest, completionTag);
#endif

	if (sddb_check_ht() && !IsTransactionBlock())
	{
		elog(WARNING, "This database has already been shutdown and this process will be killed within seconds.");
		return;
	}
}
