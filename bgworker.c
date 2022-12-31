/*-------------------------------------------------------------------------
 * bgworker.c
 *
 * Copyright (c) 2008-2023, PostgreSQL Global Development Group
 * Copyright (c) 2020-2023, Hironobu Suzuki @ interdb.jp
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#if PG_VERSION_NUM >= 140000
#include "storage/latch.h"
#endif
#include "access/xact.h"
#include "executor/spi.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"

#include "shutdown_db.h"
#include "hashtable.h"

/*
 * extern variables
 */
extern sddbSharedState * sddb;
extern HTAB *sddb_hash;

/*
 * local variable
 */
static int	sddb_killer_naptime = 15;

/*
 * Function declarations
 */
void		shutdown_db_init(Datum) pg_attribute_noreturn();
void		shutdown_db_init(Datum main_arg);

PG_FUNCTION_INFO_V1(sddb_killer_launch);
void		sddb_killer_main(Datum) pg_attribute_noreturn();

static void start_tx(void);
static void commit_tx(void);

/*
 * flags set by signal handlers
 */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static void shutdown_db_sigterm(SIGNAL_ARGS);
static void shutdown_db_sighup(SIGNAL_ARGS);
static void sddb_killer_sigterm(SIGNAL_ARGS);
static void sddb_killer_sighup(SIGNAL_ARGS);

/*
 * Signal handler for SIGTERM
 */
static void
shutdown_db_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	/* do nothing */
	errno = save_errno;
}

static void
sddb_killer_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 */
static void
shutdown_db_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	/* do nothing */
	errno = save_errno;
}

static void
sddb_killer_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

/*
 * Helper function to start/commit transaction.
 */
static void
start_tx(void)
{
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
}

static void
commit_tx(void)
{
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Initialize shutdown_db. This function is only executed when PostgreSQL starts.
 */
void
shutdown_db_init(Datum main_arg)
{
	int			ret;
	int			ntup;
	bool		isnull;
	StringInfoData buf;

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGHUP, shutdown_db_sighup);
	pqsignal(SIGTERM, shutdown_db_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to postgres database */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	start_tx();

	/* Could we use CREATE SCHEMA IF NOT EXISTS? */
	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT count(*) FROM pg_namespace WHERE nspname = %s;",
					 quote_literal_cstr(SCHEMA));

	pgstat_report_activity(STATE_RUNNING, "initializing shutdown_db schema");
	SetCurrentStatementStartTimestamp();
	ret = SPI_execute(buf.data, true, 0);

	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	if (SPI_processed != 1)
		elog(FATAL, "not a singleton result");

	ntup = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc,
									   1, &isnull));

	if (isnull)
		elog(FATAL, "null result");

	/* Create schema and functions/views */
	if (ntup == 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE SCHEMA %s;"
						 "CREATE FUNCTION %s.sddb_show_db("
						 "   OUT dbid oid,"
						 "   OUT mode int,"
						 "   OUT is_running bool)"
						 "   RETURNS SETOF record"
						 "   AS 'shutdown_db'"
						 "   LANGUAGE C;"

						 "CREATE VIEW %s.show_db_list"
						 "  AS"
						 "  WITH db_list AS ("
						 "      SELECT s.dbid, d.datname, s.mode, s.is_running FROM %s.sddb_show_db() AS s, pg_database AS d WHERE s.dbid = d.oid"
						 "  )"
						 "  SELECT l.dbid, l.datname,"
						 "      CASE WHEN l.mode = 0 THEN 'INIT'"
						 "           WHEN l.mode = 1 THEN 'NORMAL'"
						 "           WHEN l.mode = 2 THEN 'ABORT'"
						 "           WHEN l.mode = 3 THEN 'IMMEDIATE'"
						 "           ELSE 'TRANSACTIONAL'"
						 "      END AS mode,"
						 "      count(a.datid) AS num_users,"
						 "      l.is_running AS killer_process_running"
						 "         FROM db_list AS l LEFT JOIN pg_stat_activity AS a  ON l.dbid = a.datid"
						 "               GROUP BY  l.dbid, l.datname, l.mode, l.is_running ORDER BY l.dbid;",
						 SCHEMA,
						 SCHEMA,
						 SCHEMA,
						 SCHEMA
			);

		pgstat_report_activity(STATE_RUNNING, "creating shutdown_db schema, functions and views.");
		SetCurrentStatementStartTimestamp();
		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to create shutdown_db schema, functions and views");

		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE FUNCTION %s.shutdown_normal(TEXT) RETURNS void"
						 "  AS 'shutdown_db'"
						 "  LANGUAGE C;"
						 "CREATE FUNCTION %s.shutdown_abort(TEXT) RETURNS void"
						 "  AS 'shutdown_db'"
						 "  LANGUAGE C;"
						 "CREATE FUNCTION %s.shutdown_immediate(TEXT) RETURNS void"
						 "  AS 'shutdown_db'"
						 "  LANGUAGE C;"
						 "CREATE FUNCTION %s.shutdown_transactional(TEXT) RETURNS void"
						 "  AS 'shutdown_db'"
						 "  LANGUAGE C;"
						 "CREATE FUNCTION %s.startup(TEXT) RETURNS void"
						 "  AS 'shutdown_db'"
						 "  LANGUAGE C;",
						 SCHEMA,
						 SCHEMA,
						 SCHEMA,
						 SCHEMA,
						 SCHEMA
			);

		pgstat_report_activity(STATE_RUNNING, "creating shutdown_db functions.");
		SetCurrentStatementStartTimestamp();
		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to create shutdown_db schema, functions and views");

		resetStringInfo(&buf);
		appendStringInfo(&buf,

		/*
		 * This function kills the backend processes which is accessing the
		 * database whose id is dbid.
		 *
		 * If the argument `idle` is true, this function only kills the
		 * backend process whose state is idle, i.e., the backends that are in
		 * the transaction block are not killed. Otherwise, `idle` is false,
		 * all backends that are accessing the database whose id is dbid are
		 * killed.
		 *
		 * After processing, if `idle` is true, this function returns the
		 * number of the remaining backend processes that are still running
		 * because of in the transaction block; if `idle` is false, this
		 * function always returns 0 because all corresponding backend
		 * processes are killed.
		 */
						 "CREATE FUNCTION %s.sddb_kill_processes(dbid OID, idle BOOL) RETURNS integer"
						 " AS $$"
						 "DECLARE"
						 "	pid Oid;"
						 "	query text;"
						 "	num_running int;"
						 "	activity pg_stat_activity\%%ROWTYPE;"
						 "BEGIN"
						 "	num_running := 0;"
						 "   	FOR activity IN SELECT * FROM pg_stat_activity WHERE datid = dbid LOOP"
						 "		pid := activity.pid;"
						 "		query := \'SELECT pg_cancel_backend(\' || pid || \'); SELECT pg_terminate_backend(\' || pid || \');\';"
						 "		num_running := num_running + 1;"
						 "		IF idle IS TRUE THEN"
						 "			IF activity.state = \'idle\' THEN"
						 "				EXECUTE query;"
						 "				num_running := num_running - 1;"
						 "			END IF;"
						 "		ELSE"
						 "			EXECUTE query;"
						 "			num_running := num_running - 1;"
						 "		END IF;"
						 "	END LOOP;"
						 "	RETURN num_running;"
						 "END;"
						 "$$ LANGUAGE plpgsql;"

						 "CREATE FUNCTION %s.sddb_killer_launch(INTEGER) RETURNS pg_catalog.int4 STRICT"
						 "  AS 'shutdown_db'"
						 "  LANGUAGE C;",
						 SCHEMA,
						 SCHEMA
			);

		pgstat_report_activity(STATE_RUNNING, "creating shutdown_db functions.");
		SetCurrentStatementStartTimestamp();
		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to create shutdown_db schema, functions and views.");

		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "REVOKE ALL ON FUNCTION %s.sddb_killer_launch(INT) FROM PUBLIC;"
						 "REVOKE ALL ON FUNCTION %s.sddb_kill_processes(dbid OID, idle BOOL) FROM PUBLIC;"
						 "REVOKE ALL ON FUNCTION %s.shutdown_normal(TEXT) FROM PUBLIC;"
						 "REVOKE ALL ON FUNCTION %s.shutdown_abort(TEXT) FROM PUBLIC;"
						 "REVOKE ALL ON FUNCTION %s.shutdown_immediate(TEXT) FROM PUBLIC;"
						 "REVOKE ALL ON FUNCTION %s.shutdown_transactional(TEXT) FROM PUBLIC;"
						 "REVOKE ALL ON FUNCTION %s.startup(TEXT) FROM PUBLIC;"
						 "REVOKE ALL ON FUNCTION %s.sddb_show_db(OUT dbid oid, OUT is_running bool) FROM PUBLIC;",
						 SCHEMA, SCHEMA, SCHEMA, SCHEMA,
						 SCHEMA, SCHEMA, SCHEMA, SCHEMA
			);

		pgstat_report_activity(STATE_RUNNING, "revoke all functions from public.");
		SetCurrentStatementStartTimestamp();
		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to revoke shutdown_db functions.");
	}

	/*
	 * Initialize the hash table
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT oid FROM pg_database"
					 " WHERE datallowconn = false AND datname"
					 " NOT IN ('template0', 'template1', 'postgres');"
		);

	pgstat_report_activity(STATE_RUNNING, buf.data);
	SetCurrentStatementStartTimestamp();
	ret = SPI_execute(buf.data, true, 0);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR, (errmsg("SPI_execute failed: error code %d", ret)));

	if (SPI_processed > 0)
	{
		int			i;
		int64		dbid;

		for (i = 0; i < SPI_processed; i++)
		{
			dbid = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
											   SPI_tuptable->tupdesc,
											   1, &isnull));
			sddb_store_entry(dbid, INIT, false);
		}
	}

	commit_tx();

	proc_exit(0);
}

/*
 * This function is executed when shutdown_db.shutdown_transactional() runs.
 *
 * This function periodically checks the number of users who run the transactions
 * in the database whose id is dbid, and if the number of users is 0, which means
 * that there is no running transaction in the target database, this function ends;
 * otherwise, this function continues to check.
 */
void
sddb_killer_main(Datum main_arg)
{
	int			dbid = DatumGetInt32(main_arg);
	StringInfoData buf;
	bool		isnull;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, sddb_killer_sighup);
	pqsignal(SIGTERM, sddb_killer_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT %s.sddb_kill_processes(%d, TRUE)",
					 SCHEMA, dbid);

	/* Set pid to the hashtale's entry */
	sddb_set_pid2entry(dbid, MyProcPid);

	/* Increment sddb->num_hbgw */
	SpinLockAcquire(&sddb->elock);
	sddb->num_bgw++;
	SpinLockRelease(&sddb->elock);

	/*
	 * Main loop
	 */
	while (!got_sigterm)
	{
		int			ret,
					rc,
					running_processes;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   sddb_killer_naptime * 1000L,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		/* emergency bailout if postmaster has died. */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();

		/* In case of a SIGHUP, do nothing. */
		if (got_sighup)
			got_sighup = false;

		/* In case of a SIGTERM, reduce num_bgw and halt. */
		if (got_sigterm)
		{
			got_sigterm = false;

			SpinLockAcquire(&sddb->elock);
			Assert(sddb->num_hbgw > 0);
			sddb->num_bgw--;
			SpinLockRelease(&sddb->elock);

			proc_exit(0);
		}

		start_tx();

		ret = SPI_execute(buf.data, true, 0);
		if (ret != SPI_OK_SELECT)
		{
			commit_tx();
			elog(FATAL, "SPI_execute failed: error code %d", ret);
			break;
		}

		if (SPI_processed != 1)
		{
			commit_tx();
			elog(FATAL, "%s returned invalid value", __func__);
			break;
		}

		running_processes = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
														SPI_tuptable->tupdesc,
														1, &isnull));

		commit_tx();

		if (isnull)
		{
			elog(FATAL, "null result");
			break;
		}

		if (running_processes == 0)
		{
			/* Set entry(dbid).is_running = false */
			sddb_set_entry(dbid, false);
			elog(LOG, "%s is going down.....", __func__);

			/* reduce num_bgw */
			SpinLockAcquire(&sddb->elock);
			Assert(sddb->num_hbgw > 0);
			sddb->num_bgw--;
			SpinLockRelease(&sddb->elock);

			proc_exit(0);
		}
	}

	proc_exit(1);
}

/*
 * Dynamically launch an SPI worker by the shutdown_transactional() command.
 */
Datum
sddb_killer_launch(PG_FUNCTION_ARGS)
{
	int32		dbid = PG_GETARG_INT32(0);
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;

	sprintf(worker.bgw_library_name, "shutdown_db");
	sprintf(worker.bgw_function_name, "sddb_killer_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "sddb_killer dbid=%d", dbid);
	snprintf(worker.bgw_type, BGW_MAXLEN, "sddb_killer");
	worker.bgw_main_arg = Int32GetDatum(dbid);

	/* Set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		elog(ERROR, "This function could not be executed because of the background worker failure.");
		PG_RETURN_NULL();
	}

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	pfree(handle);

	PG_RETURN_INT32(pid);
}
