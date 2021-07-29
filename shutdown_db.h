/*-------------------------------------------------------------------------
 * shutdown_db.h
 *
 * Copyright (c) 2008-2019, PostgreSQL Global Development Group
 * Copyright (c) 2020, hironobu suzuki@interdb.jp
 *-------------------------------------------------------------------------
 */
#ifndef __SHUTDOWN_DB_H__
#define __SHUTDOWN_DB_H__

#include "storage/lwlock.h"

/*
 * Define constants
 */
#define SHUTDOWN_DB_COLS		 3

#define SCHEMA "shutdown_db"

enum mode
{
	INIT = 0,
	NORMAL,
	ABORT,
	IMMEDIATE,
	TRANSACTIONAL
};

/*
 * Define data types
 */
typedef struct sddbHashKey
{
	Oid			dbid;			/* the id of the shutdown database */
}			sddbHashKey;

typedef struct sddbEntry
{
	sddbHashKey key;			/* hash key of entry - MUST BE FIRST */
	slock_t		mutex;			/* protects the entry */
	Oid			dbid;			/* the id of the shutdown database */
	int			mode;			/* shutdown mode */
	bool		is_running;		/* whether users are using this database */
	pid_t		pid;			/* the pid of killer bgworker process if it's
								 * running; otherwise InvalidPid */
}			sddbEntry;

/*
 * Global shared state
 */
typedef struct sddbSharedState
{
	LWLock	   *lock;			/* protects hashtable search/modification */
	int			num_ht;			/* number of hashtable elements */
	int			num_bgw;		/* number of running bgworkers */
	slock_t		elock;			/* protects the variable `num_ht` and
								 * `num_bgw` */
}			sddbSharedState;

#endif
