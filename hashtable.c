/*-------------------------------------------------------------------------
 * hashtable.c
 *
 * Copyright (c) 2008-2025, PostgreSQL Global Development Group
 * Copyright (c) 2020-2025, Hironobu Suzuki @ interdb.jp
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
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
static sddbEntry * alloc_entry(sddbHashKey * key);


/*
 * Allocate a new hash table entry.
 * Caller must hold an exclusive lock on sddb->lock
 */
static sddbEntry *
alloc_entry(sddbHashKey * key)
{
	sddbEntry  *entry;
	bool		found;

	/*
	 * Find or create an entry with desired hash code. If hash table is full,
	 * return NULL.
	 */
	if ((entry = (sddbEntry *) hash_search(sddb_hash, key, HASH_ENTER_NULL, &found)) == NULL)
		return entry;

	if (!found)
	{
		/* New entry, initialize it */
		SpinLockInit(&entry->mutex);
		entry->dbid = InvalidOid;
		entry->mode = INIT;
		entry->is_running = false;
		entry->pid = InvalidPid;
	}

	return entry;
}


/*
 * Store the entry whose key is dbid to the hash table.
 */
bool
sddb_store_entry(const Oid dbid, const int mode, const bool is_running)
{
	sddbHashKey key;
	sddbEntry  *entry;
	sddbEntry  *e;

	/* Safety check... */
	if (!sddb || !sddb_hash)
		return false;

	/* Set key */
	key.dbid = dbid;

	/* Look up the hash table entry with shared lock. */
	LWLockAcquire(sddb->lock, LW_SHARED);
	entry = (sddbEntry *) hash_search(sddb_hash, &key, HASH_FIND, NULL);
	LWLockRelease(sddb->lock);

	if (entry != NULL)
	{
		elog(WARNING, "database %u is already stored.", dbid);
		return false;
	}

	LWLockAcquire(sddb->lock, LW_EXCLUSIVE);

	/* Create new entry */
	if ((entry = alloc_entry(&key)) == NULL)
	{
		/* New entry was not created since hash table is full. */
		LWLockRelease(sddb->lock);
		elog(WARNING, "New entry was not created since hash table is full.");
		return false;
	}

	/* Store data into the entry. */
	e = (sddbEntry *) entry;

	SpinLockAcquire(&e->mutex);
	e->dbid = dbid;
	e->mode = mode;
	e->is_running = is_running;
	e->pid = InvalidPid;
	SpinLockRelease(&e->mutex);

	LWLockRelease(sddb->lock);

	SpinLockAcquire(&sddb->elock);
	sddb->num_ht++;
	SpinLockRelease(&sddb->elock);

	return true;
}


/*
 * Find the entry whose key is dbid in the hash table.
 */
bool
sddb_find_entry(const Oid dbid, const bool is_running)
{
	sddbHashKey key;
	sddbEntry  *entry;
	sddbEntry  *e;

	/* Safety check... */
	if (!sddb || !sddb_hash)
		return false;

	/* quick check */
	SpinLockAcquire(&sddb->elock);
	if (sddb->num_ht == 0)
	{
		SpinLockRelease(&sddb->elock);
		return false;
	}
	SpinLockRelease(&sddb->elock);

	/* Set key */
	key.dbid = dbid;

	/* Look up the hash table entry with shared lock. */
	LWLockAcquire(sddb->lock, LW_SHARED);
	entry = (sddbEntry *) hash_search(sddb_hash, &key, HASH_FIND, NULL);
	LWLockRelease(sddb->lock);

	if (entry == NULL)
		return false;

	if (is_running)
	{
		e = (sddbEntry *) entry;

		SpinLockAcquire(&e->mutex);
		if (e->is_running == true)
		{
			SpinLockRelease(&e->mutex);
			return true;
		}
		SpinLockRelease(&e->mutex);

		return false;
	}

	return true;
}

/* Return the pid of the killer process which is invoked for polling to the
 * transactions in the database whose id is dbid.
 *
 * If is_active is true, it only returns the pid when the killer process is
 * running; if `is_active` is true and the killer process is already halted,
 * returns InvalidPid. If `is_active` is false, it returns the pid even if
 * the killer process has been already halted.
 */
pid_t
sddb_get_pid(const Oid dbid, const bool is_active)
{
	sddbHashKey key;
	sddbEntry  *entry;
	sddbEntry  *e;
	bool		is_running;
	pid_t		pid;

	/* Safety check... */
	if (!sddb || !sddb_hash)
		return InvalidPid;

	/* Set key */
	key.dbid = dbid;

	/* Look up the hash table entry with exclusive lock. */
	LWLockAcquire(sddb->lock, LW_EXCLUSIVE);
	entry = (sddbEntry *) hash_search(sddb_hash, &key, HASH_FIND, NULL);

	if (entry == NULL)
	{
		LWLockRelease(sddb->lock);
		return InvalidPid;
	}

	/* Store data into the entry. */
	e = (sddbEntry *) entry;

	SpinLockAcquire(&e->mutex);
	is_running = e->is_running;
	pid = e->pid;
	SpinLockRelease(&e->mutex);

	LWLockRelease(sddb->lock);

	if (!is_active)
		return pid;

	return (is_running == true) ? pid : InvalidPid;
}

/*
 * Set the `is_running` value into the entry whose key is dbid.
 */
bool
sddb_set_entry(const Oid dbid, const bool is_running)
{
	sddbHashKey key;
	sddbEntry  *entry;
	sddbEntry  *e;

	/* Safety check... */
	if (!sddb || !sddb_hash)
		return false;

	/* Set key */
	key.dbid = dbid;

	/* Look up the hash table entry with exclusive lock. */
	LWLockAcquire(sddb->lock, LW_EXCLUSIVE);
	entry = (sddbEntry *) hash_search(sddb_hash, &key, HASH_FIND, NULL);

	if (entry == NULL)
	{
		LWLockRelease(sddb->lock);
		return false;
	}

	/* Store data into the entry. */
	e = (sddbEntry *) entry;

	SpinLockAcquire(&e->mutex);
	e->is_running = is_running;
	SpinLockRelease(&e->mutex);

	LWLockRelease(sddb->lock);

	return true;
}

/*
 * Set the `pid` value into the entry whose key is dbid, where `pid` is
 * the pid of the killer bgworker which runs to check the activity
 * of the database, whose id is dbid.
 */
bool
sddb_set_pid2entry(const Oid dbid, const pid_t pid)
{
	sddbHashKey key;
	sddbEntry  *entry;
	sddbEntry  *e;

	/* Safety check... */
	if (!sddb || !sddb_hash)
		return false;

	/* Set key */
	key.dbid = dbid;

	/* Look up the hash table entry with exclusive lock. */
	LWLockAcquire(sddb->lock, LW_EXCLUSIVE);
	entry = (sddbEntry *) hash_search(sddb_hash, &key, HASH_FIND, NULL);

	if (entry == NULL)
	{
		LWLockRelease(sddb->lock);
		return false;
	}

	/* Store data into the entry. */
	e = (sddbEntry *) entry;

	SpinLockAcquire(&e->mutex);
	e->pid = pid;
	SpinLockRelease(&e->mutex);

	LWLockRelease(sddb->lock);

	return true;
}


/*
 * Delete stored entry
 */
void
sddb_delete_entry(const Oid dbid)
{
	sddbHashKey key;

	/* Safety check... */
	if (!sddb || !sddb_hash)
		return;

	key.dbid = dbid;

	LWLockAcquire(sddb->lock, LW_EXCLUSIVE);
	hash_search(sddb_hash, &key, HASH_REMOVE, NULL);
	LWLockRelease(sddb->lock);

	SpinLockAcquire(&sddb->elock);
	Assert(0 < sddb->num_ht);
	sddb->num_ht--;
	SpinLockRelease(&sddb->elock);
}
