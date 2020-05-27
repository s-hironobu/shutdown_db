/*-------------------------------------------------------------------------
 * hashtable.h
 *
 * Copyright (c) 2008-2019, PostgreSQL Global Development Group
 * Copyright (c) 2020, hironobu suzuki@interdb.jp
 *-------------------------------------------------------------------------
 */
#ifndef __HASHTABLE_H__
#define __HASHTABLE_H__

/*
 * Function declarations
 */
bool		sddb_store_entry(const Oid dbid, const int mode, const bool is_running);
void		sddb_delete_entry(const Oid dbid);
bool		sddb_find_entry(const Oid dbid, const bool is_running);
bool		sddb_set_entry(const Oid dbid, const bool is_running);
bool		sddb_set_pid2entry(const Oid dbid, const pid_t pid);
pid_t		sddb_get_pid(const Oid dbid, const bool is_active);

#endif
