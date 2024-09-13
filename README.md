# shutdown_db

This is a module that emulates the [Oracle's shutdown commands](https://docs.oracle.com/cd/B19306_01/server.102/b14231/start.htm#i1006543) by controlling access for each DB.


## Version

Version 1.0 Alpha 1

## Installation

You can install it to do the usual way shown below.

```
$ cd contrib
$ git clone https://github.com/s-hironobu/shutdown_db.git
$ cd shutdown_db
$ make && make install
```
You must add the line shown below in your postgresql.conf.

```
shared_preload_libraries = 'shutdown_db'
```

When the server is first started after installation, a schema 'shutdown_db', some functions and a view are created in the `postgres` database.


## How to use

At first, access to the `postgres` database.

```
$ psql postgres

psql (12.2)
Type "help" for help.
```

The `shutdown_db.show_db_list` shows the list of shutdown databases.

```
postgres=# SELECT * FROM shutdown_db.show_db_list;
 dbid  | datname | mode | num_users | killer_process_running 
-------+---------+------+-----------+------------------------
 24927 | test1   | INIT |         0 | f
(1 row)
```

If you want to prohibit access to the database `test3`, execute the `shutdown_db.shutdown_transactional()` function.

```
postgres=# SELECT shutdown_db.shutdown_transactional('test3');
 shutdown_transactional 
------------------------
 
(1 row)

postgres=# SELECT * FROM shutdown_db.show_db_list;
 dbid  | datname |     mode      | num_users | killer_process_running 
-------+---------+---------------+-----------+------------------------
 24927 | test1   | INIT          |         0 | f
 24929 | test3   | TRANSACTIONAL |         1 | t
(2 rows)
```

The database `test3` is no longer accessible.

```
$ psql test3
psql: error: could not connect to server: FATAL:  database "test3" is not currently accepting connections
```


This module provides four modes: shutdown_normal, shutdown_abort, shutdown_immediate and shutdown_transactional.


If you want to start the shutdown database, execute the `shutdown_db.startup()` function.

```
postgres=# SELECT shutdown_db.startup('test3');
 startup 
---------
 
(1 row)
```


## WARNING


1. Do not use [ALTER DATABASE ALLOW_CONNECTIONS](https://www.postgresql.org/docs/current/sql-alterdatabase.html) if you use this module. The reason why is that this module internally uses ALTER DATABASE ALLOW_CONNECTIONS command, so inconsistency in administrative data occurs.

2.  The `postgres` database cannot be shutdown because it stores all functions to operate this module.


## Functions
 - *shutdown_db.shutdown_normal('databasename')* : This function only prohibits access to the database. Internally, this function only executes `ALTER DATABASE ALLOW_CONNECTIONS true`.

 - *shutdown_db.shutdown_immediate('databasename')* : This function prohibits access to the database and kills the all backend process that are accesing the database. When this function is executed, the query processing in the backend processes are aborted and killed immediately.

 - *shutdown_db.shutdown_abort('databasename')* : This function is almost same as the shutdown_db.shutdown_immediate() and the difference is that it executes CHECKPOINT command after processing. 

 - *shutdown_db.shutdown_transactional('databasename')* : This function prohibits access to the database and kills the all backend process that are accesing the databaseafter the transaction processing has finished.
Even if this function is executed, the transaction processing that is already running is not killed, it is killed after the processing.


- *shutdown_db.startup('databasename')* : This function starts the shutdown database.


## View

- *shutdown_db.show_db_list*: This view shows the list of the shutdown databases.

  + *dbid* : Oid of the database
  + *datname* : Database name
  + *mode* : Shutdown mode. NORMAL, ABORT, IMMEDIATE, TRANSACTIONAL or INIT. (INIT means that this database has been shutdown from the server starting.)
  + *num_users* : The number of users who is accesing to the database.
  + *killer_process_running* : Whether the killer process is running. The killer process is a background worker process that is to kill the accessing user's backend processes after their transactions terminate. Thus, it is always false if the shutdown mode is not TRANSACTIONAL.
If true, the shutdown mode is TRANSACTIONAL and there are running transactions in the database.


## Configuration Parameter

- *shutdown_db.num_db_number* : the maxinum number of the databases which can be shutdown. Default is 10240.

## Uninstall

1. Delete `shutdown_db` from shared_preload_libraries in your postgresql.conf.
2. Execute DROP commands shown below:

```
DROP FUNCTION shutdown_db.sddb_killer_launch(INT);
DROP FUNCTION shutdown_db.sddb_kill_processes(dbid OID, idle BOOL);
DROP FUNCTION shutdown_db.shutdown_normal(TEXT);
DROP FUNCTION shutdown_db.shutdown_abort(TEXT);
DROP FUNCTION shutdown_db.shutdown_immediate(TEXT);
DROP FUNCTION shutdown_db.shutdown_transactional(TEXT);
DROP FUNCTION shutdown_db.startup(TEXT);
DROP VIEW shutdown_db.show_db_list;
DROP FUNCTION shutdown_db.sddb_show_db(OUT dbid oid, OUT mode, OUT is_running bool);
DROP SCHEMA shutdown_db CASCADE;
```

3. Restart your server.


## Change Log

- 13th Sep, 2024, Supported Version 17.
- 24th Sep, 2023, Supported Version 16.
- 31th Dec, 2022, Checked supporting Version 15.
- 29th Aug, 2021, Supported Version 14.
- 27th May, 2020: Version 1.0 Alpha 1 Released.
