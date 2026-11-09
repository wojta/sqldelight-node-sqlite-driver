# Worker threads

`node:sqlite` and `better-sqlite3` are synchronous, so each driver opens its connection in own `node:worker_threads`
thread with [worker](https://github.com/nst-cloud/worker) library and queries don't block the event loop. Rows come
back over `MessagePort` by structured clone, there is no serialization step. `sqlite3` runs queries on libuv threads
by itself, nothing here applies to it.

## Requirements

* the thread imports module file of the backend, so the build has to produce ES modules (`useEsModules()`) with
  default per-module output, not whole-program one and not a bundle
* `close()` the driver when you are done with it. Since 1.1.0 idle worker thread doesn't keep the Node.js process
  running, so app quits also without it, but close finishes the WAL file and frees threads

## Parallel reads

Since 1.1.0.

`node:sqlite` and `better-sqlite3` driver has one connection in one worker thread, so queries from concurrent
coroutines wait in line. Option `readers` opens extra connections to the same file, each in own worker thread:

```kotlin
val driver = initNodeSqliteSqlDriver(filename = "test.db", schema = Database.Schema, readers = 2)
```

* `SELECT` query outside a transaction goes to the least busy reader, `WITH` too when it has no `INSERT`, `UPDATE`,
  `DELETE` or `REPLACE`
* everything else runs on the writer connection, also all queries inside transaction, so they see its uncommitted rows
* the driver switches the database to WAL mode and it stays in WAL after close, while open there are `-wal` and
  `-shm` files next to database, WAL doesn't work on network file systems
* needs a database file, `:memory:` with readers throws `IllegalArgumentException`
* every reader is one more thread and SQLite connection, it costs memory

It helps only when many coroutines query at once. In the project benchmark 2 readers gave 15–55 % more indexed lookups
per second with 4–16 coroutines, after that the main thread was the limit, so more than 2 or 3 readers rarely pays off.
WAL alone also made parallel autocommit writes few times faster there.

## Row batching

Since 1.1.1.

Queries with a top-level `LIMIT` fetch the whole result in one worker message (including `LIMIT ?`, so a huge bound
value loads all those rows at once; `LIMIT -1` stays streamed). Other queries stream rows in batches of `batchSize`
(default `2048`), configurable on `initNodeSqliteSqlDriver` and `initBetterSqlite3SqlDriver`.
