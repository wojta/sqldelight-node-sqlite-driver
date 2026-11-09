package cz.sazel.sqldelight.node.sqlite.betterSqlite3

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlSchema
import cloud.nst.worker.TaskRegistry
import cz.sazel.sqldelight.node.sqlite.DEFAULT_BATCH_SIZE
import cz.sazel.sqldelight.node.sqlite.InternalSQLiteDriverApi
import cz.sazel.sqldelight.node.sqlite.SQLiteConnectionTask
import cz.sazel.sqldelight.node.sqlite.SQLiteDriver
import cz.sazel.sqldelight.node.sqlite.betterSqlite3.externals.newBetterSqlite3Database
import cz.sazel.sqldelight.node.sqlite.openWorkerDriver
import cz.sazel.sqldelight.node.sqlite.withSchema

private const val TASK_NAME = "cz.sazel.sqldelight.better-sqlite3"

private val moduleUrl: String = js("import.meta.url").unsafeCast<String>()

/** Runs in every worker thread that imports this module. */
@OptIn(ExperimentalStdlibApi::class, InternalSQLiteDriverApi::class)
@EagerInitialization
@Suppress("unused")
private val registration: Unit = TaskRegistry.register(TASK_NAME) {
    SQLiteConnectionTask { BetterSqlite3Connection(newBetterSqlite3Database(it)) }
}

/**
 * Opens [filename] with the `better-sqlite3` Node.js module, in its own worker thread.
 * @param schema created right after opening when given
 * @param beginImmediate start transactions with `BEGIN IMMEDIATE`
 * @param readers extra connections for `SELECT` queries outside transactions, each in its own worker thread, so
 * reads run in parallel; switches the database to WAL, needs a database file; since 1.1.0
 * @param batchSize rows transferred per worker message for queries without `LIMIT`, default `2048`
 * ([DEFAULT_BATCH_SIZE]); since 1.1.1
 */
@OptIn(InternalSQLiteDriverApi::class)
suspend fun initBetterSqlite3SqlDriver(
    filename: String,
    schema: SqlSchema<QueryResult.AsyncValue<Unit>>? = null,
    beginImmediate: Boolean = false,
    readers: Int = 0,
    batchSize: Int = DEFAULT_BATCH_SIZE,
): SQLiteDriver =
    openWorkerDriver(TASK_NAME, moduleUrl, filename, beginImmediate, readers, batchSize).withSchema(schema)
