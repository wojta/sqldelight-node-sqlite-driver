package cz.sazel.sqldelight.node.sqlite.betterSqlite3

import cz.sazel.sqldelight.node.sqlite.InternalSQLiteDriverApi
import cz.sazel.sqldelight.node.sqlite.SQLiteConnection
import cz.sazel.sqldelight.node.sqlite.SQLiteStatement
import cz.sazel.sqldelight.node.sqlite.betterSqlite3.externals.BetterSqlite3Database
import cz.sazel.sqldelight.node.sqlite.betterSqlite3.externals.BetterSqlite3Statement
import cz.sazel.sqldelight.node.sqlite.betterSqlite3.externals.RowIterator
import cz.sazel.sqldelight.node.sqlite.withLongsAsBigInt
import cz.sazel.sqldelight.node.sqlite.wrapJsErrors

@OptIn(InternalSQLiteDriverApi::class)
internal class BetterSqlite3Connection(private val db: BetterSqlite3Database) : SQLiteConnection {

    override suspend fun prepare(sql: String): SQLiteStatement =
        BetterSqlite3StatementWrapper(wrapJsErrors { db.prepare(sql) })

    override suspend fun exec(sql: String) = wrapJsErrors { db.exec(sql) }

    override suspend fun close() = wrapJsErrors { db.close() }
}

/**
 * better-sqlite3 allows `bind` only once per statement, but transactions reuse statements,
 * so parameters are kept and passed to each `run` / `iterate`.
 */
@OptIn(InternalSQLiteDriverApi::class)
@Suppress("SpreadOperator") // parameters go to a JS varargs API
internal class BetterSqlite3StatementWrapper(private val statement: BetterSqlite3Statement) : SQLiteStatement {
    private var parameters: Array<Any?> = emptyArray()
    private var rows: RowIterator? = null

    init {
        if (statement.reader) statement.raw(true)
        // integers beyond 2^53 would otherwise lose precision silently, the cursor checks the range on read
        statement.safeIntegers(true)
    }

    override suspend fun bind(parameters: List<Any?>) {
        closeRows()
        this.parameters = parameters.withLongsAsBigInt()
    }

    override suspend fun run(): Long = wrapJsErrors {
        closeRows()
        statement.run(*parameters).changes.toLong()
    }

    override suspend fun step(): Array<dynamic>? = wrapJsErrors {
        if (!statement.reader) {
            statement.run(*parameters)
            return@wrapJsErrors null
        }
        val rows = rows ?: statement.iterate(*parameters).also { rows = it }
        val next = rows.next()
        if (next.done) null else next.value
    }

    override suspend fun all(): Array<Array<dynamic>> = wrapJsErrors {
        closeRows()
        statement.all(*parameters)
    }

    override suspend fun reset() = closeRows()

    override suspend fun finalize() = closeRows()

    private fun closeRows() {
        rows?.asDynamic()?.`return`()
        rows = null
    }
}
