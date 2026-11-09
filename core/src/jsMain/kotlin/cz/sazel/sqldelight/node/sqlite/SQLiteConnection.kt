package cz.sazel.sqldelight.node.sqlite

/**
 * Database connection of a SQLite backend module, wrapped by [SQLiteDriver].
 * Implementations throw [SQLiteJsException] on SQLite errors.
 */
interface SQLiteConnection {
    suspend fun prepare(sql: String): SQLiteStatement

    suspend fun exec(sql: String)

    suspend fun close()
}

/**
 * Prepared statement of a [SQLiteConnection].
 */
interface SQLiteStatement {
    /**
     * Binds positional parameters for the next [run] or [step].
     * Values are `null`, [Long], [Double], [String] or a Node.js `Buffer`.
     */
    suspend fun bind(parameters: List<Any?>)

    /**
     * Executes the statement, returns the number of changed rows.
     */
    suspend fun run(): Long

    /**
     * Fetches the next row as column values, `null` when there are no more rows.
     */
    suspend fun step(): Array<dynamic>?

    /**
     * Fetches all remaining rows at once.
     */
    suspend fun all(): Array<Array<dynamic>> {
        val rows = mutableListOf<Array<dynamic>>()
        while (true) {
            rows.add(step() ?: break)
        }
        return rows.toTypedArray()
    }

    /**
     * Rewinds the statement so [step] starts from the first row again.
     */
    suspend fun reset()

    suspend fun finalize()
}

/**
 * Marks API that is public only for backend modules and shared tests.
 */
@RequiresOptIn(level = RequiresOptIn.Level.ERROR)
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS, AnnotationTarget.PROPERTY)
annotation class InternalSQLiteDriverApi

/**
 * Converts [SQLiteStatement.bind] parameters for backends that bind JS numbers as REAL,
 * [Long] becomes a JS `BigInt` so it is bound as INTEGER.
 */
@InternalSQLiteDriverApi
fun List<Any?>.withLongsAsBigInt(): Array<Any?> = map { if (it is Long) jsBigInt(it.toString()) else it }.toTypedArray()

@Suppress("UnusedParameter")
private fun jsBigInt(value: String): dynamic = js("BigInt(value)")

/**
 * Runs [block] and rethrows any JS error as [SQLiteJsException].
 */
@InternalSQLiteDriverApi
inline fun <T> wrapJsErrors(block: () -> T): T =
    try {
        block()
    } catch (e: SQLiteException) {
        throw e
    } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
        throw SQLiteJsException(e)
    }
