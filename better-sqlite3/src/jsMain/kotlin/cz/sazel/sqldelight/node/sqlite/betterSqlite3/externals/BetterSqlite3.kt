@file:Suppress("UnusedParameter", "UnusedPrivateProperty")

package cz.sazel.sqldelight.node.sqlite.betterSqlite3.externals

@JsModule("better-sqlite3")
private external object BetterSqlite3Module

/**
 * ES module import of the CommonJS package wraps its exported constructor in `default`.
 */
internal fun newBetterSqlite3Database(filename: String): BetterSqlite3Database {
    val module = BetterSqlite3Module.asDynamic()
    val constructor = if (jsTypeOf(module) == "function") module else module.default
    return js("new constructor(filename)").unsafeCast<BetterSqlite3Database>()
}

internal external interface BetterSqlite3Database {
    fun prepare(sql: String): BetterSqlite3Statement
    fun exec(sql: String)
    fun close()
}

internal external interface BetterSqlite3Statement {
    /** `true` when the statement returns rows */
    val reader: Boolean

    fun run(vararg parameters: Any?): RunResult
    fun all(vararg parameters: Any?): Array<Array<dynamic>>
    fun iterate(vararg parameters: Any?): RowIterator

    /** Rows as arrays instead of objects, allowed only on readers. */
    fun raw(toggle: Boolean)
    fun safeIntegers(toggle: Boolean)
}

internal external interface RunResult {
    val changes: Int
}

internal external interface RowIterator {
    fun next(): IteratorResult
}

internal external interface IteratorResult {
    val done: Boolean
    val value: Array<dynamic>?
}
