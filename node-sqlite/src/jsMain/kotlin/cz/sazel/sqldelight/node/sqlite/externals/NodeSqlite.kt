@file:Suppress("UnusedParameter", "UnusedPrivateProperty")

package cz.sazel.sqldelight.node.sqlite.externals

@JsModule("node:sqlite")
internal external object NodeSqlite {
    class DatabaseSync(path: String) {
        fun prepare(sql: String): StatementSync
        fun exec(sql: String)
        fun close()
    }

    class StatementSync {
        fun run(vararg parameters: Any?): RunResult
        fun all(vararg parameters: Any?): Array<Array<dynamic>>
        fun iterate(vararg parameters: Any?): RowIterator
        fun setReturnArrays(enabled: Boolean)
        fun setReadBigInts(enabled: Boolean)
    }
}

internal external interface RunResult {
    /** `number`, or `bigint` with read BigInts enabled */
    val changes: Any
}

internal external interface RowIterator {
    fun next(): IteratorResult
}

internal external interface IteratorResult {
    val done: Boolean
    val value: Array<dynamic>?
}
