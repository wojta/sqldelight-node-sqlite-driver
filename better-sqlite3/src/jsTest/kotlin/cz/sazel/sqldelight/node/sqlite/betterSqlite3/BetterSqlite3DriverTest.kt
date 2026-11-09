package cz.sazel.sqldelight.node.sqlite.betterSqlite3

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlSchema
import cz.sazel.sqldelight.node.sqlite.test.DriverWithLibraryTest
import cz.sazel.sqldelight.node.sqlite.test.ReaderPoolTest
import cz.sazel.sqldelight.node.sqlite.test.SQLiteDriverTest

class BetterSqlite3DriverTest : SQLiteDriverTest() {
    override suspend fun createDriver(
        filename: String,
        schema: SqlSchema<QueryResult.AsyncValue<Unit>>?,
        beginImmediate: Boolean,
    ) = initBetterSqlite3SqlDriver(filename, schema = schema, beginImmediate = beginImmediate)
}

class BetterSqlite3DriverWithLibraryTest : DriverWithLibraryTest() {
    override suspend fun createDriver(filename: String, schema: SqlSchema<QueryResult.AsyncValue<Unit>>) =
        initBetterSqlite3SqlDriver(filename, schema = schema)
}

class BetterSqlite3ReaderPoolTest : ReaderPoolTest() {
    override suspend fun createDriver(
        filename: String,
        schema: SqlSchema<QueryResult.AsyncValue<Unit>>?,
        readers: Int,
        batchSize: Int,
    ) = initBetterSqlite3SqlDriver(filename, schema = schema, readers = readers, batchSize = batchSize)
}
