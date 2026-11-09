package cz.sazel.sqldelight.node.sqlite

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlSchema
import cz.sazel.sqldelight.node.sqlite.test.DriverWithLibraryTest
import cz.sazel.sqldelight.node.sqlite.test.ReaderPoolTest
import cz.sazel.sqldelight.node.sqlite.test.SQLiteDriverTest

class NodeSqliteDriverTest : SQLiteDriverTest() {
    override suspend fun createDriver(
        filename: String,
        schema: SqlSchema<QueryResult.AsyncValue<Unit>>?,
        beginImmediate: Boolean,
    ) = initNodeSqliteSqlDriver(filename, schema = schema, beginImmediate = beginImmediate)
}

class NodeSqliteDriverWithLibraryTest : DriverWithLibraryTest() {
    override suspend fun createDriver(filename: String, schema: SqlSchema<QueryResult.AsyncValue<Unit>>) =
        initNodeSqliteSqlDriver(filename, schema = schema)
}

class NodeSqliteReaderPoolTest : ReaderPoolTest() {
    override suspend fun createDriver(
        filename: String,
        schema: SqlSchema<QueryResult.AsyncValue<Unit>>?,
        readers: Int,
        batchSize: Int,
    ) = initNodeSqliteSqlDriver(filename, schema = schema, readers = readers, batchSize = batchSize)
}
