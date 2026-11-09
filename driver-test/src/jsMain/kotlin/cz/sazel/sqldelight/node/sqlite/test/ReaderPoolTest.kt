package cz.sazel.sqldelight.node.sqlite.test

import app.cash.sqldelight.async.coroutines.awaitAsList
import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlSchema
import cz.sazel.sqldelight.dontuse.HockeyPlayer
import cz.sazel.sqldelight.node.sqlite.SQLiteDriver
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Tests of backends with reader connections, run against every backend that supports them.
 */
abstract class ReaderPoolTest {
    abstract suspend fun createDriver(
        filename: String,
        schema: SqlSchema<QueryResult.AsyncValue<Unit>>?,
        readers: Int,
    ): SQLiteDriver

    private val dbName = "reader_pool_test.db"
    private val dbFiles = listOf(dbName, "$dbName-wal", "$dbName-shm")

    private fun withDatabase(fn: suspend Database.(SQLiteDriver) -> Unit) = runTest {
        dbFiles.forEach(::deleteIfExists)
        val driver = createDriver(dbName, Database.Schema, readers = 2)
        try {
            Database(driver).fn(driver)
        } finally {
            driver.closeSuspending()
            dbFiles.forEach(::deleteIfExists)
        }
    }

    private fun players(from: Long, count: Int) = (0 until count).map { HockeyPlayer(from + it, "Reader $it") }

    @Test
    fun testDatabaseIsInWalMode() = withDatabase { driver ->
        val mode = driver.executeQuery(null, "PRAGMA journal_mode", { cursor ->
            QueryResult.AsyncValue { cursor.next().await(); cursor.getString(0) }
        }, 0).await()
        assertEquals("wal", mode)
    }

    @Test
    fun testConcurrentReadsSeeCommittedWrites() = withDatabase {
        transaction {
            players(100, 50).forEach { testDataBaseDontUseQueries.insertFullPlayerObject(it) }
        }
        val sizes = coroutineScope {
            (1..20).map { async { testDataBaseDontUseQueries.selectAll().awaitAsList().size } }.awaitAll()
        }
        assertEquals(List(20) { 51 }, sizes)
    }

    @Test
    fun testReadsInTransactionSeeUncommittedRows() = withDatabase {
        transaction {
            testDataBaseDontUseQueries.insertFullPlayerObject(HockeyPlayer(8, "Teemu Selanne"))
            assertEquals(2, testDataBaseDontUseQueries.selectAll().awaitAsList().size)
        }
    }

    @Test
    fun testReadsDuringWrites() = withDatabase {
        val inserted = players(200, 100)
        val counts = coroutineScope {
            launch { inserted.forEach { testDataBaseDontUseQueries.insertFullPlayerObject(it) } }
            (1..50).map { async { testDataBaseDontUseQueries.selectAll().awaitAsList().size } }.awaitAll()
        }
        assertTrue(counts.all { it in 1..inserted.size + 1 }, "unexpected counts $counts")
        assertEquals(inserted.size + 1, testDataBaseDontUseQueries.selectAll().awaitAsList().size)
    }

    @Test
    fun testWithSelectQuery() = withDatabase { driver ->
        val count = driver.executeQuery(
            null,
            "WITH p AS (SELECT * FROM hockeyPlayer) SELECT count(*) FROM p",
            { cursor -> QueryResult.AsyncValue { cursor.next().await(); cursor.getLong(0) } },
            0,
        ).await()
        assertEquals(1L, count)
    }

    @Test
    fun testCloseRemovesWalFile() = runTest {
        dbFiles.forEach(::deleteIfExists)
        val driver = createDriver(dbName, Database.Schema, readers = 2)
        try {
            Database(driver).testDataBaseDontUseQueries.apply {
                insertFullPlayerObject(HockeyPlayer(8, "Teemu Selanne"))
                assertEquals(2, selectAll().awaitAsList().size)
            }
            assertTrue(fileExists("$dbName-wal"))
            driver.closeSuspending()
            driver.closeSuspending()
            assertFalse(fileExists("$dbName-wal"))
        } finally {
            dbFiles.forEach(::deleteIfExists)
        }
    }

    @Test
    fun testInMemoryDatabaseRejectsReaders() = runTest {
        assertFailsWith<IllegalArgumentException> { createDriver(":memory:", null, readers = 1) }
    }

    @Test
    fun testCloseSuspendingKeepsProcessAlive() = runInFreshProcess("testCloseSuspendingKeepsProcessAlive") {
        dbFiles.forEach(::deleteIfExists)
        try {
            val driver = createDriver(dbName, Database.Schema, readers = 2)
            Database(driver).testDataBaseDontUseQueries.insertFullPlayerObject(HockeyPlayer(8, "Teemu Selanne"))
            driver.closeSuspending()
            assertFalse(fileExists("$dbName-wal"))
        } finally {
            dbFiles.forEach(::deleteIfExists)
        }
    }
}
