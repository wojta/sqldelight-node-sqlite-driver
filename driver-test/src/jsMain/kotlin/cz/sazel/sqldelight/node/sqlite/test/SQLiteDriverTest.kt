@file:OptIn(InternalSQLiteDriverApi::class)

package cz.sazel.sqldelight.node.sqlite.test

import app.cash.sqldelight.async.coroutines.await
import app.cash.sqldelight.async.coroutines.awaitCreate
import app.cash.sqldelight.async.coroutines.awaitQuery
import app.cash.sqldelight.db.*
import cz.sazel.sqldelight.node.sqlite.InternalSQLiteDriverApi
import cz.sazel.sqldelight.node.sqlite.SQLiteDriver
import cz.sazel.sqldelight.node.sqlite.SQLiteException
import cz.sazel.sqldelight.node.sqlite.SQLiteJsException
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlin.random.Random
import kotlin.test.*
import kotlin.time.Duration.Companion.milliseconds

private const val SQLITE_BUSY = 5

typealias InsertFunction = suspend (SqlPreparedStatement.() -> Unit) -> Unit

/**
 * Driver tests run against every backend, a backend module extends this class in its tests.
 */
abstract class SQLiteDriverTest {
    abstract suspend fun createDriver(
        filename: String,
        schema: SqlSchema<QueryResult.AsyncValue<Unit>>? = null,
        beginImmediate: Boolean = false,
    ): SQLiteDriver

    protected val schema = object : SqlSchema<QueryResult.AsyncValue<Unit>> {
        override val version = 1L

        override fun create(driver: SqlDriver): QueryResult.AsyncValue<Unit> = QueryResult.AsyncValue {
            driver.execute(
                0,
                """
              |CREATE TABLE test (
              |  id INTEGER PRIMARY KEY,
              |  value TEXT
              |);
        """.trimMargin(),
                0,
            ).await()
            driver.execute(
                1,
                """
              |CREATE TABLE nullability_test (
              |  id INTEGER PRIMARY KEY,
              |  integer_value INTEGER,
              |  text_value TEXT,
              |  blob_value BLOB,
              |  real_value REAL,
              |  bool_value BOOLEAN
              |);
        """.trimMargin(),
                0,
            ).await()
        }

        override fun migrate(
            driver: SqlDriver,
            oldVersion: Long,
            newVersion: Long,
            vararg callbacks: AfterVersion
        ): QueryResult.AsyncValue<Unit> = QueryResult.AsyncValue {}
    }

    private fun runTest(block: suspend (SqlDriver) -> Unit) = kotlinx.coroutines.test.runTest {
        try {
            deleteIfExists("test.db")
            val driver = createDriver("test.db", schema = schema)
            println("db test.db created")
            block(driver)
            driver.close()
            println("deleting db")
            unlinkSync("test.db")
        } catch (e: Exception) {
            e.printStackTrace()
            fail(e.message)
        }
    }

    @Test
    fun test_just_driver_init() = runTest {
        println("Driver init")
    }

    @Test
    fun insert_can_run_multiple_times() = runTest { driver ->

        val insert: InsertFunction = { binders: SqlPreparedStatement.() -> Unit ->
            driver.await(2, "INSERT INTO test VALUES (?, ?);", 2, binders)
        }

        suspend fun query(mapper: suspend (SqlCursor) -> Unit) {
            driver.awaitQuery(3, "SELECT * FROM test", mapper, 0)
        }

        suspend fun changes(mapper: suspend (SqlCursor) -> Long?): Long? {
            return driver.awaitQuery(4, "SELECT changes()", mapper, 0)
        }

        query {
            assertFalse(it.next().await())
        }

        insert {
            bindLong(0, 1)
            bindString(1, "Alec")
        }

        query {
            assertTrue(it.next().await())
            assertFalse(it.next().await())
        }

        assertEquals(1, changes { it.next().await(); it.getLong(0) })

        query {
            assertTrue(it.next().await())
            assertEquals(1, it.getLong(0))
            assertEquals("Alec", it.getString(1))
        }

        insert {
            bindLong(0, 2)
            bindString(1, "Jake")
        }
        assertEquals(1, changes { it.next().await(); it.getLong(0) })

        query {
            assertTrue(it.next().await())
            assertEquals(1, it.getLong(0))
            assertEquals("Alec", it.getString(1))
            assertTrue(it.next().await())
            assertEquals(2, it.getLong(0))
            assertEquals("Jake", it.getString(1))
        }

        driver.await(5, "DELETE FROM test", 0)
        assertEquals(2, changes { it.next().await(); it.getLong(0) })

        query {
            assertFalse(it.next().await())
        }
    }

    /**
     * Regression: [SQLiteDriver.execute] used to discard the statement's `changes` and always return 0.
     */
    @Test
    fun execute_returns_number_of_rows_affected() = runTest { driver ->
        assertEquals(1, driver.execute(20, "INSERT INTO test VALUES (?, ?);", 2) {
            bindLong(0, 1)
            bindString(1, "Alec")
        }.await())
        assertEquals(1, driver.execute(21, "INSERT INTO test VALUES (?, ?);", 2) {
            bindLong(0, 2)
            bindString(1, "Jake")
        }.await())
        assertEquals(2, driver.execute(22, "UPDATE test SET value = 'Whoever'", 0).await())
        assertEquals(1, driver.execute(23, "DELETE FROM test WHERE id = ?", 1) { bindLong(0, 1) }.await())
        assertEquals(1, driver.execute(24, "DELETE FROM test", 0).await())
    }

    @Test
    fun query_can_run_multiple_times() = runTest { driver ->

        val insert: InsertFunction = { binders: SqlPreparedStatement.() -> Unit ->
            driver.await(2, "INSERT INTO test VALUES (?, ?);", 2, binders)
        }

        suspend fun changes(mapper: suspend (SqlCursor) -> Long?): Long? {
            return driver.awaitQuery(4, "SELECT changes()", mapper, 0)
        }

        insert {
            bindLong(0, 1)
            bindString(1, "Alec")
        }
        assertEquals(1, changes { it.next().await(); it.getLong(0) })
        insert {
            bindLong(0, 2)
            bindString(1, "Jake")
        }
        assertEquals(1, changes { it.next().await(); it.getLong(0) })

        suspend fun query(binders: SqlPreparedStatement.() -> Unit, mapper: suspend (SqlCursor) -> Unit) {
            driver.awaitQuery(6, "SELECT * FROM test WHERE value = ?", mapper, 1, binders)
        }
        query(
            binders = {
                bindString(0, "Jake")
            },
            mapper = {
                assertTrue(it.next().await())
                assertEquals(2, it.getLong(0))
                assertEquals("Jake", it.getString(1))
            },
        )

        // Second time running the query is fine
        query(
            binders = {
                bindString(0, "Jake")
            },
            mapper = {
                assertTrue(it.next().await())
                assertEquals(2, it.getLong(0))
                assertEquals("Jake", it.getString(1))
            },
        )
    }

    @Test
    fun sqlResultSet_getters_return_null_if_the_column_values_are_NULL() = runTest { driver ->
        val insert: InsertFunction = { binders: SqlPreparedStatement.() -> Unit ->
            driver.await(7, "INSERT INTO nullability_test VALUES (?, ?, ?, ?, ?, ?);", 6, binders)
        }

        suspend fun changes(mapper: suspend (SqlCursor) -> Long?): Long? {
            return driver.awaitQuery(4, "SELECT changes()", mapper, 0)
        }

        insert {
            bindLong(0, 1)
            bindLong(1, null)
            bindString(2, null)
            bindBytes(3, null)
            bindDouble(4, null)
            bindBoolean(5, null)
        }

        val mapper: suspend (SqlCursor) -> Unit = {
            assertTrue(it.next().await())
            assertEquals(1, it.getLong(0))
            assertNull(it.getLong(1))
            assertNull(it.getString(2))
            assertNull(it.getBytes(3))
            assertNull(it.getDouble(4))
            assertNull(it.getBoolean(5))
        }
        driver.awaitQuery(8, "SELECT * FROM nullability_test", mapper, 0)
        changes { it.next().await(); it.getLong(0) }
    }

    @Test
    fun types_are_correctly_converted_from_JS_to_Kotlin_and_back() = runTest { driver ->
        val insert: InsertFunction = { binders: SqlPreparedStatement.() -> Unit ->
            driver.await(7, "INSERT INTO nullability_test VALUES (?, ?, ?, ?, ?, ?);", 6, binders)
        }

        // MAX_SAFE_LONG (2^53) itself, not Long.MAX_VALUE: values beyond it are rejected,
        // see long_round_trips_up_to_2_pow_53_but_not_beyond.
        val maxSafeLong = 1L shl 53
        insert {
            bindLong(0, 1)
            bindLong(1, maxSafeLong)
            bindString(2, "Hello")
            bindBytes(3, ByteArray(5) { it.toByte() })
            bindDouble(4, Float.MAX_VALUE.toDouble())
            bindBoolean(5, true)
        }

        val mapper: suspend (SqlCursor) -> Unit = {
            assertTrue(it.next().await())
            assertEquals(1, it.getLong(0))
            assertEquals(maxSafeLong, it.getLong(1))
            assertEquals("Hello", it.getString(2))
            val bytes = assertNotNull(it.getBytes(3))
            assertContentEquals(ByteArray(5) { i -> i.toByte() }, bytes)
            assertEquals(Float.MAX_VALUE.toDouble(), it.getDouble(4))
            assertEquals(true, it.getBoolean(5))
        }
        driver.awaitQuery(8, "SELECT * FROM nullability_test", mapper, 0)
    }

    @Test
    fun test_transaction_commit() = runTest { driver ->
        val insert: InsertFunction = { binders: SqlPreparedStatement.() -> Unit ->
            driver.await(12, "INSERT INTO test VALUES (?, ?);", 2, binders)
        }

        driver.newTransaction().await()
        val success = try {
            insert {
                bindLong(0, 3)
                bindString(1, "Hello")
            }
            true
        } catch (e: SQLiteException) {
            false
        }

        try {
            assertTrue(success)
        } finally {
            (driver as SQLiteDriver)._endTransactionForTests(success)?.await()
        }
    }

    @Test
    fun test_transaction_rollback() = runTest { driver ->
        val insert: InsertFunction = { binders: SqlPreparedStatement.() -> Unit ->
            driver.await(13, "INSERT INTO nonexisting_table VALUES (?, ?);", 2, binders)
        }
        driver.newTransaction().await()
        val success = try {
            insert {
                bindLong(0, 3)
                bindString(1, "Hello")
            }
            true
        } catch (e: SQLiteException) {
            false
        }

        try {
            assertFalse(success)
        } finally {
            (driver as SQLiteDriver)._endTransactionForTests(success)?.await()
        }
    }

    @Test
    fun exceptions_are_handled_correctly() = runTest { driver ->
        val error = assertFailsWith<SQLiteJsException> {
            schema.awaitCreate(driver)
        }
        assertContains(error.toString(), "table test already exists")
        assertEquals(1, error.errorNumber)
    }

    @Test
    fun blob_round_trips_various_sizes() = runTest { driver ->
        val sizes = listOf(0, 1, 16, 2048, 1024 * 1024)
        sizes.forEachIndexed { idx, size ->
            val id = idx.toLong()
            val bytes = Random(idx).nextBytes(size)
            driver.execute(100 + idx, "INSERT INTO nullability_test (id, blob_value) VALUES (?, ?);", 2) {
                bindLong(0, id)
                bindBytes(1, bytes)
            }.await()

            driver.awaitQuery(200 + idx, "SELECT blob_value FROM nullability_test WHERE id = ?", { cursor ->
                assertTrue(cursor.next().await())
                assertContentEquals(bytes, assertNotNull(cursor.getBytes(0)))
            }, 1) { bindLong(0, id) }
        }
    }

    /**
     * Node's Buffer pool means a value's `byteOffset` is not always 0; getBytes must respect it.
     */
    @Test
    fun blob_read_after_many_small_allocations_is_not_corrupted_by_the_pool() = runTest { driver ->
        val count = 200
        val expected = (0 until count).map { i -> ByteArray(4) { (i + it).toByte() } }
        expected.forEachIndexed { i, bytes ->
            driver.execute(300 + i, "INSERT INTO nullability_test (id, blob_value) VALUES (?, ?);", 2) {
                bindLong(0, i.toLong())
                bindBytes(1, bytes)
            }.await()
        }

        driver.awaitQuery(500, "SELECT blob_value FROM nullability_test ORDER BY id", { cursor ->
            var i = 0
            while (cursor.next().await()) {
                assertContentEquals(expected[i], assertNotNull(cursor.getBytes(0)))
                i++
            }
            assertEquals(count, i)
        }, 0)
    }

    @Test
    fun blob_column_typeof_is_blob_not_text() = runTest { driver ->
        driver.execute(600, "INSERT INTO nullability_test (id, blob_value) VALUES (?, ?);", 2) {
            bindLong(0, 1)
            bindBytes(1, byteArrayOf(1, 2, 3))
        }.await()

        driver.awaitQuery(601, "SELECT typeof(blob_value) FROM nullability_test WHERE id = ?", { cursor ->
            assertTrue(cursor.next().await())
            assertEquals("blob", cursor.getString(0))
        }, 1) { bindLong(0, 1) }
    }

    @Test
    fun long_round_trips_at_2_pow_53_but_binding_beyond_it_throws() = runTest { driver ->
        val maxSafe = 1L shl 53
        driver.execute(700, "INSERT INTO nullability_test (id, integer_value) VALUES (?, ?);", 2) {
            bindLong(0, 1)
            bindLong(1, maxSafe)
        }.await()

        driver.awaitQuery(701, "SELECT integer_value FROM nullability_test WHERE id = 1", { cursor ->
            assertTrue(cursor.next().await())
            assertEquals(maxSafe, cursor.getLong(0))
        }, 0)

        // Beyond 2^53 a Long is no longer representable exactly as a JS double, so binding
        // it would silently store the wrong value; bindLong rejects it instead.
        assertFailsWith<SQLiteException> {
            driver.execute(702, "INSERT INTO nullability_test (id, integer_value) VALUES (?, ?);", 2) {
                bindLong(0, 2)
                bindLong(1, maxSafe + 1)
            }.await()
        }
    }

    @Test
    fun reading_a_long_beyond_2_pow_53_throws() = runTest { driver ->
        // Written directly in SQL so it bypasses bindLong's own check and lands in storage exactly,
        // exercising getLong's guard on the read side.
        driver.execute(710, "INSERT INTO nullability_test (id, integer_value) VALUES (1, 9223372036854775807);", 0).await()

        driver.awaitQuery(711, "SELECT integer_value FROM nullability_test WHERE id = 1", { cursor ->
            assertTrue(cursor.next().await())
            assertFailsWith<SQLiteException> { cursor.getLong(0) }
        }, 0)
    }

    /**
     * The second connection gets no busy timeout, SQLITE_BUSY from BEGIN itself shows the write lock is taken up front.
     */
    @Test
    fun begin_immediate_takes_write_lock_at_begin() = kotlinx.coroutines.test.runTest {
        val file = "test_begin_immediate_lock.db"
        deleteIfExists(file)
        try {
            val driver1 = createDriver(file, beginImmediate = true)
            driver1.execute(0, "CREATE TABLE t (id INTEGER)", 0).await()
            val driver2 = createDriver(file, beginImmediate = true)
            // better-sqlite3 defaults to a 5 s busy timeout
            driver2.executeQuery(1, "PRAGMA busy_timeout=0", { QueryResult.AsyncValue { it.next().await() } }, 0).await()

            driver1.newTransaction().await()
            val error = assertFailsWith<SQLiteJsException> { driver2.newTransaction().await() }
            assertEquals(SQLITE_BUSY, error.errorNumber)
            assertNull(driver2.currentTransaction())

            driver1._endTransactionForTests(true)?.await()
            driver2.newTransaction().await()
            driver2._endTransactionForTests(true)?.await()

            driver1.closeSuspending()
            driver2.closeSuspending()
        } finally {
            deleteIfExists(file)
        }
    }

    /**
     * Each connection waits for the busy timeout on its own thread, so the other one can finish its transaction meanwhile.
     */
    @Test
    fun begin_immediate_waits_for_busy_timeout_instead_of_failing_on_lock_upgrade() = kotlinx.coroutines.test.runTest {
        val file = "test_begin_immediate.db"
        deleteIfExists(file)
        try {
            val driver1 = createDriver(file, beginImmediate = true)
            driver1.executeQuery(0, "PRAGMA journal_mode=WAL", { QueryResult.AsyncValue { it.next().await() } }, 0).await()
            driver1.executeQuery(1, "PRAGMA busy_timeout=3000", { QueryResult.AsyncValue { it.next().await() } }, 0).await()
            driver1.execute(2, "CREATE TABLE t (id INTEGER)", 0).await()

            val driver2 = createDriver(file, beginImmediate = true)
            driver2.executeQuery(0, "PRAGMA busy_timeout=3000", { QueryResult.AsyncValue { it.next().await() } }, 0).await()

            driver1.newTransaction().await()
            driver1.execute(3, "INSERT INTO t VALUES (1)", 0).await()

            val driver2Tx = async { driver2.newTransaction().await() }
            delay(100.milliseconds)
            driver1._endTransactionForTests(true)?.await()

            assertNotNull(driver2Tx.await())
            driver2._endTransactionForTests(true)?.await()

            driver1.closeSuspending()
            driver2.closeSuspending()
        } finally {
            listOf(file, "$file-wal", "$file-shm").forEach(::deleteIfExists)
        }
    }

    @Test
    fun long_query_does_not_block_event_loop() = runTest { driver ->
        var ticks = 0
        val timer = setInterval({ ticks++ }, 1)
        try {
            driver.awaitQuery(
                800,
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 2000000) SELECT count(*) FROM c",
                { cursor ->
                    assertTrue(cursor.next().await())
                    assertEquals(2000000L, cursor.getLong(0))
                },
                0,
            )
        } finally {
            clearInterval(timer)
        }
        assertTrue(ticks > 0, "timer never fired while the query ran")
    }

    @Test
    fun closeSuspending_checkpoints_wal_and_is_idempotent() = kotlinx.coroutines.test.runTest {
        val file = "close-wal-test.db"
        listOf(file, "$file-wal", "$file-shm").forEach(::deleteIfExists)
        val driver = createDriver(file, schema = schema)
        try {
            driver.executeQuery(null, "PRAGMA journal_mode=WAL", { cursor -> QueryResult.AsyncValue { cursor.next().await() } }, 0).await()
            driver.execute(null, "INSERT INTO test VALUES (1, 'a')", 0).await()
            assertTrue(fileExists("$file-wal"))

            driver.closeSuspending()
            driver.closeSuspending()
            driver.close()

            assertFalse(fileExists("$file-wal"))
            assertFalse(fileExists("$file-shm"))
        } finally {
            listOf(file, "$file-wal", "$file-shm").forEach(::deleteIfExists)
        }
    }

    @Test
    fun closeSuspending_keeps_process_alive() = runInFreshProcess("closeSuspending_keeps_process_alive") {
        val file = "close-alive-test.db"
        listOf(file, "$file-wal", "$file-shm").forEach(::deleteIfExists)
        try {
            val driver = createDriver(file, schema = schema)
            driver.execute(null, "INSERT INTO test VALUES (1, 'a')", 0).await()
            driver.closeSuspending()
        } finally {
            listOf(file, "$file-wal", "$file-shm").forEach(::deleteIfExists)
        }
    }
}

@Suppress("UnusedParameter")
private external fun setInterval(handler: () -> Unit, timeout: Int): dynamic

@Suppress("UnusedParameter")
private external fun clearInterval(handle: dynamic)
