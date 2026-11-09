package cz.sazel.sqldelight.node.sqlite.test

import app.cash.sqldelight.async.coroutines.awaitAsList
import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlSchema
import cz.sazel.sqldelight.node.sqlite.SQLiteDriver
import cz.sazel.sqldelight.node.sqlite.executeAsFlow
import cz.sazel.sqldelight.dontuse.HockeyPlayer
import cz.sazel.sqldelight.dontuse.TestDataBaseDontUseQueries
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

typealias Database = cz.sazel.sqldelight._dont_use.TestDatabaseDontUse

/**
 * Tests through SQLDelight generated code, run against every backend like [SQLiteDriverTest].
 */
abstract class DriverWithLibraryTest {
    abstract suspend fun createDriver(filename: String, schema: SqlSchema<QueryResult.AsyncValue<Unit>>): SQLiteDriver

    private lateinit var driver: SQLiteDriver
    private lateinit var playerQueries: TestDataBaseDontUseQueries
    private val dbName = "driver_test.db"

    private fun withDatabase(fn: suspend Database.() -> Unit) = runTest {
        val dbName = this@DriverWithLibraryTest.dbName
        deleteIfExists(dbName)
        try {
            driver = createDriver(dbName, Database.Schema)
            val database = Database(driver)
            playerQueries = database.testDataBaseDontUseQueries
            database.fn()
        } finally {
            driver.close()
            unlinkSync(dbName)
        }
    }

    @Test
    fun testInitialContents() = withDatabase {
        playerQueries.selectAll().awaitAsList().let {
            assertEquals(initialPlayer, it.first())
            assertEquals(1, it.size)
        }
    }

    @Test
    fun testInsert() = withDatabase {
        val insertedPlayer = HockeyPlayer(player_number = 8, full_name = "Teemu Selanne")
        transaction {
            playerQueries.insertFullPlayerObject(insertedPlayer)
        }
        playerQueries.selectAll().awaitAsList().let {
            assertContains(it, initialPlayer)
            assertContains(it, insertedPlayer)
            assertEquals(2, it.size)
        }
    }

    @Test
    fun testInsertWithCancelledTransaction() = withDatabase {
        val insertedPlayer = HockeyPlayer(player_number = 18, full_name = "Saku Koivu")
        try {
            transaction {
                playerQueries.insertFullPlayerObject(insertedPlayer)
                error("Rollback")
            }
        } catch (e: Exception) {
            // do nothing
        } finally {
            playerQueries.selectAll().awaitAsList().let {
                assertContains(it, initialPlayer)
                assertEquals(1, it.size)
            }
        }
    }

    /**
     * Regression: [SQLiteDriver.Transaction.endTransaction] used to restore itself instead of the enclosing
     * transaction, so every transaction after the first one skipped both BEGIN and ROLLBACK and silently committed.
     */
    @Test
    fun testSecondTransactionStillBeginsAndRollsBack() = withDatabase {
        transaction {
            playerQueries.insertFullPlayerObject(HockeyPlayer(player_number = 8, full_name = "Teemu Selanne"))
        }
        assertNull(driver.currentTransaction())

        val rolledBackPlayer = HockeyPlayer(player_number = 18, full_name = "Saku Koivu")
        try {
            transaction {
                playerQueries.insertFullPlayerObject(rolledBackPlayer)
                error("Rollback")
            }
        } catch (e: Exception) {
            // do nothing
        }

        playerQueries.selectAll().awaitAsList().let {
            assertFalse(rolledBackPlayer in it)
            assertContains(it, initialPlayer)
            assertEquals(2, it.size)
        }
    }

    @Test
    fun testMultipleInsertsAndPartialCursorRead() = withDatabase {
        val insertedPlayers = (0..10).map { HockeyPlayer(player_number = 100L + it, full_name = "Player $it") }
        transaction {
            insertedPlayers.forEach {
                playerQueries.insertFullPlayerObject(it)
            }
        }
        playerQueries.selectAll().awaitAsList().take(4).let {
            println(it)
            assertContains(it, initialPlayer)
            assertEquals(4, it.size)
        }
    }

    @Test
    fun testUpdate() = withDatabase {
        val updatedPlayer = initialPlayer.copy(full_name = "Petr Sykora")
        transaction {
            playerQueries.updateName(updatedPlayer.full_name, initialPlayer.player_number)
        }
        playerQueries.selectAll().awaitAsList().let {
            assertContains(it, updatedPlayer)
            assertEquals(1, it.size)
        }
    }

    @Test
    fun testDelete() = withDatabase {
        transaction {
            playerQueries.deletePlayer(initialPlayer.player_number)
        }
        playerQueries.selectAll().awaitAsList().let {
            assertEquals(0, it.size)
        }
    }

    @Test
    fun testExecuteAsFlowReturnsAllRows() = withDatabase {
        val insertedPlayers = (0..4).map { HockeyPlayer(player_number = 200L + it, full_name = "Flow $it") }
        transaction {
            insertedPlayers.forEach { playerQueries.insertFullPlayerObject(it) }
        }
        playerQueries.selectAll().executeAsFlow().toList().let {
            assertContains(it, initialPlayer)
            assertEquals(6, it.size)
        }
    }

    /**
     * Regression: the cursor used to be created eagerly and captured by the returned flow, so collecting it a
     * second time reused an already exhausted cursor and yielded nothing.
     */
    @Test
    fun testExecuteAsFlowIsColdAndCanBeCollectedTwice() = withDatabase {
        val flow = playerQueries.selectAll().executeAsFlow()
        assertEquals(listOf(initialPlayer), flow.toList())
        assertEquals(listOf(initialPlayer), flow.toList())
    }

    @Test
    fun testExecuteAsFlowSupportsPartialCollection() = withDatabase {
        val insertedPlayers = (0..9).map { HockeyPlayer(player_number = 300L + it, full_name = "Partial $it") }
        transaction {
            insertedPlayers.forEach { playerQueries.insertFullPlayerObject(it) }
        }
        assertEquals(3, playerQueries.selectAll().executeAsFlow().take(3).toList().size)
    }

    /**
     * A statement cached by the transaction and left in the middle of its rows has to be reset before
     * the next statement runs, better-sqlite3 refuses to run anything while an iterator is open. More rows than one
     * worker batch keep the iterator open. Re-running the same statement has to start from the first row again.
     */
    @Test
    fun testPartialReadInTransactionDoesNotBlockNextStatements() = withDatabase {
        val insertedPlayers = (0..2099).map { HockeyPlayer(player_number = 1000L + it, full_name = "Busy $it") }
        transaction {
            insertedPlayers.forEach { playerQueries.insertFullPlayerObject(it) }
            assertEquals(3, playerQueries.selectAll().executeAsFlow().take(3).toList().size)
            playerQueries.deletePlayer(initialPlayer.player_number)
            assertEquals(insertedPlayers.size, playerQueries.selectAll().awaitAsList().size)
            assertEquals(insertedPlayers.size, playerQueries.selectAll().awaitAsList().size)
        }
    }

    @Test
    fun testSelectWithLimit() = withDatabase {
        val insertedPlayers = (0..100).map { HockeyPlayer(player_number = 100L + it, full_name = "Player $it") }
        transaction {
            insertedPlayers.forEach {
                playerQueries.insertFullPlayerObject(it)
            }
        }
        val list1 = playerQueries.selectWithLimit(0, 50).awaitAsList()
        assertEquals(50, list1.size)
        val list2 = playerQueries.selectWithLimit(50, 99).awaitAsList()
        assertEquals(52, list2.size)
        val list3 = playerQueries.selectAll().awaitAsList()
        assertEquals(102, list3.size)
    }

    private companion object {
        val initialPlayer = HockeyPlayer(player_number = 15, full_name = "Ryan Getzlaf")
    }
}
