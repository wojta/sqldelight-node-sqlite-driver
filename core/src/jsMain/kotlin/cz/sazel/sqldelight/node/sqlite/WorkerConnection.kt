package cz.sazel.sqldelight.node.sqlite

import cloud.nst.worker.JobTicket
import cloud.nst.worker.Task
import cloud.nst.worker.ThreadPool
import cloud.nst.worker.Transferred
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.serialization.builtins.serializer
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

private const val OPEN_REQUEST_ID = 0
private const val NO_REPLY = -1
private const val STATEMENT_CACHE_SIZE = 32

@InternalSQLiteDriverApi
const val DEFAULT_BATCH_SIZE = 2048

private val limitClause = Regex("""\sLIMIT\s[^)]*$""", RegexOption.IGNORE_CASE)

/**
 * Opens a connection in its own worker thread. The thread imports [registrarModule], which registers
 * a [SQLiteConnectionTask] as [taskName]. Requests and rows go over a `MessagePort` by structured clone.
 *
 * The port keeps message order and the worker handles requests one by one, so a statement is prepared, bound
 * and run or stepped in a single round-trip, and resetting or finalizing it needs no reply at all.
 */
@InternalSQLiteDriverApi
suspend fun openWorkerConnection(
    taskName: String,
    registrarModule: String,
    filename: String,
    batchSize: Int = DEFAULT_BATCH_SIZE,
): SQLiteConnection {
    require(batchSize > 0) { "batchSize must be positive" }
    val scope = CoroutineScope(SupervisorJob())
    val pool = ThreadPool(count = 1, coroutineScope = scope, registrarModules = listOf(registrarModule))
    pool.start()
    // an idle driver doesn't keep Node.js running, the port is referenced only while a request waits for reply
    pool.unref()
    val channel: dynamic = js("new MessageChannel()")
    val connection = WorkerSQLiteConnection(channel.port1, pool, scope, batchSize)
    try {
        val ticket = pool.transfer(
            taskName, filename, String.serializer(), Unit.serializer(), channel.port2.unsafeCast<Any>(), thread = 0,
        )
        connection.awaitOpen(ticket)
    } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
        connection.shutdown()
        throw e as? SQLiteException ?: SQLiteJsException(e)
    }
    return connection
}

/**
 * Opens a [SQLiteDriver] with a writer and [readers] reader connections, each by [openWorkerConnection].
 * Readers need WAL, so the writer switches the database file to it first.
 * @since 1.1.0
 */
@InternalSQLiteDriverApi
@Suppress("LongParameterList")
suspend fun openWorkerDriver(
    taskName: String,
    registrarModule: String,
    filename: String,
    beginImmediate: Boolean,
    readers: Int,
    batchSize: Int = DEFAULT_BATCH_SIZE,
): SQLiteDriver {
    require(readers >= 0) { "readers must not be negative" }
    require(readers == 0 || !filename.isInMemory()) {
        "readers need a database file, in-memory databases are separate per connection"
    }
    require(batchSize > 0) { "batchSize must be positive" }
    val writer = openWorkerConnection(taskName, registrarModule, filename, batchSize)
    val opened = mutableListOf<SQLiteConnection>()
    try {
        if (readers > 0) writer.exec("PRAGMA journal_mode=WAL")
        coroutineScope {
            repeat(readers) {
                launch { opened += openWorkerConnection(taskName, registrarModule, filename, batchSize) }
            }
        }
    } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
        (opened + writer).forEach { runCatching { it.close() } }
        throw e
    }
    return SQLiteDriver(writer, beginImmediate, opened)
}

private fun String.isInMemory() =
    isEmpty() || this == ":memory:" || startsWith("file::memory:") || "mode=memory" in this

/**
 * Worker side of [openWorkerConnection]: opens the connection with [open] and serves requests
 * of the main thread until it closes the connection.
 */
@InternalSQLiteDriverApi
class SQLiteConnectionTask(private val open: (filename: String) -> SQLiteConnection) :
    Task<String, Unit>(String.serializer(), Unit.serializer()) {

    override suspend fun invoke(input: String) {
        val port: dynamic = checkNotNull(currentCoroutineContext()[Transferred]) { "No port transferred" }.value
        val requests = Channel<dynamic>(Channel.UNLIMITED)
        port.on("message") { request: dynamic -> requests.trySend(request) }

        val server = try {
            ConnectionServer(wrapJsErrors { open(input) })
        } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
            port.postMessage(errorReply(OPEN_REQUEST_ID, e))
            return
        }
        port.postMessage(reply(OPEN_REQUEST_ID, null))

        for (request in requests) {
            val id = request.id.unsafeCast<Int>()
            val op = request.op.unsafeCast<String>()
            try {
                val value = server.handle(op, request)
                if (id != NO_REPLY) port.postMessage(reply(id, value))
                if (op == "close") break
            } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                if (id != NO_REPLY) port.postMessage(errorReply(id, e)) else console.error(e)
            }
        }
    }

    private fun reply(id: Int, value: Any?): dynamic {
        val reply: dynamic = js("({})")
        reply.id = id
        reply.value = value
        return reply
    }

    private fun errorReply(id: Int, e: Throwable): dynamic {
        val error: dynamic = js("({})")
        error.message = e.message
        // the original JS error doesn't survive the thread boundary with its SQLite code
        error.errno = (e as? SQLiteJsException)?.errorNumber ?: -1
        val reply: dynamic = js("({})")
        reply.id = id
        reply.error = error
        return reply
    }
}

private class ConnectionServer(private val connection: SQLiteConnection) {
    private class Prepared(val sql: String, val statement: SQLiteStatement)

    private val statements = mutableMapOf<Int, Prepared>()

    /** Finalized statements kept for reuse by their SQL, oldest first. */
    private val cache = ArrayDeque<Prepared>()

    suspend fun handle(op: String, request: dynamic): Any? = when (op) {
        "exec" -> connection.exec(request.sql.unsafeCast<String>()).let { null }
        "close" -> close().let { null }
        "reset" -> statements[request.stmt.unsafeCast<Int>()]?.statement?.reset().let { null }
        "finalize" -> release(request.stmt.unsafeCast<Int>()).let { null }
        else -> handleStatement(op, request)
    }

    private suspend fun handleStatement(op: String, request: dynamic): Any? {
        val id = request.stmt.unsafeCast<Int>()
        val sql = request.sql.unsafeCast<String?>()
        val statement = statements[id]?.statement
            ?: sql?.let { prepare(id, it) }
            ?: throw SQLiteException("Unknown statement $id")
        if (request.reset == true) statement.reset()
        request.params.unsafeCast<Array<Any?>?>()?.let { statement.bind(it.toList()) }
        return when (op) {
            "run" -> statement.run().toDouble()
            "all" -> batch(statement.all(), done = true)
            "step" -> stepBatch(statement, request.batch.unsafeCast<Int>())
            else -> throw SQLiteException("Unknown request $op")
        }
    }

    private suspend fun prepare(id: Int, sql: String): SQLiteStatement {
        val index = cache.indexOfLast { it.sql == sql }
        val prepared = if (index >= 0) cache.removeAt(index) else Prepared(sql, connection.prepare(sql))
        statements[id] = prepared
        return prepared.statement
    }

    private suspend fun release(id: Int) {
        val prepared = statements.remove(id) ?: return
        prepared.statement.reset()
        cache.addLast(prepared)
        if (cache.size > STATEMENT_CACHE_SIZE) cache.removeFirst().statement.finalize()
    }

    private suspend fun close() {
        (cache + statements.values).forEach { it.statement.finalize() }
        cache.clear()
        statements.clear()
        connection.close()
    }

    private suspend fun stepBatch(statement: SQLiteStatement, batchSize: Int): dynamic {
        val rows = mutableListOf<Array<dynamic>>()
        var done = false
        while (rows.size < batchSize) {
            val row = statement.step()
            if (row == null) {
                done = true
                break
            }
            rows.add(row)
        }
        return batch(rows.toTypedArray(), done)
    }

    private fun batch(rows: Array<Array<dynamic>>, done: Boolean): dynamic {
        val batch: dynamic = js("({})")
        batch.rows = rows
        batch.done = done
        return batch
    }
}

private class WorkerSQLiteConnection(
    private val port: dynamic,
    private val pool: ThreadPool,
    private val scope: CoroutineScope,
    private val batchSize: Int,
) : SQLiteConnection {
    private val pending = mutableMapOf<Int, CancellableContinuation<dynamic>>()
    private var nextRequestId = OPEN_REQUEST_ID + 1
    private var nextStatementId = 0
    private lateinit var ticket: JobTicket<Unit>
    private var dead = false

    init {
        port.on("message") { reply: dynamic ->
            val continuation = untrack(reply.id.unsafeCast<Int>())
            val error = reply.error
            when {
                continuation == null -> Unit
                error == null -> continuation.resume(reply.value)
                else -> continuation.resumeWithException(SQLiteJsException(remoteError(error)))
            }
        }
    }

    suspend fun awaitOpen(ticket: JobTicket<Unit>) {
        this.ticket = ticket
        scope.launch {
            try {
                ticket.await()
            } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                // the worker thread died, nothing pending gets a reply anymore
                dead = true
                val waiting = pending.values.toList()
                pending.clear()
                port.unref()
                waiting.forEach { it.resumeWithException(SQLiteJsException(e)) }
            }
        }
        suspendCancellableCoroutine<dynamic> { track(OPEN_REQUEST_ID, it) }
    }

    private fun track(id: Int, continuation: CancellableContinuation<dynamic>) {
        if (pending.isEmpty()) port.ref()
        pending[id] = continuation
    }

    private fun untrack(id: Int): CancellableContinuation<dynamic>? =
        pending.remove(id).also { if (pending.isEmpty()) port.unref() }

    suspend fun request(op: String, fill: (dynamic) -> Unit = {}): dynamic = suspendCancellableCoroutine { cont ->
        val id = nextRequestId++
        val request: dynamic = js("({})")
        request.id = id
        request.op = op
        fill(request)
        track(id, cont)
        cont.invokeOnCancellation { untrack(id) }
        try {
            port.postMessage(request)
        } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
            untrack(id)
            cont.resumeWithException(SQLiteJsException(e))
        }
    }

    /** Errors in [fill] or of a closed port are dropped, the worker logs failures of these requests itself. */
    fun send(op: String, fill: (dynamic) -> Unit) {
        val request: dynamic = js("({})")
        request.id = NO_REPLY
        request.op = op
        fill(request)
        runCatching { port.postMessage(request) }
    }

    /** The worker prepares it with the first request that uses it. */
    override suspend fun prepare(sql: String): SQLiteStatement =
        WorkerSQLiteStatement(this, nextStatementId++, sql, batchSize)

    override suspend fun exec(sql: String) {
        request("exec") { it.sql = sql }
    }

    override suspend fun close() {
        // close reply unrefs the port before the worker finishes a task, hold the loop until shutdown
        pool.ref()
        try {
            request("close")
        } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
            if (dead) shutdown() else pool.unref()
            throw e
        }
        try {
            ticket.await()
        } finally {
            shutdown()
        }
    }

    fun shutdown() {
        pool.shutdown()
        port.close()
        scope.cancel()
    }

    @Suppress("UnusedParameter")
    private fun remoteError(error: dynamic): Throwable =
        js("Object.assign(new Error(error.message), { errno: error.errno })").unsafeCast<Throwable>()
}

/**
 * Parameters are sent with the next [run] or [step]. Queries with `LIMIT` fetch all rows at once,
 * others come in batches of [batchSize].
 */
@OptIn(InternalSQLiteDriverApi::class)
private class WorkerSQLiteStatement(
    private val connection: WorkerSQLiteConnection,
    private val id: Int,
    private val sql: String,
    private val batchSize: Int,
) : SQLiteStatement {
    private val fetchAll = limitClause.containsMatchIn(sql)
    private var prepared = false
    private var sent = false
    private var parameters: Array<Any?>? = null
    private var restart = true
    /** The worker has a row iterator in progress, which blocks other statements in better-sqlite3. */
    private var iterating = false
    private var rows: Array<Array<dynamic>>? = null
    private var position = 0
    private var done = false

    override suspend fun bind(parameters: List<Any?>) {
        clearRows()
        // a Kotlin Long doesn't survive structured clone
        this.parameters = parameters.withLongsAsBigInt()
    }

    override suspend fun run(): Long {
        clearRows()
        iterating = false
        return request("run").unsafeCast<Double>().toLong()
    }

    override suspend fun step(): Array<dynamic>? {
        if (!hasBufferedRow() && !done) {
            val batch = request(if (fetchAll) "all" else "step")
            rows = batch.rows.unsafeCast<Array<Array<dynamic>>>()
            position = 0
            done = batch.done.unsafeCast<Boolean>()
            iterating = !done
        }
        return if (hasBufferedRow()) rows!![position++] else null
    }

    override suspend fun reset() {
        clearRows()
        if (iterating) {
            iterating = false
            connection.send("reset") { it.stmt = id }
        }
    }

    override suspend fun finalize() {
        clearRows()
        if (sent) connection.send("finalize") { it.stmt = id }
    }

    private fun hasBufferedRow() = rows?.let { position < it.size } ?: false

    private fun clearRows() {
        rows = null
        position = 0
        done = false
        restart = true
    }

    private suspend fun request(op: String): dynamic {
        val parameters = parameters
        val restart = restart
        this.parameters = null
        this.restart = false
        sent = true
        val reply = connection.request(op) {
            it.stmt = id
            if (!prepared) it.sql = sql
            if (parameters != null) it.params = parameters
            if (restart) it.reset = true
            if (op == "step") it.batch = batchSize
        }
        prepared = true
        return reply
    }
}
