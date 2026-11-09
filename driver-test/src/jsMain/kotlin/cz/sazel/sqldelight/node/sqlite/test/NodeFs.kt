@file:Suppress("UnusedParameter")

package cz.sazel.sqldelight.node.sqlite.test

import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine
import kotlin.js.json
import kotlin.test.assertEquals

@JsModule("node:fs")
internal external object NodeFs {
    fun existsSync(path: String): Boolean
    fun unlinkSync(path: String)
    fun readFileSync(path: String, encoding: String): String
}

@JsModule("node:child_process")
private external object ChildProcess {
    fun spawnSync(command: String, args: Array<String>, options: dynamic): dynamic
}

@JsModule("node:url")
private external object NodeUrl {
    fun pathToFileURL(path: String): dynamic
}

private external interface NodeProcess {
    val execPath: String
    val env: dynamic
    var exitCode: Int?
    fun on(event: String, listener: (Int) -> Unit)
}

@JsName("process")
private external val nodeProcess: NodeProcess

fun unlinkSync(path: String) = NodeFs.unlinkSync(path)

fun deleteIfExists(path: String) {
    if (NodeFs.existsSync(path)) NodeFs.unlinkSync(path)
}

fun fileExists(path: String): Boolean = NodeFs.existsSync(path)

private const val CHILD_TIMEOUT_MS = 60_000

/**
 * Mocha's test timeout timer holds the event loop, so a missing ref in the driver only shows up
 * in child process with no test runner around it.
 */
fun Any.runInFreshProcess(testName: String, block: suspend () -> Unit) {
    val suiteName = this::class.simpleName ?: error("Missing test class name")
    if (
        nodeProcess.env.FRESH_PROCESS_SUITE.unsafeCast<String?>() == suiteName &&
        nodeProcess.env.FRESH_PROCESS_TEST.unsafeCast<String?>() == testName
    ) {
        var finished = false
        nodeProcess.on("exit") { code ->
            if (code == 0 && !finished) nodeProcess.exitCode = 2
        }
        suspend {
            block()
            finished = true
        }.startCoroutine(Continuation(EmptyCoroutineContext) { result ->
            result.onFailure {
                console.error(it)
                nodeProcess.exitCode = 1
            }
        })
        return
    }
    val main = JSON.parse<dynamic>(NodeFs.readFileSync("package.json", "utf8")).main.unsafeCast<String>()
    val testModule = NodeUrl.pathToFileURL(main).href
    val script = """
        const suite = process.env.FRESH_PROCESS_SUITE;
        const target = process.env.FRESH_PROCESS_TEST;
        let currentSuite = '';
        globalThis.describe = (name, fn) => {
            const prev = currentSuite;
            currentSuite = name;
            fn();
            currentSuite = prev;
        };
        globalThis.xdescribe = () => {};
        globalThis.it = (name, fn) => { if (currentSuite === suite && name === target) fn(); };
        globalThis.xit = () => {};
        import(process.env.FRESH_PROCESS_MODULE);
    """.trimIndent()
    val env = js("Object.assign({}, process.env)")
    env.FRESH_PROCESS_SUITE = suiteName
    env.FRESH_PROCESS_TEST = testName
    env.FRESH_PROCESS_MODULE = testModule
    val child = ChildProcess.spawnSync(
        nodeProcess.execPath,
        arrayOf("-e", script),
        json("env" to env, "stdio" to "inherit", "timeout" to CHILD_TIMEOUT_MS),
    )
    assertEquals(
        0,
        child.status.unsafeCast<Int?>(),
        "$suiteName.$testName exited before finishing " +
            "(status=${child.status}, signal=${child.signal}, error=${child.error})",
    )
}
