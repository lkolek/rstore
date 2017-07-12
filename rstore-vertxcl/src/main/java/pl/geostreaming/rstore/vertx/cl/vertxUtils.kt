package pl.geostreaming.rstore.vertx.cl

import io.vertx.core.Future
import io.vertx.core.Handler
import kotlin.coroutines.experimental.*

/**
 * Created by lkolek on 12.07.2017.
 */


/**
 * Vert.x Future adapter for Kotlin coroutine continuation
 */
private class VertxFutureCoroutine<T>(override val context: CoroutineContext, private val delegate: Future<T> = Future.future()) :
        Future<T> by delegate, Continuation<T> {

    override fun resume(value: T) = delegate.complete(value)

    override fun resumeWithException(exception: Throwable) = delegate.fail(exception)
}

/**
 * Create a coroutine
 */
fun <T> launchFuture(context: CoroutineContext = EmptyCoroutineContext,
                     block: suspend () -> T): Future<T> =
        VertxFutureCoroutine<T>(context).also { futureContinuation ->
            block.startCoroutine(completion = futureContinuation)
        }

/**
 * Await for resume and return result
 */
suspend fun <T> handle(block: (handler: Handler<T>) -> Unit): T =
        suspendCoroutine <T> { cont: Continuation<T> ->
            // handler calls `resume`
            val handler = Handler<T> { cont.resume(it) }
            block(handler)
        }