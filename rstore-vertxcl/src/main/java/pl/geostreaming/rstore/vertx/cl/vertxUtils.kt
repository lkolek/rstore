package pl.geostreaming.rstore.vertx.cl

import io.vertx.core.Future
import io.vertx.core.Handler
import kotlin.coroutines.experimental.*
import io.vertx.core.AsyncResult
import io.vertx.core.Context
import io.vertx.core.eventbus.EventBus
import io.vertx.core.eventbus.Message
import io.vertx.core.impl.VertxImpl
import kotlinx.coroutines.experimental.CancellableContinuation
import kotlinx.coroutines.experimental.Delay
import java.util.concurrent.TimeUnit
/**
 * Created by lkolek on 12.07.2017.
 */





//////////////////////////




class VertxContinuation<in T>(val vertxContext: Context, val cont: Continuation<T>) : Continuation<T> by cont {
    override fun resume(value: T) {
        vertxContext.runOnContext { cont.resume(value) }
    }

    override fun resumeWithException(exception: Throwable) {
        vertxContext.runOnContext { cont.resumeWithException(exception) }
    }
}

object CurrentVertx : AbstractCoroutineContextElement(ContinuationInterceptor), ContinuationInterceptor, Delay {
    val vertxContext: Context
        get() = VertxImpl.context() ?: throw IllegalStateException("Can't use CurrentVertx if not in a vertx-supplied thread")

    override fun <T> interceptContinuation(continuation: Continuation<T>) = VertxContinuation(vertxContext, continuation)

    override fun scheduleResumeAfterDelay(time: Long, unit: TimeUnit, continuation: CancellableContinuation<Unit>) {
        vertxContext.owner().setTimer(unit.toMillis(time)) { continuation.resume(Unit) }
    }
}

inline suspend fun <T> vx(crossinline callback: (Handler<AsyncResult<T>>) -> Unit) = suspendCoroutine<T> { cont ->
    callback(Handler { result: AsyncResult<T> ->
        if (result.succeeded()) {
            cont.resume(result.result())
        } else {
            cont.resumeWithException(result.cause())
        }
    })
}

// wrapper around message in case it's not needed to avoid calling .body() all the time
inline suspend fun <T> vxm(crossinline callback: (Handler<AsyncResult<Message<T>>>) -> Unit) = suspendCoroutine<T> { cont ->
    callback(Handler { result: AsyncResult<Message<T>> ->
        if (result.succeeded()) {
            cont.resume(result.result().body())
        } else {
            cont.resumeWithException(result.cause())
        }
    })
}

// example of a more specific vert.x method wrapper
suspend fun <TReply> EventBus.sendAsync(address: String, obj: Any) = vx<Message<TReply>> {
    send(address, obj, it)
}



///////////////



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
fun <T> launchFuture(context: CoroutineContext = CurrentVertx,
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
