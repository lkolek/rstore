package pl.geostreaming.rstore.vertx.worker

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpHeaders
import io.vertx.core.json.Json
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.newSingleThreadContext
import pl.geostreaming.rstore.core.model.IdList
import pl.geostreaming.rstore.core.model.NewId
import pl.geostreaming.rstore.core.model.ObjId
import pl.geostreaming.rstore.core.node.RelicaOpLog
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.suspendCoroutine

/**
 * Created by lkolek on 29.06.2017.
 */


class RemoteVertxReplica (
        val vertx:Vertx,
        val host:String,
        val port:Int,
        override val replId:Int,
        ctx: CoroutineContext? = null

): RelicaOpLog{
    val context = ctx ?: newSingleThreadContext("vx remote")
    private val httpCl =  vertx.createHttpClient(HttpClientOptions().apply {
        defaultHost = host
        defaultPort = port
        //            logActivity=true
        maxPoolSize = 20
    });
    private val base64dec = Base64.getUrlDecoder();
    private val base64enc = Base64.getUrlEncoder();
    private val baseUri = "/api/rstore";

    val getCnt = AtomicInteger();

    suspend override fun get(oid: ObjId)  =suspendCoroutine<ByteArray?>{ x->
        val uri = "${baseUri}/get/${base64enc.encodeToString(oid)}"
        if(getCnt.incrementAndGet() % 1000 == 0)
        println("get request no ${getCnt.get()} ${uri}")
        httpCl.getNow(uri ){res ->
            when(res.statusCode()){
                200 -> res.bodyHandler{ b -> x.resume(b.bytes)}
                404 -> x.resume(null)
                else -> x.resumeWithException(RuntimeException("Http get ex ${res.statusCode()}"))
            }
        }
    }

    suspend override fun put(obj: ObjId)  =suspendCoroutine<ByteArray>{ x->
        httpCl.post(baseUri + "/put"){ res ->
            when(res.statusCode()){
                200 -> res.bodyHandler{ b -> x.resume( base64dec.decode(b.bytes)) }
                else -> x.resumeWithException(RuntimeException("Http post ex ${res.statusCode()}"))
            }
        }
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/octet-stream")
                .write(Buffer.buffer(obj))
                .end()
    }

    suspend override fun queryIds(afertSeqId: Long, cnt: Int)  =suspendCoroutine<IdList>{ x->
        httpCl.getNow(baseUri + "/ids/json/${afertSeqId}/${cnt}"){ res ->
            if(res.statusCode() == 200){
                res.bodyHandler{ b -> x.resume( Json.decodeValue(b,IdList::class.java) )}
            } else {
                x.resumeWithException(RuntimeException("Http get ex ${res.statusCode()}"))
            }
        }
    }

    suspend override fun has(oid: ObjId) =suspendCoroutine<Boolean>{ x->
        httpCl.getNow(baseUri + "/has/" +base64enc.encode(oid) ){res ->
            when(res.statusCode()){
                200 -> res.bodyHandler{ b -> x.resume( "true" ==  b.toString("UTF-8"))}
                else -> x.resumeWithException(RuntimeException("Http get ex ${res.statusCode()}"))
            }
        }
    }

    suspend override fun listenNewIds(): Channel<NewId> {
        val ch = Channel<NewId>(50);
        httpCl.websocket(baseUri + "/newIds/json") { socket ->
            println("/newIds/json connected");
            socket.textMessageHandler { x ->
                val xx = Json.decodeValue(x, NewId::class.java)
//                println("NewId received ${xx}")
                async(context){
                    if(xx.seq %1000L == 0L) println("Sended ${xx}")
                    ch.send( xx )
                }

            }
        }

        return ch;
    }

//    private suspend fun <T> toSuspend( l: ()->
}