package pl.geostreaming.rstore.vertx.worker

import io.vertx.core.Vertx
import io.vertx.core.http.HttpClientOptions
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
    });
    private val base64dec = Base64.getUrlDecoder();
    private val base64enc = Base64.getUrlEncoder();
    private val baseUri = "/api/rstore";

    suspend override fun get(oid: ObjId)  =suspendCoroutine<ByteArray?>{ x->
        httpCl.getNow(baseUri + "/get/" +base64enc.encode(oid) ){res ->
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
                async(context){
                    ch.send( xx )
                }

            }
        }

        return ch;
    }

//    private suspend fun <T> toSuspend( l: ()->
}