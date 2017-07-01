package pl.geostreaming.rstore.vertx.websocket

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.json.Json
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newSingleThreadContext
import pl.geostreaming.rstore.core.model.NewId
import pl.geostreaming.rstore.core.msg.ChanneledRemote
import pl.geostreaming.rstore.core.msg.RsOpReq
import pl.geostreaming.rstore.core.msg.RsOpResp
import kotlin.coroutines.experimental.CoroutineContext

/**
 * Created by lkolek on 01.07.2017.
 */
class RemoteVertexWebsocketReplica(
        val vertx: Vertx,
        val host:String,
        val port:Int,

        replId:Int,
        context: CoroutineContext = newSingleThreadContext("remote Chann r${replId}")
):ChanneledRemote(
        replId,
        Channel<RsOpReq>(10),
        Channel<RsOpResp>(10),
        context
)
{
    val inb = inboxRemote as Channel<RsOpReq>;
    val outb = outboxRemote as Channel<RsOpResp>;
    private val baseUri = "/api/rstore";

    private companion object {
        val fstCfg = fstBinary.fstCfg
    }

    private val httpCl =  vertx.createHttpClient(HttpClientOptions().apply {
        defaultHost = host
        defaultPort = port
        //            logActivity=true
        maxPoolSize = 20
    })

    init{
        httpCl.websocket(baseUri + "/channel") { socket ->
            println("/channel connected");

            socket.binaryMessageHandler { x ->
                val opr = fstCfg.asObject(x.bytes) as? RsOpResp
                if(opr != null){
                    async(context){outb.send(opr)}
                }
            }

            launch(context){
                inb.consumeEach { op ->
                    val b = fstCfg.asByteArray(op)
                    socket.writeBinaryMessage(Buffer.buffer(b))
                }
            }

        }

    }

}