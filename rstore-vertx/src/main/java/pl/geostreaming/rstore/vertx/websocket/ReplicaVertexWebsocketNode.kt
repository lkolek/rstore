package pl.geostreaming.rstore.vertx.websocket

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.json.Json
import io.vertx.ext.web.Router
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newSingleThreadContext
import org.nustaq.serialization.FSTConfiguration
import pl.geostreaming.rstore.core.model.HeartbitData
import pl.geostreaming.rstore.core.model.IdList
import pl.geostreaming.rstore.core.model.NewId
import pl.geostreaming.rstore.core.model.NotThisNode
import pl.geostreaming.rstore.core.msg.*
import pl.geostreaming.rstore.core.node.ReplicaManager
import kotlin.coroutines.experimental.CoroutineContext

object fstBinary {
    val fstCfg = FSTConfiguration.createDefaultConfiguration()
    init {
        fstCfg.registerClass(HeartbitData::class.java, IdList::class.java, NotThisNode::class.java,Pair::class.java,
                NewId::class.java,

                RsOp::class.java, RsOpReq::class.java,
                RsOpReq_get::class.java, RsOpReq_has::class.java,RsOpReq_listenNewIds::class.java,
                RsOpReq_put::class.java, RsOpReq_queryIds::class.java,

                RsOpResp::class.java,RsOpRes_bad::class.java,
                RsOpRes_get::class.java,RsOpRes_has::class.java,RsOpRes_listenNewIds::class.java, RsOpRes_listenNewIds_send::class.java,
                RsOpRes_put::class.java,RsOpRes_queryIds::class.java)
    }
}

/**
 * Created by lkolek on 01.07.2017.
 */
class ReplicaVertexWebsocketNode(
        val vertx: Vertx,
        val repl: ReplicaManager,
        val context: CoroutineContext = newSingleThreadContext("Chann r${repl.replId}")
        ){


    private companion object {
        val fstCfg = fstBinary.fstCfg
    }
    val router: Router = Router.router(vertx)



    init {
        router.route("/channel").handler { r ->
            println("channel upgrade")
            val socket = r.request().upgrade()
            val ch = ChanneledReplica(repl, context)

            socket.binaryMessageHandler { x ->
                val b= x.bytes

                launch(context) {
                    val op = fstCfg.asObject(b) as? RsOpReq;
                    if(op !== null)
                        ch.inbox.send(op)
                }
            }

            launch(context) {
                ch.outbox.consumeEach { op->
                    val x = fstCfg.asByteArray(op)
                    while (socket.writeQueueFull()) {
                        delay(20)
                    }
                    socket.writeBinaryMessage(Buffer.buffer(x))
                }
            }
        }
    }

}