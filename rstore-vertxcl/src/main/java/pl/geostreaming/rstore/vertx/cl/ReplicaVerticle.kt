package pl.geostreaming.rstore.vertx.cl

import io.vertx.core.AbstractVerticle
import io.vertx.core.Context
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.launch
import org.nustaq.serialization.FSTConfiguration
import pl.geostreaming.rstore.core.channels.ChanneledReplica
import pl.geostreaming.rstore.core.model.HeartbitData
import pl.geostreaming.rstore.core.model.IdList
import pl.geostreaming.rstore.core.model.NewId
import pl.geostreaming.rstore.core.model.NotThisNode
import pl.geostreaming.rstore.core.msg.*
import pl.geostreaming.rstore.core.node.ReplicaManager


/**
 * Created by lkolek on 10.07.2017.
 */


object fstBinary {
    val fstCfg = FSTConfiguration.createDefaultConfiguration()
    init {
        fstCfg.registerClass(HeartbitData::class.java, IdList::class.java, NotThisNode::class.java,Pair::class.java,
                NewId::class.java,

                RsOp::class.java, RsOpReq::class.java,
                RsOpReq_get::class.java, RsOpReq_has::class.java, RsOpReq_listenNewIds::class.java,
                RsOpReq_put::class.java, RsOpReq_queryIds::class.java,

                RsOpResp::class.java, RsOpRes_bad::class.java,
                RsOpRes_get::class.java, RsOpRes_has::class.java, RsOpRes_listenNewIds::class.java, RsOpRes_listenNewIds_send::class.java,
                RsOpRes_put::class.java, RsOpRes_queryIds::class.java)
    }
}


class ReplicaVerticle (val repl: ReplicaManager) :AbstractVerticle() {

    val chRepl = ChanneledReplica<(RsOpResp)->Unit>(repl)

    init {
        launch(chRepl.context){
            chRepl.outbox.consumeEach{ (op,handler) ->
                handler.invoke(op);
            }
        }
    }

    private companion object {
        val fstCfg = fstBinary.fstCfg
    }

    override fun init(vertx: Vertx?, context: Context?) {
        super.init(vertx, context)
    }

    override fun start(startFuture: Future<Void>?) {
        vertx.eventBus().consumer<Buffer>("/base"){ h ->
            val op = fstCfg.asObject(h.body().bytes) as? RsOpReq
            when(op){
                is RsOpReq -> async(chRepl.context){
                    chRepl.inbox.send( op to  { r -> h.reply( Buffer.buffer( fstCfg.asByteArray(r) ))})
                }
                else -> h.reply( Buffer.buffer( fstCfg.asByteArray( RsOpRes_bad(0, "Bad") ) ))
            }

        }

    }
}