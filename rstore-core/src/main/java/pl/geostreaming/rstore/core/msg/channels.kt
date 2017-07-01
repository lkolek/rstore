package pl.geostreaming.rstore.core.msg

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newSingleThreadContext
import kotlinx.coroutines.experimental.selects.select
import pl.geostreaming.rstore.core.model.IdList
import pl.geostreaming.rstore.core.model.NewId
import pl.geostreaming.rstore.core.model.ObjId
import pl.geostreaming.rstore.core.node.RelicaOpLog
import pl.geostreaming.rstore.core.node.ReplicaManager
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.createCoroutine
import kotlin.coroutines.experimental.suspendCoroutine

/**
 * Created by lkolek on 30.06.2017.
 */

class ChanneledReplica(
        val repl:ReplicaManager,
        val context: CoroutineContext = newSingleThreadContext("Chann r${repl.replId}"),
        val concurrency:Int = 5
){
    private val chIn= Channel<RsOpReq>(10)
    private val chOut= Channel<RsOpResp>(10)

    val inbox: SendChannel<RsOpReq> = chIn;
    val outbox: ReceiveChannel<RsOpResp> = chOut;


    init{
        (1..concurrency).forEach {
            launch(context){
                chIn.consumeEach{ev ->
                    val resp = process(ev)
                    chOut.send(resp)
                }
            }
        }

    }

    private suspend fun process( ev:RsOp ):RsOpResp= when (ev) {
        is RsOpReq_queryIds -> RsOpRes_queryIds(ev.opid, repl.queryIds(ev.afertSeqId, ev.cnt))
        is RsOpReq_put -> RsOpRes_put(ev.opid, repl.put(ev.obj))
        is RsOpReq_get -> RsOpRes_get(ev.opid, repl.get(ev.oid))
        is RsOpReq_has -> RsOpRes_has(ev.opid, repl.has(ev.oid))
        is RsOpReq_listenNewIds -> {
            launch(context){
                repl.listenNewIds().consumeEach{id -> chOut.send(RsOpRes_listenNewIds_send(ev.opid, id))}

            }
            RsOpRes_listenNewIds(ev.opid)
        }
        else -> RsOpRes_bad(ev.opid, "op not supported")
    }
}

open class ChanneledRemote(
        override val replId:Int,
        val inboxRemote: SendChannel<RsOpReq>,
        val outboxRemote: ReceiveChannel<RsOpResp>,
        val context: CoroutineContext = newSingleThreadContext("remote Chann r${replId}")
): RelicaOpLog
{
    private var opseq:Long = 0L
        get() {field++; return field}

    private val handlers = HashMap<Long,suspend  (RsOpResp) ->Unit >()

    init{
        launch(context){
            outboxRemote.consumeEach { x ->
                try {
//                    if(x.opid % 1000L == 0L){
//                        println("consume ${x.opid} handlers=${handlers.size}")
//                    }

                    val hh = handlers.get(x.opid)
                    if(hh != null){
                        hh.invoke(x)
                    } else {
                        println("no handler for: ${x.opid}, ${x.javaClass.name}")
                    }
                }catch(ex:Exception){
                    println("rec remote ex:${ex.message}")
                    ex.printStackTrace()
                }

            }
        }
    }

    private suspend fun <R> RsOpReq.sendAnd(block: suspend (RsOpResp) -> R ):R{
        val cc = CompletableFuture<RsOpResp>()
        handlers.put(this.opid){ x-> cc.complete(x); handlers.remove(this.opid) }
        inboxRemote.send(this);
        val xx1 = cc.await();

        val ret = block.invoke(xx1)
        return ret;
    }


    override suspend fun queryIds(afterSeqId:Long, cnt:Int): IdList =run(context){
        val q = RsOpReq_queryIds(opseq, afterSeqId, cnt);
        q.sendAnd { r-> (r as? RsOpRes_queryIds ?: throw RuntimeException("bad response")).ret }

    }

    override suspend fun put(obj:ByteArray):ObjId =run(context){
        val q = RsOpReq_put(opseq, obj.copyOf());
        q.sendAnd { r-> (r as? RsOpRes_put ?: throw RuntimeException("bad response")).ret }

    }

    override suspend fun get(oid:ObjId):ByteArray? =run(context){
        val q = RsOpReq_get(opseq, oid);
        q.sendAnd { r-> (r as? RsOpRes_get ?: throw RuntimeException("bad response")).ret }

    }

    override suspend fun has(oid:ObjId):Boolean =run(context){
        val q = RsOpReq_has(opseq, oid);
        q.sendAnd { r-> (r as? RsOpRes_has ?: throw RuntimeException("bad response")).ret }

    }

    override suspend fun listenNewIds():Channel<NewId> =run(context){
        val q = RsOpReq_listenNewIds(opseq);
        val ret = Channel<NewId>(100);

        handlers.put(q.opid, { r ->
            when(r){
                is RsOpRes_listenNewIds_send-> ret.send(r.ret)
                is RsOpRes_listenNewIds-> println("listenNewIds installed")
                else -> println("bad listenNewIds")
            }
        })

        inboxRemote.send(q);
        ret;
    }

}