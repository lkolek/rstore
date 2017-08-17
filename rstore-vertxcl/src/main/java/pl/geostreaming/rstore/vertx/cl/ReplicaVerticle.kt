package pl.geostreaming.rstore.vertx.cl

import io.vertx.core.*
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.ReadStream
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newSingleThreadContext
import kotlinx.coroutines.experimental.selects.select
import org.mapdb.DB
import org.nustaq.serialization.FSTConfiguration
import pl.geostreaming.rstore.core.channels.ChanneledRemote
import pl.geostreaming.rstore.core.channels.ChanneledReplica
import pl.geostreaming.rstore.core.model.*
import pl.geostreaming.rstore.core.msg.*
import pl.geostreaming.rstore.core.node.ReplicaInstance
import pl.geostreaming.rstore.core.node.ReplicaManager
import pl.geostreaming.rstore.core.node.ReplicaMapdbImpl
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.suspendCoroutine


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


class ReplicaVerticle (
        val busAddr:String,
        val cl:RsCluster,
        val replId:Int,
        val db: DB
//        ,
//        val repl: ReplicaInstance,
//        val cocontext: CoroutineContext = newSingleThreadContext("ReplicaVerticle r${replId}")
) :AbstractVerticle() {

    fun runOnContext(block: suspend ()->Unit){
        context.runOnContext{
            launchFuture { block.invoke() }
        }
    }

    val cocontext = CurrentVertx as CoroutineContext

    lateinit var repl:ReplicaInstance
        private set

    private lateinit var chRepl:ChanneledReplica<(RsOpResp)->Unit>

    private companion object {
        val fstCfg = fstBinary.fstCfg

        suspend fun <T> ReadStream<T>.forEach(block: suspend (T) -> Unit) {
            suspendCoroutine<Unit> { cont: Continuation<Unit> ->
                handler { handler ->
                    pause()
                    launchFuture {
                        block(handler)
                    }.setHandler { asyncResult ->
                        if (asyncResult.succeeded()) {
                            resume()
                        } else {
                            // remove handler
                            handler(null)
                            cont.resumeWithException(asyncResult.cause())
                        }
                    }
                }
                exceptionHandler { cont.resumeWithException(it) }
                endHandler { cont.resume(Unit) }
            }
        }
    }

    override fun init(vertx: Vertx?, context: Context?) {
        super.init(vertx, context)

        // init replica providing proper context
        repl = ReplicaMapdbImpl(replId,cl,db,CurrentVertx)
        chRepl = ChanneledReplica<(RsOpResp)->Unit>(repl, CurrentVertx)

    }

    override fun start(startFuture: Future<Void>?) {

        launch(CurrentVertx) {
            println("consuming channell")
            chRepl.outbox.consumeEach { (op, handler) ->
//                println("channell received: ${op}")
                handler.invoke(op);
            }
        }

        launch(CurrentVertx) {
            val eb = vertx.eventBus();
            repl.heartbit().consumeEach { hb ->
                eb.publish(busAddr+"/heartbit", Buffer.buffer(fstCfg.asByteArray( RsHeartbit( hb )) )  );
            }
        }




        launch(CurrentVertx){
            val eb = vertx.eventBus();
            val connected = HashMap<Int, EventBusRemoteRepl>();
            val con = eb.consumer<Buffer>(busAddr+"/heartbit")
            println("installing new ids publisher at: ${nodeAddr(repl.replId) + "/newids"}")
            val newIdsPubl = eb.publisher<Buffer>(nodeAddr(repl.replId) + "/newids")

            launch(CurrentVertx) {
                repl.listenNewIds().consumeEach { nr ->
                    newIdsPubl.send(Buffer.buffer(fstCfg.asByteArray(RsOpRes_listenNewIds_send(0, nr))) )
                }
            }

            con.forEach { h ->
                val op = fstCfg.asObject(h.body().bytes) as? RsOp
                when (op) {
                    is RsHeartbit -> {
                        // if not connected, introduce
                        println("heartbit via esb: ${op.hb}")
                        val rrid = op.hb.replId;
                        if ( rrid != replId &&  !connected.containsKey(rrid)) {
                            val rr = EventBusRemoteRepl(rrid, nodeAddr(rrid), CurrentVertx, vertx) {
                                connected.remove(rrid)
                            }
                            repl.introduceFrom(rr.chr);
                            connected.put(rrid, rr)


                        }

                        repl.onHeartbit(op.hb)
                    }
                    else -> {println("Bad! in heartbit handler")}
                }
            }
        }

        launch(CurrentVertx) {
            val eb = vertx.eventBus();

            val con = eb.consumer<Buffer>(nodeAddr(repl.replId))

            println("consuming esb")

            con.forEach { h ->
                val op = fstCfg.asObject(h.body().bytes) as? RsOp
                when (op) {

                    is RsOpReq -> chRepl.inbox.send(op to { r ->
                        h.reply(Buffer.buffer(fstCfg.asByteArray(r)))
                    })
                    else -> h.reply(Buffer.buffer(fstCfg.asByteArray(RsOpRes_bad(0, "Bad"))))
                }
            }
        }

    }

    fun nodeAddr(nodeId:Int) = busAddr + "/nodes/" + nodeId;


    class EventBusRemoteRepl(
            val replId:Int,
            val addr:String,
            val cocontext: CoroutineContext = newSingleThreadContext("remote Chann r${replId}"),
            val vertx: Vertx,
            val discHandler: suspend (EventBusRemoteRepl) -> Unit
    ) {
        val inboxRemote = Channel<RsOpReq>(10)
        val outboxRemote = Channel<RsOpResp>(10)
        val chr = ChanneledRemote(replId, inboxRemote, outboxRemote)


        var listenJob:Job? = null;

        init {
            launch(CurrentVertx) {
                val eb = vertx.eventBus();
                inboxRemote.consumeEach { op ->

                    if(op is RsOpReq_listenNewIds){
                        println("LISTEN NEW IDS req, opid=${op.opid}")
                        // instead of sending back, just install listener

                        listenJob?.cancel();
                        listenJob = launch(CurrentVertx){
                            println("Installing newids handler at ${addr + "/newids"}")
                            eb.consumer<Buffer>( addr + "/newids" ).forEach{ h ->
                                (fstCfg.asObject(h.body().bytes) as? RsOpRes_listenNewIds_send)?.let{x ->
//                                    println("received RsOpRes_listenNewIds_send: ${op}")
                                    outboxRemote.send(x.copy(opid = op.opid))
                                }

                            }
                        }

                    }
                    else {
                        val h = eb.sendAsync<Buffer>(addr, Buffer.buffer(fstCfg.asByteArray(op)))
                        val opr = fstCfg.asObject(h.body().bytes) as? RsOpResp
                        opr?.let { x -> outboxRemote.send(x) }
                    }
                    Unit;
                }
            }


        }
    }
}


