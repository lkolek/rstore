package pl.geostreaming.rstore.vertx.worker

import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpServer
import io.vertx.core.json.Json
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.sockjs.SockJSHandler
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.ConflatedChannel
import kotlinx.coroutines.experimental.channels.consumeEach
import org.nustaq.serialization.FSTConfiguration
import pl.geostreaming.rstore.core.model.HeartbitData
import pl.geostreaming.rstore.core.node.ReplicaManager
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.experimental.CoroutineContext
import org.nustaq.kson.Kson.conf
import pl.geostreaming.rstore.core.model.IdList
import pl.geostreaming.rstore.core.model.NotThisNode
import kotlin.coroutines.experimental.suspendCoroutine


/**
 * Created by lkolek on 29.06.2017.
 */


class ReplicaVertxNode(
        val vertx:Vertx,
        val repl:ReplicaManager,

        ctx: CoroutineContext? = null
) {
    val router:Router = Router.router(vertx)
    val context = ctx ?: newSingleThreadContext("vertx repl ctx")
    private val lastHeartbit = AtomicReference<HeartbitData>();
    private val base64dec = Base64.getUrlDecoder();
    private val base64enc = Base64.getUrlEncoder();
    private val sockNewIds = SockJSHandler.create(vertx)
    private val sockHeartbit = SockJSHandler.create(vertx)

    private companion object {
        val fstCfg = FSTConfiguration.createDefaultConfiguration()
        init {
            fstCfg.registerClass(HeartbitData::class.java, IdList::class.java,NotThisNode::class.java,Pair::class.java)
        }
    }



    init {
        Json.mapper.registerModule(KotlinModule())
        Json.prettyMapper.registerModule(KotlinModule())


        launch(context){ repl.heartbit().consumeEach { x -> lastHeartbit.set(x) }}

        router.route().handler(BodyHandler.create())


            router.get("/health").produces("plain/text")
                    .handler { r ->
                        launch(context) {
                            val txt = "Health: " + (lastHeartbit.get()?.toString() ?: "not known");
                            with(r.response()) {
                                putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                end(txt)
                            }
                        }
                    }

            router.get("/replId").produces("plain/text").handler { r ->
                with(r.response()) {
                    putHeader(HttpHeaders.CONTENT_TYPE, "plain/text; charset=utf-8")
                    end("" + repl.replId)
                }
            }

            router.get("/get/:oid").produces("application/octet-stream")
                    .handler { r ->
                        try {
//                        println("getting oid: ${r.pathParam("oid")}")
                            val oid = base64dec.decode(r.pathParam("oid"))
                            launch(context) {
                                val obj = repl.get(oid)
                                with(r.response()) {
                                    if (obj != null) {
//                                putHeader(HttpHeaders.CONTENT_TYPE,"application/octet-stream")
                                        end(Buffer.buffer(obj));
                                    } else {
                                        setStatusCode(404).end();
                                    }
                                }
                            }
                        } catch(ex: Exception) {
                            println("Ex during /get: ${ex.message}")
                        }
                    }


            router.post("/put")
                    .consumes("application/octet-stream")
                    .produces("plain/text")
                    .handler { r ->
                        launch(context) {
                            try {
                                val bytes = r.body.bytes
                                val oid = repl.put(bytes)
                                with(r.response()) {
                                    putHeader(HttpHeaders.CONTENT_TYPE, "plain/text; charset=utf-8")
                                    end(base64enc.encodeToString(oid))
                                }
                            } catch (ex: Exception) {
                                println("EX:${ex.message}");
                                ex.printStackTrace();
                            }
                        }
                    }

            router.get("/ids/json/:after/:cnt")
                    .produces("application/json")
                    .handler { r ->
                        launch(context) {
                            try {
                                val after = r.pathParam("after").toLong()
                                val cnt = r.pathParam("cnt").toInt()
                                val ids = repl.queryIds(after, cnt)
                                with(r.response()) {
                                    putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                    end(Json.encode(ids))
                                }
                            } catch (ex: Exception) {
                                println("EX:${ex.message}");
                                ex.printStackTrace();
                            }
                        }
                    }

            router.get("/ids/fst/:after/:cnt")
                    .produces("application/octet-stream")
                    .handler { r ->
                        launch(context) {
                            try {
                                val after = r.pathParam("after").toLong()
                                val cnt = r.pathParam("cnt").toInt()
                                val ids = repl.queryIds(after, cnt)
                                with(r.response()) {
                                    putHeader(HttpHeaders.CONTENT_TYPE, "application/octet-stream")
                                    end(Buffer.buffer(fstCfg.asByteArray(ids)))
                                }
                            } catch (ex: Exception) {
                                println("EX:${ex.message}");
                                ex.printStackTrace();
                            }
                        }
                    }

            router.get("/has/:oid")
                    .produces("plain/text")
                    .handler { r ->
                        launch(context) {
                            try {
                                val oid = base64dec.decode(r.pathParam("oid"))

                                with(r.response()) {
                                    putHeader(HttpHeaders.CONTENT_TYPE, "plain/text; charset=utf-8")
                                    end(if (repl.has(oid)) "true" else "false")
                                }
                            } catch (ex: Exception) {
                                println("EX:${ex.message}");
                                ex.printStackTrace();
                            }
                        }
                    }

            // sockJS: no kotlin/vertx client?

//        sockNewIds.socketHandler { sock ->
//            sock.handler { buff ->   println("Socket received: ${buff}") }
//            launch(context){
//                repl.listenNewIds().consumeEach { x->
//                    sock.write( Json.encode(x) )
//                }
//            }
//        }
//
//        router.route("/newIds/json/*").handler(sockNewIds)
//
//        sockHeartbit.socketHandler { sock ->
//            sock.handler { buff ->   println("Socket received: ${buff}") }
//            launch(context){
//                repl.heartbit().consumeEach { x->
//                    sock.write( Json.encode(x) )
//                }
//            }
//        }
//
//        router.route("/heartbit/json/*").handler(sockNewIds)

            router.route("/newIds/json").handler { r ->
                println("newIds upgrade")
                val socket = r.request().upgrade()
                socket.handler { x -> println("newIds socket received, ignoring") }
                launch(context) {
                    try {
                        println("newIds upgraded")
                        repl.listenNewIds().consumeEach { x ->
                            if (x.seq % 1000L == 0L)
                                println("to send ${x}")
                            while (socket.writeQueueFull()) {
                                delay(20)
                            }
                            socket.writeTextMessage(Json.encode(x))
                        }
                    } catch(ex: Exception) {
                        println("Socket ex ${ex.message}")
                        socket.close()
                    }
                }
            }

            router.route("/heartbit/json").handler { r ->
                val socket = r.request().upgrade()
                socket.handler { x -> println("newIds socket received, ignoring") }
                launch(context) {
                    try {
                        repl.heartbit().consumeEach { x ->
                            while (socket.writeQueueFull()) {
                                delay(20)
                            }
                            socket.writeTextMessage(Json.encode(x))
                        }
                    } catch(ex: Exception) {
                        println("Socket ex ${ex.message}")
                        socket.close()
                    }
                }
            }

    }




}

