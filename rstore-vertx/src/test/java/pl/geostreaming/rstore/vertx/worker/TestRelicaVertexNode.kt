package pl.geostreaming.rstore.vertx.worker

import io.vertx.core.Vertx
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpServerOptions
import io.vertx.ext.web.Router
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test
import pl.geostreaming.rstore.core.model.RsCluster
import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.node.ReplTestBase
import pl.geostreaming.rstore.vertx.websocket.RemoteVertexWebsocketReplica
import pl.geostreaming.rstore.vertx.websocket.ReplicaVertexWebsocketNode
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by lkolek on 29.06.2017.
 */
class TestRelicaVertexNode : ReplTestBase(){
//    val CNT = 30_000
//    val REC_SIZE = 10_000

    val CNT = 1_000
    val REC_SIZE = 1000_000

//        @Test
    fun test1(){
        val v = Vertx.vertx();
        val s = v.createHttpServer();
        val rr = Router.router(v);


        val cfg = RsClusterDef.Builder().withNode(1,"localhost:8000").build()
        val cl = RsCluster(cfg);
        val r1 = makeReplInmem(1,cl);

        val vr1 = ReplicaVertxNode(v,r1)


        rr.mountSubRouter("/api/rstore", vr1.router)
        s.requestHandler{ rr.accept(it) }.listen(8000)

        // client

//        val sock = SockJS

        val opts = HttpClientOptions().apply {
//            logActivity=true
        }
        println("creting client")
        val httpcl = v.createHttpClient(opts)
        println("creted client")
        httpcl.websocket(8000, "localhost", "/api/rstore/heartbit/json"){ socket ->
            println("Socket connected")
            socket.textMessageHandler { x -> println("socket received: ${x}") }
            socket.binaryMessageHandler { x -> println("socket received bin: ${x}")}
        }


        Thread.sleep(600_000);

        s.close();
    }

//    @Test
    fun test2(){
        val cfg = RsClusterDef.Builder()
                .withNode(1,"localhost:8000")
                .withNode(2,"localhost:8001")
                .withReplicationFactor(2)
                .build();

        val cl = RsCluster(cfg);
        val r1 = makeReplFile(1, cl);
        val r2 = makeReplFile(2, cl);
//        val r1 = makeReplInmem(1, cl);
//        val r2 = makeReplInmem(2, cl);

        val v = Vertx.vertx();
        val s = v.createHttpServer();
        val rr = Router.router(v);



        val v2 = Vertx.vertx();
        val s2 = v.createHttpServer();
        val rr2 = Router.router(v);



        val vr1 = ReplicaVertxNode(v,r1, r1.context)
        rr.mountSubRouter("/api/rstore", vr1.router)
        s.requestHandler{ rr.accept(it) }.listen(8000)


        val vr2 = ReplicaVertxNode(v2,r2, r2.context)
        rr2.mountSubRouter("/api/rstore", vr2.router)
        s2.requestHandler{ rr2.accept(it) }.listen(8001);

        println("---- before")
        Thread.sleep(1000)
        println("---- after")

        val r1r = RemoteVertxReplica(v,"localhost",8000,1)
        val r2r = RemoteVertxReplica(v2,"localhost",8001,2)
        Thread.sleep(500)

        r1.introduceFrom(r2r);
        r2.introduceFrom(r1r);
        Thread.sleep(500)

//
//
//        val vr1 = ReplicaVertexWebsocketNode(v,r1,r1.context)
//        rr.mountSubRouter("/api/rstore", vr1.router)
//        s.requestHandler{ rr.accept(it) }.listen(8000)
//
//        val vr2 = ReplicaVertexWebsocketNode(v2,r2)
//        rr2.mountSubRouter("/api/rstore", vr2.router)
//        s2.requestHandler{ rr2.accept(it) }.listen(8001);
//
//        Thread.sleep(500)
//
//        val r1r = RemoteVertexWebsocketReplica(v,"localhost",8000,1)
//        val r2r = RemoteVertexWebsocketReplica(v2,"localhost",8001,2)
//
//        Thread.sleep(500)
//
//        r1.introduceFrom(r2r);
//        r2.introduceFrom(r1r);
//        Thread.sleep(500)
//        val pending = AtomicInteger();


        runBlocking {

            val ll = (1..CNT).map { x ->
//                if (pending.get() > 100) {
//                    while (pending.get() > 10) {
//                        delay(100)
//                    }
//                }
//                pending.getAndIncrement();
                val xx = r2r.put(randByteArray(REC_SIZE))
                xx
            }.last()

            while(!r1.has(ll)) {
                delay(100)
            }


            delay(100)
        }


//        Thread.sleep(600_000);

        s.close();
        s2.close();
        v.close();
        v2.close();

        r1.close()
        r2.close()
    }



//    @Test
    fun test3(){
        val cfg = RsClusterDef.Builder()
                .withNode(1,"localhost:8000")
                .withNode(2,"localhost:8001")
                .withReplicationFactor(2)
                .build();

        val cl = RsCluster(cfg);
        val r1 = makeReplFile(1, cl);
        val r2 = makeReplFile(2, cl);
//        val r1 = makeReplInmem(1, cl);
//        val r2 = makeReplInmem(2, cl);

        val sOpt = HttpServerOptions().apply {
            maxWebsocketMessageSize = ((((REC_SIZE + 1000) /maxWebsocketFrameSize)+1) * maxWebsocketFrameSize)
            println("max size:${maxWebsocketMessageSize}")
        }

        val v = Vertx.vertx();
        val s = v.createHttpServer(sOpt);

        val rr = Router.router(v);



        val v2 = Vertx.vertx();
        val s2 = v.createHttpServer(sOpt);
        val rr2 = Router.router(v);



//        val vr1 = ReplicaVertxNode(v,r1)
//        rr.mountSubRouter("/api/rstore", vr1.router)
//        s.requestHandler{ rr.accept(it) }.listen(8000)
//
//
//        val vr2 = ReplicaVertxNode(v2,r2)
//        rr2.mountSubRouter("/api/rstore", vr2.router)
//        s2.requestHandler{ rr2.accept(it) }.listen(8001);
//
//        println("---- before")
//        Thread.sleep(1000)
//        println("---- after")
//
//        val r1r = RemoteVertxReplica(v,"localhost",8000,1)
//        val r2r = RemoteVertxReplica(v2,"localhost",8001,2)
//        Thread.sleep(1000)
//
//        r1.introduceFrom(r2r);
//        r2.introduceFrom(r1r);
//        Thread.sleep(1000)
//


        val vr1 = ReplicaVertexWebsocketNode(v,r1
//                ,r1.context
        )
        rr.mountSubRouter("/api/rstore", vr1.router)
        s.requestHandler{ rr.accept(it) }.listen(8000)

        val vr2 = ReplicaVertexWebsocketNode(v2,r2
//                ,r2.context
        )
        rr2.mountSubRouter("/api/rstore", vr2.router)
        s2.requestHandler{ rr2.accept(it) }.listen(8001);

        Thread.sleep(500)

        val r1r = RemoteVertexWebsocketReplica(v,"localhost",8000,1)
        val r2r = RemoteVertexWebsocketReplica(v2,"localhost",8001,2)

        Thread.sleep(500)

        r1.introduceFrom(r2r);
        r2.introduceFrom(r1r);
        Thread.sleep(500)
//        val pending = AtomicInteger();


        runBlocking {

            val ll = (0..CNT).map { x ->
                //                if (pending.get() > 100) {
//                    while (pending.get() > 10) {
//                        delay(100)
//                    }
//                }
//                pending.getAndIncrement();
                val xx = r2r.put(randByteArray(REC_SIZE))
                xx
            }.last()

            while(!r1.has(ll)) {
                delay(100)
            }


            delay(100)
        }


//        Thread.sleep(600_000);

        s.close();
        s2.close();
        v.close();
        v2.close();
    }

    @Test
    fun test4(){
        val cfg = RsClusterDef.Builder()
                .withNode(1,"localhost:8000")
                .withNode(2,"localhost:8001")
                .withReplicationFactor(2)
                .build();

        val cl = RsCluster(cfg);
        val r1 = makeReplFile(1, cl);
        val r2 = makeReplFile(2, cl);
//        val r1 = makeReplInmem(1, cl);
//        val r2 = makeReplInmem(2, cl);

        val sOpt = HttpServerOptions().apply {
            maxWebsocketMessageSize = ((((REC_SIZE + 1000) /maxWebsocketFrameSize)+1) * maxWebsocketFrameSize)
            println("max size:${maxWebsocketMessageSize}")
        }

        val v = Vertx.vertx();
        val s = v.createHttpServer(sOpt);

        val rr = Router.router(v);



        val v2 = Vertx.vertx();
        val s2 = v.createHttpServer(sOpt);
        val rr2 = Router.router(v);



//        val vr1 = ReplicaVertxNode(v,r1)
//        rr.mountSubRouter("/api/rstore", vr1.router)
//        s.requestHandler{ rr.accept(it) }.listen(8000)
//
//
//        val vr2 = ReplicaVertxNode(v2,r2)
//        rr2.mountSubRouter("/api/rstore", vr2.router)
//        s2.requestHandler{ rr2.accept(it) }.listen(8001);
//
//        println("---- before")
//        Thread.sleep(1000)
//        println("---- after")
//
//        val r1r = RemoteVertxReplica(v,"localhost",8000,1)
//        val r2r = RemoteVertxReplica(v2,"localhost",8001,2)
//        Thread.sleep(1000)
//
//        r1.introduceFrom(r2r);
//        r2.introduceFrom(r1r);
//        Thread.sleep(1000)
//


        val vr1 = ReplicaVertexWebsocketNode(v,r1
//                ,r1.context
        )
        rr.mountSubRouter("/api/rstore", vr1.router)
        s.requestHandler{ rr.accept(it) }.listen(8000)

        val vr2 = ReplicaVertexWebsocketNode(v2,r2
//                ,r2.context
        )
        rr2.mountSubRouter("/api/rstore", vr2.router)
        s2.requestHandler{ rr2.accept(it) }.listen(8001);

        Thread.sleep(500)

        val r1r = RemoteVertexWebsocketReplica(v,"localhost",8000,1)
        val r2r = RemoteVertexWebsocketReplica(v2,"localhost",8001,2)

        Thread.sleep(500)

        r1.introduceFrom(r2r);
        r2.introduceFrom(r1r);
        Thread.sleep(500)
        val pending = AtomicInteger();


        runBlocking {

            val ll = (0..CNT).map { x ->
                if (pending.get() > 100) {
                    while (pending.get() > 10) {
                        delay(100)
                    }
                }
                pending.getAndIncrement();
                val bb = randByteArray(REC_SIZE)
//                val xx1 = async(CommonPool){ r1r.put(bb)}
                val xx = async(CommonPool){ val r = r2r.put(bb); pending.getAndDecrement(); r}
                xx
            }.last().await()

            while(! (r1.has(ll) && r2.has(ll))) {
                delay(100)
            }


            delay(100)
        }


//        Thread.sleep(600_000);

        s.close();
        s2.close();
        v.close();
        v2.close();
    }

}