package pl.geostreaming.rstore.vertx.worker

import io.vertx.core.Vertx
import io.vertx.core.http.HttpClientOptions
import io.vertx.ext.web.Router
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test
import pl.geostreaming.rstore.core.model.RsCluster
import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.node.ReplTestBase
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by lkolek on 29.06.2017.
 */
class TestRelicaVertexNode : ReplTestBase(){

//    @Test
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

    @Test
    fun test2(){
        val cfg = RsClusterDef.Builder()
                .withNode(1,"localhost:8000")
                .withNode(2,"localhost:8001")
                .withReplicationFactor(2)
                .build();

        val cl = RsCluster(cfg);

        val v = Vertx.vertx();
        val s = v.createHttpServer();
        val rr = Router.router(v);

        val r1 = makeReplInmem(1,cl);

        val vr1 = ReplicaVertxNode(v,r1)
        rr.mountSubRouter("/api/rstore", vr1.router)
        s.requestHandler{ rr.accept(it) }.listen(8000)


        val v2 = Vertx.vertx();
        val s2 = v.createHttpServer();
        val rr2 = Router.router(v);


        val r2 = makeReplInmem(2,cl);

        val vr2 = ReplicaVertxNode(v2,r2)
        rr2.mountSubRouter("/api/rstore", vr2.router)
        s2.requestHandler{ rr2.accept(it) }.listen(8001);

        println("---- before")
        Thread.sleep(1000)
        println("---- after")

        val r1r = RemoteVertxReplica(v,"localhost",8000,1)
        val r2r = RemoteVertxReplica(v2,"localhost",8001,2)
        Thread.sleep(1000)

        r1.introduceFrom(r2r);
        r2.introduceFrom(r1r);
        Thread.sleep(1000)

//        val pending = AtomicInteger();


        runBlocking {

            val ll = (0..100_000).map { x ->
//                if (pending.get() > 100) {
//                    while (pending.get() > 10) {
//                        delay(100)
//                    }
//                }
//                pending.getAndIncrement();
                val xx = r2.put(randByteArray(10_000))
                xx
            }.last()

            while(!r1.has(ll)) {
                delay(100)
            }


            delay(1000)
        }


//        Thread.sleep(600_000);

        s.close();
    }

}