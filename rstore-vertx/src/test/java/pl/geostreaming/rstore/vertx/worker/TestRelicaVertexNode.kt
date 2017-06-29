package pl.geostreaming.rstore.vertx.worker

import io.vertx.core.Vertx
import io.vertx.core.http.HttpClientOptions
import io.vertx.ext.web.Router
import org.junit.Test
import pl.geostreaming.rstore.core.model.RsCluster
import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.node.ReplTestBase

/**
 * Created by lkolek on 29.06.2017.
 */
class TestRelicaVertexNode : ReplTestBase(){

    @Test fun test1(){
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

}