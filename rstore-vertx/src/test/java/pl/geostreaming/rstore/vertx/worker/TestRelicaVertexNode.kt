package pl.geostreaming.rstore.vertx.worker

import io.vertx.core.Vertx
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

        val vr1 = ReplicaVertxNode(v,r1,s)


        rr.mountSubRouter("/api/rstore", vr1.router)
        s.requestHandler{ rr.accept(it) }.listen(8000)

        Thread.sleep(600_000);

        s.close();
    }

}