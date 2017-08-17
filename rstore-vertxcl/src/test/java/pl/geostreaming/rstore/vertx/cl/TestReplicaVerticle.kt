package pl.geostreaming.rstore.vertx.cl

import io.vertx.core.Vertx
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test
import pl.geostreaming.rstore.core.model.RsCluster
import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.node.ReplTestBase

/**
 * Created by lkolek on 13.07.2017.
 */
class TestReplicaVerticle: ReplTestBase() {

    @Test
    fun test1() {
        val REC_SIZE = 1_000_000;
        val v = Vertx.vertx();

        val cfg = RsClusterDef.Builder()
                .withNode(1,"localhost:8000")
                .withNode(2,"localhost:8001")
                .withReplicationFactor(2)
                .build();
        val cl = RsCluster(cfg);

        val rv1 = ReplicaVerticle("/repl", cl, 1,makeDb(1, false) );
        val rv2 = ReplicaVerticle("/repl", cl, 2,makeDb(2, false) );

        v.deployVerticle(rv1) { println("rv1 deployed") }


        runBlocking {
            delay(3000);
            // insert some data!

            println("----- inserting")


            rv1.runOnContext {
                (0..1000).forEach { i ->
                    val x1 = rv1.repl.put(randByteArray(REC_SIZE));
//                    delay(1)
//                    val x2 = rv1.repl.put("${i}-aaa-7618237815387153768".toByteArray());
//                    val x3 = rv1.repl.put("${i}-aaa3-7618237815387153768".toByteArray());
                }

                v.deployVerticle(rv2) { println("rv2 deployed") }


            }

            delay(50_000);


        }

        v.close();
    }
}