package pl.geostreaming.rstore.core.node

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Assert
import org.junit.Test
import pl.geostreaming.rstore.core.model.RsCluster
import pl.geostreaming.rstore.core.model.RsClusterDef
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by lkolek on 27.06.2017.
 */
class ReplicationTest:ReplTestBase() {

    @Test
    fun testRepl(){
        val cl = RsCluster(RsClusterDef.Builder()
                .withNode(1,"localhost:4001")
                .withNode(2,"localhost:4002")
                .withReplicationFactor(2)
                .build());

        val r1 = makeReplInmem(1, cl);
        val r2 = makeReplInmem(2, cl);


        r2.introduceFrom(r1);
        r1.introduceFrom(r2);

        var lastSeq = 0L;
        runBlocking {
            val cnt = AtomicInteger();
//            launch(context)
            launch(CommonPool){
                r1.listenNewIds().consumeEach {
                    println("r1: new id received")
                    cnt.incrementAndGet();
                }
            }
            launch(CommonPool){
                r2.listenNewIds().consumeEach {
                    println("r2: new id received")
                    cnt.incrementAndGet();
                }
            }

            // wait for registration
            delay(100);
            // this should fit before next hartbit


            val last = (1..1000).map { i ->
                r1.put(("" + i + ":lksjlh klfh lakj lkdsdlaks kalsk ").toByteArray());
            }.last();


            var cntr = 0;
            while(! r2.has(last) && cntr++ < 100) {
                delay(200);
            }
            Assert.assertTrue("last id in second replica:", r2.has(last))

            val last2 = (1..1000).map { i ->
                r2.put(("" + i + ":second to first ").toByteArray());
            }.last();

            cntr = 0;
            while(!r1.has(last2)&& cntr++ < 100) {
                delay(200);
            }
            Assert.assertTrue("last2 id in first replica:", r1.has(last2))


            delay(100);
            // wait for delayed commit
        }

        r1.close();
        r2.close();
    }

    @Test
    fun testReplMany(){
        val cl = RsCluster(RsClusterDef.Builder()
                .withNode(1,"localhost:4001")
                .withNode(2,"localhost:4002")
                .withReplicationFactor(2)
                .build());

        val r1 = makeReplInmem(1, cl);
        val r2 = makeReplInmem(2, cl);


        r2.introduceFrom(r1);
        r1.introduceFrom(r2);

        var lastSeq = 0L;
        runBlocking {
            val cnt = AtomicInteger();
//            launch(context)
//            launch(CommonPool){
//                r1.listenNewIds().consumeEach {
//                    println("r1: new id received")
//                    cnt.incrementAndGet();
//                }
//            }
//            launch(CommonPool){
//                r2.listenNewIds().consumeEach {
//                    println("r2: new id received")
//                    cnt.incrementAndGet();
//                }
//            }

            // wait for registration
            delay(100);
            // this should fit before next hartbit


            val last = (1..100_000).map { i ->
                if(i % 1000 == 0){
                    delay(50)
                }
//                val obj = ("" + i + ":lksjlh klfh lakj lkdsdlaks kalsk ").toByteArray()
                val obj = randByteArray(1_000);
                r1.put(obj);
            }.last();


            var cntr = 0;
            while(! r2.has(last) && cntr++ < 100) {
                delay(200);
            }
            Assert.assertTrue("last id in second replica:", r2.has(last))

            val last2 = (1..100_000).map { i ->
                if(i % 1000 == 0){
                    delay(50)
                }
                r2.put(("" + i + ":second to first ").toByteArray());
            }.last();

            cntr = 0;
            while(!r1.has(last2)&& cntr++ < 100) {
                delay(200);
            }
            Assert.assertTrue("last2 id in first replica:", r1.has(last2))


            delay(100);
            // wait for delayed commit
        }

        r1.close();
        r2.close();
    }

//    @Test
    fun testReplManySendToBoth(){
        val cl = RsCluster(RsClusterDef.Builder()
                .withNode(1,"localhost:4001")
                .withNode(2,"localhost:4002")
                .withReplicationFactor(2)
                .build());

        val CNT = 100_000;
//        val RECORD_SIZE = 50_000;
        val RECORD_SIZE = 1_000;

        val r1 = makeReplInmem(1, cl);
        val r2 = makeReplInmem(2, cl);

//        val r1 = makeReplFile(1, cl);
//        val r2 = makeReplFile(2, cl);

        r2.introduceFrom(r1);
        r1.introduceFrom(r2);

        var lastSeq = 0L;
        runBlocking {
            val cnt = AtomicInteger();
//            launch(context)
//            launch(CommonPool){
//                r1.listenNewIds().consumeEach {
//                    println("r1: new id received")
//                    cnt.incrementAndGet();
//                }
//            }
//            launch(CommonPool){
//                r2.listenNewIds().consumeEach {
//                    println("r2: new id received")
//                    cnt.incrementAndGet();
//                }
//            }

            // wait for registration
            delay(100);
            // this should fit before next hartbit


            val last = (1..CNT).map { i ->
                if(i % 100 == 0){
                    delay(10)
                }
//                val obj = ("" + i + ":lksjlh klfh lakj lkdsdlaks kalsk ").toByteArray()
                val obj = randByteArray(RECORD_SIZE);
                r1.put(obj);
                r2.put(obj);
            }.last();


            delay(5000);

            Assert.assertTrue("last id in second replica:", r2.has(last))
            Assert.assertTrue("last id in first replica:", r1.has(last))


            delay(100);
            // wait for delayed commit
        }

        r1.close();
        r2.close();
    }


//    @Test
    fun testReplManyDb(){
        val cl = RsCluster(RsClusterDef.Builder()
                .withNode(1,"localhost:4001")
                .withNode(2,"localhost:4002")
                .withReplicationFactor(2)
                .build());

        val r1 = makeReplFile(1, cl);
        val r2 = makeReplFile(2, cl);


        r2.introduceFrom(r1);
        r1.introduceFrom(r2);

        var lastSeq = 0L;
        runBlocking {
            val cnt = AtomicInteger();
//            launch(context)
//            launch(CommonPool){
//                r1.listenNewIds().consumeEach {
//                    println("r1: new id received")
//                    cnt.incrementAndGet();
//                }
//            }
//            launch(CommonPool){
//                r2.listenNewIds().consumeEach {
//                    println("r2: new id received")
//                    cnt.incrementAndGet();
//                }
//            }

            // wait for registration
            delay(100);
            // this should fit before next hartbit


            val last = (1..100_000).map { i ->
                if(i % 1000 == 0){
                    delay(500)
                }
                r1.put(("" + i + ":lksjlh klfh lakj lkdsdlaks kalsk ").toByteArray());
            }.last();


            var cntr = 0;
            while(! r2.has(last) && cntr++ < 100) {
                delay(200);
            }
            Assert.assertTrue("last id in second replica:", r2.has(last))

            val last2 = (1..100_000).map { i ->
                if(i % 1000 == 0){
                    delay(500)
                }
                r2.put(("" + i + ":second to first ").toByteArray());
            }.last();

            cntr = 0;
            while(!r1.has(last2)&& cntr++ < 100) {
                delay(200);
            }
            Assert.assertTrue("last2 id in first replica:", r1.has(last2))


            delay(100);
            // wait for delayed commit
        }

        r1.close();
        r2.close();
    }
}