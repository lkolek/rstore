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
    fun testNewIds(){
        val cl = RsCluster(RsClusterDef.Builder()
                .withNode(1,"localhost:4001")
                .withNode(2,"localhost:4002")
                .withReplicationFactor(2)
                .build());

        val r1 = makeReplInmem(1, cl);
        val r2 = makeReplInmem(2, cl);


        r2.introduceFrom(r1);

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
            val oid = r1.put("lksjlh klfh lakj lkdsdlaks kalsk ".toByteArray());
            delay(500);
            Assert.assertTrue("should have new Id", cnt.get() == 1)

            Assert.assertTrue("same obj insertion", Arrays.equals(oid, r1.put("lksjlh klfh lakj lkdsdlaks kalsk ".toByteArray())));
            delay(500);
            Assert.assertTrue("should NOT have new Id", cnt.get() == 1)

            val oid2 = r1.put("aksjhdlkahsd klfh lakj lkdsdlaks kalsk ".toByteArray());
            delay(500);
            Assert.assertTrue("should have new Id", cnt.get() == 2)


            delay(3000);
            // wait for delayed commit
        }

        r1.close();
        r2.close();
    }
}