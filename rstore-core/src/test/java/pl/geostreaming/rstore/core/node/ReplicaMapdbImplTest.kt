package pl.geostreaming.rstore.core.node

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.junit.*
import org.mapdb.DBMaker
import pl.geostreaming.rstore.core.model.ObjId
import pl.geostreaming.rstore.core.model.RsCluster
import pl.geostreaming.rstore.core.model.RsClusterDef
import java.io.File
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by lkolek on 27.06.2017.
 */


open class ReplTestBase {
    companion object {
        val location = "../data"

        @BeforeClass @JvmStatic
        fun beforeClass(){
//            System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
            with(File(location)){ Assume.assumeTrue("Data directory exists: ${location}", exists() && isDirectory) }
        }

        val clDef = RsClusterDef.Builder()
                .withNode(0,"localhost:4000")
                .withNode(1,"localhost:4001")
                .withNode(2,"localhost:4002")
                .withNode(3,"localhost:4003")
                .withNode(4,"localhost:4004")
                .withNode(5,"localhost:4005")
                .withReplicationFactor(3)
//            .withSlotSize(50)
                .build();
    }

    @Before
    fun clean(){
        File(location).listFiles().filter { it.name.startsWith("db") } .forEach { f -> f.delete() }
    }

    val rnd = Random()

    fun randByteArray(size:Int):ByteArray{
        val ba = ByteArray(size);
        val bb = ByteBuffer.wrap(ba)
        (0 until size/4).forEach { i-> bb.putInt(i, rnd.nextInt()) }
        return ba;
    }

    fun makeReplInmem(id:Int, cl:RsCluster, noMt:Boolean = false)= ReplicaMapdbImpl(id,cl,
            DBMaker.memoryDirectDB().apply {
                if(noMt){
                    concurrencyDisable()
                }
            }.make());
    fun makeReplFile(id:Int, cl:RsCluster)= ReplicaMapdbImpl(id,cl,
            DBMaker.fileDB(location+"/db${id}.db").apply {
                fileMmapEnable()
                closeOnJvmShutdown()

            }.make());

    fun ObjId.arrEquals( other:ObjId) = Arrays.equals(this,other)

}

class ReplicaMapdbImplTest :ReplTestBase() {



    @Test
    fun testHeartbit(){
        val cl = RsCluster(RsClusterDef.Builder().withNode(1,"localhost:4001").build());

        val r1 = makeReplInmem(1, cl);
        var lastSeq = 0L;
        runBlocking {
            (1..3).forEach { i ->
                val hb =  r1.heartbit().receive(); println("hb ${i}:${hb}");
                lastSeq = hb.lastSeq;
                Assert.assertTrue("lastSeq=0", lastSeq == 0L);
            }
            // this should fit before next hartbit
            val oid = r1.put("lksjlh klfh lakj lkdsdlaks kalsk ".toByteArray());

            val hb =  r1.heartbit().receive();
            Assert.assertTrue(hb.lastSeq > 0)
        }

        r1.close();
    }


    @Test
    fun testNewIds(){
        val cl = RsCluster(RsClusterDef.Builder().withNode(1,"localhost:4001").build());

        val r1 = makeReplInmem(1, cl);
        var lastSeq = 0L;
        runBlocking {
            val cnt = AtomicInteger();
//            launch(context)
            launch(CommonPool)
            {
                r1.listenNewIds().consumeEach {
                    println("new id received")
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
    }


    @Test
    fun testQueryIds(){
        val cl = RsCluster(RsClusterDef.Builder().withNode(1,"localhost:4001").build());

        val r1 = makeReplInmem(1, cl);
        var lastSeq = 0L;
        runBlocking(r1.context) {
            val obj1 = "1 lksjlh klfh lakj lkdsdlaks kalsk ".toByteArray()
            val oid = r1.put(obj1);
            delay(100);
            val ret = r1.queryIds(0,10)
            Assert.assertTrue("should have new Id",oid.arrEquals( ret.ids[0].oid))

            val oid2 = r1.put("2 klfh lakj lkdsdlaks kalsk ".toByteArray());
            delay(100);
            val ret2 = r1.queryIds(0,10);
            Assert.assertTrue("old Id present",oid.arrEquals( ret2.ids[0].oid))
            Assert.assertTrue("new Id present",oid2.arrEquals( ret2.ids[1].oid))
            Assert.assertTrue("new Id present",oid2.arrEquals( r1.queryIds(1,10).ids[0].oid))

            Assert.assertTrue("no more ids",r1.queryIds(2,10).ids.isEmpty())


            Assert.assertTrue("no has oid",r1.has(oid));
            val obj1retr = r1.get(oid)!!;
            Assert.assertTrue("obj1 = get(oid)",obj1.arrEquals(obj1retr))
        }

        r1.close();
    }
}