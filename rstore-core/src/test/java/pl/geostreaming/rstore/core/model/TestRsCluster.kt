package pl.geostreaming.rstore.core.model

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.Assert
import org.junit.Test
import pl.geostreaming.rstore.core.util.toHexString

/**
 * Created by lkolek on 21.06.2017.
 */

class TestRsCluster{
    val clDef = RsClusterDef.Builder()
            .withNode(1,"localhost:4001")
            .withNode(2,"localhost:4002")
            .withNode(3,"localhost:4003")
            .withNode(4,"localhost:4004")
            .withNode(5,"localhost:4005")
            .withReplicationFactor(2)
            .withSlotSize(50)
        .build();


    @Test
    fun testClusterJsonSerDe() {

        val mapper = jacksonObjectMapper()
        val def = mapper.writeValueAsString(clDef);
        println("def (json):" + def );

        val cldef2 = mapper.readValue(def,RsClusterDef::class.java)
        println("deserialized:" + cldef2);

        Assert.assertEquals("ser + deser equal",clDef,cldef2)
    }

    @Test
    fun test1() {

        val cc = RsCluster(clDef);

        val slCounter = IntArray(clDef.replSlotSize);
        val slCounter2 = IntArray(clDef.nodes.size);

        (0..10000).forEach {
            val xx = ("akjshdjhgakd" + it + Math.random()).toByteArray();
            val xxId = cc.objectId(xx);
            val xxSlot = cc.slotForId(xxId)
            val xxR = cc.replicasForObjectId(xxId)[0]

            slCounter[xxSlot] ++;

            slCounter2[xxR.id - 1] ++;

            println("obj: {" + xx.toHexString() + "}\n id:" + xxId.toHexString() + "\n slot:" + xxSlot)
        }

        println("---------");
        for(i in 0 until clDef.replSlotSize){ println("" + i + ":" + slCounter[i]) }
        println("---------");
        for(i in 0 until slCounter2.size){ println("" + i + ":" + slCounter2[i]) }
    }


//    @Test
    fun testHashPerformance() {
        val cc = RsCluster(clDef);
        val CNT =100000

        var xxx1 = "lashdkl alkjahlkd lkasd lkashd lkhalskd klashdk hasklhd lkashlkd aklsjd lksakl da d";
        for(i in 0..6) {
            xxx1 += xxx1;
        }

        println("OBJ SIZE > " + xxx1.toByteArray().size)

        val bytesArrs = Array<ByteArray>(200){
            ("akjshdjhgakd" + it + xxx1).toByteArray()
        };

        val ts = System.currentTimeMillis();
        (1..CNT).forEach {
            val xx = bytesArrs.get(it % bytesArrs.size);
            val xxId = cc.objectId(xx);
        }
        val t = System.currentTimeMillis() - ts;
        println("t [ms]= " + t + ", ops/ms:" + ( CNT.toDouble() /  t.toDouble()))
    }
}