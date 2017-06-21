package pl.geostreaming.rstore.core.model

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
    fun test1() {

        val cc = RsCluster(clDef);

        val slCounter = IntArray(clDef.REPL_SLOT_SIZE);

        (0..10000).forEach {
            val xx = ("akjshdjhgakd" + it).toByteArray();
            val xxId = cc.objectId(xx);
            val xxSlot = cc.slotForId(xxId)

            slCounter[xxSlot] ++;

            println("obj: {" + xx.toHexString() + "}\n id:" + xxId.toHexString() + "\n slot:" + xxSlot)
        }

        println("---------");
        for(i in 0 until clDef.REPL_SLOT_SIZE){ println("" + i + ":" + slCounter[i]) }
    }
}