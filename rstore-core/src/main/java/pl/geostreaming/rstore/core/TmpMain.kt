package pl.geostreaming.rstore.core

import pl.geostreaming.rstore.core.model.RsCluster
import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.util.toHexString

/**
 * Created by lkolek on 21.06.2017.
 */
class TmpMain {

}

class Test1{
    fun test1( xx: String ){
        println("Testing:" + xx)
    }
}

fun main(args: Array<String>){
    val x = Test1();
//    x.test1(args[0])

    val c = RsClusterDef.Builder()
            .withNode(1,"localhost:4001")
            .withNode(2,"localhost:4002")
            .withNode(3,"localhost:4003")
            .withNode(4,"localhost:4004")
            .withNode(5,"localhost:4005")
            .withReplicationFactor(2)
            .build()

    c.dump()

    println("----")
    println(c)

    println("----")
    val cc = RsCluster(c);

    val xx = "akjshdjhgakd".toByteArray();
    val xxId = cc.objectId(xx);
    val xxSlot = cc.slotForId(xxId)

    println("obj: {" + xx.toHexString() + "}\n id:"+ xxId.toHexString() + "\n slot:" + xxSlot)

}