package pl.geostreaming.rstore.core

import pl.geostreaming.rstore.core.model.RsClusterStruct
import pl.geostreaming.rstore.core.model.RsNode

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

    val c = RsClusterStruct.Builder().node(1,"localhost:4001").node(2,"localhost:4002").build()

    c.dump()

    println("----")
    println(c)
}