package pl.geostreaming.rstore.core.model

import org.junit.Test
import org.nustaq.kontraktor.Actors
import org.nustaq.kontraktor.Callback
import org.nustaq.kontraktor.remoting.encoding.SerializerType
import pl.geostreaming.rstore.core.node.RsNodeActor
import pl.geostreaming.rstore.core.node.impl.RsNodeActorImpl
import org.nustaq.kontraktor.remoting.tcp.TCPNIOPublisher
import org.nustaq.kontraktor.remoting.tcp.TCPConnectable
import org.nustaq.kontraktor.remoting.websockets.WebSocketConnectable
import org.nustaq.kontraktor.remoting.websockets.WebSocketPublisher
import pl.geostreaming.rstore.core.util.toHexString
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer


/**
 * Created by lkolek on 21.06.2017.
 */

class TestActor {

    val clDef = RsClusterDef.Builder()
            .withNode(1,"localhost:4001")
            .withNode(2,"localhost:4002")
            .withNode(3,"localhost:4003")
            .withNode(4,"localhost:4004")
            .withNode(5,"localhost:4005")
            .withReplicationFactor(2)
            .withSlotSize(50)
            .build();

    fun initActor(id:Int, dbLoc:String, useTr:Boolean){
        val act = Actors.AsActor(RsNodeActorImpl::class.java)
        act.init(id,clDef,dbLoc,useTr)
    }




    @Test
    fun test3() {
        val act = Actors.AsActor(RsNodeActorImpl::class.java)
        act.init(1,clDef,"../data/tmp2.db",false)
        val port = 4001;
        val enc = SerializerType.FSTSer;

        /**/
        val pp = TCPNIOPublisher(act, port)
//                .serType(enc)
                .publish().await();

        val con = TCPConnectable(RsNodeActor::class.java, "localhost", port)
//                .serType(enc);
        val actRem = con.connect<RsNodeActor>{ _, _ -> println("disconnect") }.await()
        /**/

        /*
        val pp = WebSocketPublisher(act,"localhost","/xx",4001)
                .serType(enc)
                .publish().await();
        val con = WebSocketConnectable(RsNodeActorImpl::class.java, "http://localhost:4001/xx").serType(enc);
        val actRem = con.connect<RsNodeActor>{ _, _ -> println("disconnect") }.await()
        */

//        actRem.listenIds( Callback{ (s,id), err -> println("INS: " + s + "->" + id.toHexString() )})
        actRem.listenIds( Callback{ (s,id), err -> println("INS2: " + s + "->" + id.toHexString() )})

        println("inserting -------------")

        val ac = AtomicInteger()
        var add = "ldkajsdkh lksjad lkjas dlkjahs lkhsalk lkasjhdkjas lkdjalkjhsdklahskljhkl djalks d"
//        (0..4).forEach { add = add+add }
        val xx = ArrayList((0..1_0_000).map {
            x -> while(actRem.isMailboxPressured){
            Actors.yield();
        }
            Pair(x,actRem.put(("abc" + x +"test" + add).toByteArray(),true))
        });
        xx
                .reversed().take(100).reversed()
                .forEach{ (i,x) -> try {  val y = x. await(); println(""+ i +":" + y.toHexString()) }catch(ex:Exception){}}


        var from = 0L;
        for(ixx in 1..10) {
            println("queryIds -------------")
            val ids = ArrayList(actRem.queryNewIds(from, 100_000).await().ids);
            ids.forEachIndexed { i, x -> println("" + i + ":" + x.toHexString()) }

            println("get -----------")
            val id2 = ArrayList(ids.map { x -> Pair(x, actRem.get(x)) });

            id2
                    .takeLast(100)
                    .forEachIndexed { i, (_, x) -> x.then { r, err -> println("" + i + ":" + r.toHexString()) }.await() }

            from += ids.size;
        }
        println("stop -----------")

        actRem.close();
        act.stop();
        pp.close().await()
    }
}
