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

    @Test
    fun test1() {
        val act = Actors.AsActor(RsNodeActorImpl::class.java)

        val x = act.test1("abc").await();
        println("X:" + x);

        act.stop();
    }

    @Test
    fun test2() {
        val act = Actors.AsActor(RsNodeActorImpl::class.java)
        act.init(1,clDef,"../data/tmp.db")
        val port = 4001;
        val enc = SerializerType.FSTSer;

        val pp = TCPNIOPublisher(act, port)
                .serType(enc)
                .publish().await();

        val con = TCPConnectable(RsNodeActorImpl::class.java, "localhost", port).serType(enc);
        val actRem = con.connect<RsNodeActor>{ _, _ -> println("disconnect") }.await()

        val cfg = act.cfg().await();
        println("cfg local:" + cfg);

        val cfg2 = actRem.cfg().await();
        println("cfg:" + cfg2);

        val x = actRem.test1("abc").await();
        println("X:" + x);

        actRem.close();
        act.stop();
        pp.close().await()
    }

    @Test
    fun test3() {
        val act = Actors.AsActor(RsNodeActorImpl::class.java)
        act.init(1,clDef,"../data/tmp2.db")
        val port = 4001;
        val enc = SerializerType.FSTSer;

        /*
        val pp = TCPNIOPublisher(act, port)
                .serType(enc)
                .publish().await();

        val con = TCPConnectable(RsNodeActorImpl::class.java, "localhost", port).serType(enc);
        val actRem = con.connect<RsNodeActor>{ _, _ -> println("disconnect") }.await()
        */

        val pp = WebSocketPublisher(act,"localhost","/xx",4001)
                .serType(enc)
                .publish().await();
        val con = WebSocketConnectable(RsNodeActorImpl::class.java, "http://localhost:4001/xx").serType(enc);
        val actRem = con.connect<RsNodeActor>{ _, _ -> println("disconnect") }.await()

        println("inserting -------------")

        val ac = AtomicInteger()
        (0..1000).forEach {
            x -> try{
//                actRem.put(("abc" + x +"test").toByteArray(),true).await();
                act.put(("abc" + x +"test").toByteArray(),true).await();
            } catch (ex:Exception ){
                println("x failed:"+x + " - " + ex.message)
            }
        }

        println("queryIds -------------")
        actRem.queryNewIds(0,1000).await().ids.forEachIndexed{ i,x -> println(""+i+ ":" + x.toHexString())}

        println("stop -----------")

        actRem.close();
        act.stop();
        pp.close().await()
    }
}