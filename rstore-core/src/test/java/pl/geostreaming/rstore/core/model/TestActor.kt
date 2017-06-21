package pl.geostreaming.rstore.core.model

import org.junit.Test
import org.nustaq.kontraktor.Actors
import org.nustaq.kontraktor.Callback
import org.nustaq.kontraktor.remoting.encoding.SerializerType
import pl.geostreaming.rstore.core.node.RsNodeActor
import pl.geostreaming.rstore.core.node.impl.RsNodeActorImpl
import org.nustaq.kontraktor.remoting.tcp.TCPNIOPublisher
import org.nustaq.kontraktor.remoting.tcp.TCPConnectable





/**
 * Created by lkolek on 21.06.2017.
 */

class TestActor {

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

        val port = 4001;
        val enc = SerializerType.FSTSer;

        TCPNIOPublisher(act, port)
                .serType(enc)
                .publish().await();

        val con = TCPConnectable(RsNodeActorImpl::class.java, "localhost", port).serType(enc);
        val actRem = con.connect<RsNodeActor>{ _, _ -> println("disconnect") }.await()


        val x = actRem.test1("abc").await();
        println("X:" + x);

        act.stop();

    }
}