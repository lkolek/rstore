package pl.geostreaming.rstore.kontraktor.n2

import org.junit.Test
import org.nustaq.kontraktor.Actors
import org.nustaq.kontraktor.remoting.tcp.TCPConnectable
import org.nustaq.kontraktor.remoting.tcp.TCPNIOPublisher
import pl.geostreaming.rstore.core.model.RsCluster
import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.node.ReplTestBase
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by lkolek on 28.06.2017.
 */

class TestKontraktorReplica : ReplTestBase(){


    @Test
    fun testLocal(){
        val cl = RsCluster(RsClusterDef.Builder()
                .withNode(1,"localhost:4001")
                .withNode(2,"localhost:4002")
                .withReplicationFactor(2)
                .build());

        val r1 = makeReplInmem(1, cl)
        val r2 = makeReplInmem(2, cl)

        val r1Act = Actors.AsActor(RsNodeActorImpl::class.java)
        r1Act.init(r1)
        val r2Act = Actors.AsActor(RsNodeActorImpl::class.java)
        r2Act.init(r2)
        Thread.sleep(100)


//        r1Act.introduce(2, r2Act).await();
//        r2Act.introduce(1, r1Act).await();

        val pp = TCPNIOPublisher(r1Act, 4001)
                //                .serType(enc)
                .publish().await();

        val con = TCPConnectable(RsNodeActor::class.java, "localhost", 4001)
        //                .serType(enc);
        val actRem = con.connect<RsNodeActor>{ _, _ -> println("disconnect") }.await()



        val pp2 = TCPNIOPublisher(r2Act, 4002)
                //                .serType(enc)
                .publish().await();

        val con2 = TCPConnectable(RsNodeActor::class.java, "localhost", 4002)
        //                .serType(enc);
        val actRem2 = con2.connect<RsNodeActor>{ _, _ -> println("disconnect") }.await()




        actRem.introduce( 2, actRem2).await();
        actRem2.introduce(1,actRem).await();

        val pending = AtomicInteger();

        (0..100_000).forEach { x ->
            if(pending.get() > 100) {
                while (pending.get() > 10) {
                    Thread.sleep(100)
                }
            }
            pending.getAndIncrement();
            val xx = actRem.put(randByteArray(10_000), true)
                    .onResult { pending.getAndDecrement() }
                    .onError { pending.getAndDecrement() }
                    .onTimeout { -> pending.getAndDecrement() }

//            pending.getAndIncrement();
//            val xx2 = actRem2.put(randByteArray(1_000), true)
//                    .onResult { pending.getAndDecrement() }
//                    .onError { pending.getAndDecrement() }
//                    .onTimeout { -> pending.getAndDecrement() }
        }
        while(pending.get() > 0){
            Thread.sleep(100)
        }

        println("------- SEND FINISHED-----")



        Thread.sleep(10000)

        r1Act.stop();
        r2Act.stop();

    }


}