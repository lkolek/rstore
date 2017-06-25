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
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer


/**
 * Created by lkolek on 21.06.2017.
 */

class TestActor {

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

    fun initActor(id:Int, dbLoc:String, useTr:Boolean){
        val act = Actors.AsActor(RsNodeActorImpl::class.java)
        act.init(id,clDef,dbLoc,useTr)
    }



    fun makeAct(id:Int):Pair<RsNodeActor, RsNodeActor>{
        val act = Actors.AsActor(RsNodeActorImpl::class.java)
        act.init(id,clDef,"../data/tmp"+id+".db",true)
        val port = 4000 + id;
        val enc = SerializerType.FSTSer;

        /**/
        val pp = TCPNIOPublisher(act, port)
//                .serType(enc)
                .publish().await();

        val con = TCPConnectable(RsNodeActor::class.java, "localhost", port)
//                .serType(enc);
        val actRem = con.connect<RsNodeActor>{ _, _ -> println("disconnect") }.await()

        return Pair(act,actRem);
    }

//    @Test
    fun test3() {
        val (act,actRem) = makeAct(1);

        /**/

        /*
        val pp = WebSocketPublisher(act,"localhost","/xx",4001)
                .serType(enc)
                .publish().await();
        val con = WebSocketConnectable(RsNodeActorImpl::class.java, "http://localhost:4001/xx").serType(enc);
        val actRem = con.connect<RsNodeActor>{ _, _ -> println("disconnect") }.await()
        */

        actRem.listenIds( Callback{ (s,id), err ->  if(s % 50L == 0L) println("INS: " + s + "->" + id.toHexString() )})
//        actRem.listenIds( Callback{ (s,id), err -> println("INS2: " + s + "->" + id.toHexString() )})

        println("inserting -------------")

        val ac = AtomicInteger()
        var add = "ldkajsdkh lksjad lkjas dlkjahs lkhsalk lkasjhdkjas lkdjalkjhsdklahskljhkl djalks d"
//        (0..4).forEach { add = add+add }
        val xx = ArrayList((0..10_000).map {
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
            ids.forEach { (i, x) -> println("" + i + ":" + x.toHexString()) }

            println("get -----------")
            val id2 = ArrayList(ids.map { x -> Pair(x, actRem.get(x.second)) });

            id2
                    .takeLast(100)
                    .forEachIndexed { i, (_, x) ->
                        x.then { r, err -> println("" + i + ":" + r.toHexString());

                        }.await() }

            from += ids.size;
        }
        println("stop -----------")

        actRem.close();
        act.stop();
//        pp.close().await()
    }


    fun randByteArray(size:Int):ByteArray{
        val ba = ByteArray(size);
        val bb = ByteBuffer.wrap(ba)
        (0 until size/4).forEach { i-> bb.putInt(i, (Math.random() * Int.MAX_VALUE).toInt() ) }
        return ba;
    }

    @Test
    fun test4() {

        val acts = ArrayList((0..5).map{ x -> makeAct(x) });

        val (act,actRem) = acts[0];
        val (act2,actRem2) = acts[1];

        acts.forEachIndexed { i,  (ac,acR) -> acR.listenIds( Callback{ (s,id), err -> if(s % 500L == 0L) println("INS to ${i}: " + s + "->" + id.toHexString() )}) };

        acts.forEachIndexed { i,  (_,acR) ->
            acts.forEachIndexed {j, (_,acR2) ->
                if( i!=j){
                    acR.introduce(j,acR2, 0).await();
                }
            }
        };


        // TODO: don't know seqId, it should be xxxx
//        actRem2.introduce(1, actRem, 0 ).await();
//        actRem.introduce(2, actRem2, 0 ).await();


//        actRem.listenIds( Callback{ (s,id), err -> println("INS2: " + s + "->" + id.toHexString() )})



        val shouldFinished = AtomicBoolean(false);
        val finished = ArrayList( (0..5).map{ AtomicBoolean(false);})

        val ts0 = System.currentTimeMillis();

        (0..5).map{x ->
            acts
                .get(x).second
                .listenHeartbit(Callback { result, error -> if(Actors.isResult(error)) {
                    println("####  HEARTBIT: " + result.replId + ", seqId:" + result.lastSeq + ", toPr=" + result.totalBelow )
                    if(result.totalBelow <= 0L && shouldFinished.get()){
                        println("FINISHED, t=" + (System.currentTimeMillis()-ts0));
                        finished.get(x).set(true)
                    }
                } })
        }
//        actRem2.listenHeartbit(Callback { result, error -> if(Actors.isResult(error)) {
//            println("####  HEARTBIT: " + result.replId + ", toPr=" + result.totalBelow)
//            if(result.totalBelow <= 0L && shouldFinished.get()){
//                println("FINISHED, t=" + (System.currentTimeMillis()-ts0));
//                finished2.set(true)
//            }
//        } })
//        actRem.listenHeartbit(Callback { result, error -> if(Actors.isResult(error)) {
//            println("####  HEARTBIT: " + result.replId + ", toPr=" + result.totalBelow)
//            if(result.totalBelow <= 0L && shouldFinished.get()){
//                println("FINISHED, t=" + (System.currentTimeMillis()-ts0));
//                finished1.set(true)
//            }
//        } })
        println("inserting -------------")

        val ac = AtomicInteger()
        var add = "ldkajsdkh lksjad lkjas dlkjahs lkhsalk lkasjhdkjas lkdjalkjhsdklahskljhkl djalks d"
        (0..4).forEach { add = add+add }


        val cl = RsCluster(clDef)

        val pendingAdds = AtomicInteger();
//        (0..1_230_000).forEach {
        (0..100_000).forEach {
            x ->
//            Thread.sleep(1);

            while(actRem.isMailboxPressured || pendingAdds.get() > 14){
                Actors.yield();
            };
//            val obj = ("abc" + x +"test" + add).toByteArray();
            val obj = randByteArray(1024*100);

            val oid = cl.objectId( obj)

            val all = true;

            if(all) {

                (0..5).forEach { rr ->
                    if (cl.isReplicaForObjectId(oid, rr)) {
                        pendingAdds.incrementAndGet();
                        acts[rr].second.put(obj, true)
                                .timeoutIn(1000)
                                .onResult { pendingAdds.decrementAndGet(); }
                                .onError { pendingAdds.decrementAndGet(); }
                                .onTimeout { x -> pendingAdds.decrementAndGet(); }
                    }

                }
            } else {
                pendingAdds.incrementAndGet();
                acts[ cl.firstReplIdxForObjectId(oid)].second.put(obj, true)
                        .timeoutIn(1000)
                        .onResult { pendingAdds.decrementAndGet(); }
                        .onError { pendingAdds.decrementAndGet(); }
                        .onTimeout { x -> pendingAdds.decrementAndGet(); }
            }
//            if(cl.isReplicaForObjectId( oid,1)) {
//                pendingAdds.incrementAndGet();
//                val ret = Pair(x, actRem.put(obj, true)
//                        .timeoutIn(1000)
//                        .onResult { pendingAdds.decrementAndGet(); }
//                        .onError { pendingAdds.decrementAndGet(); }
//                        .onTimeout { x -> pendingAdds.decrementAndGet(); }
//                )
//            }

//            pendingAdds.incrementAndGet();
//            actRem2.put(obj,true)
//                    .timeoutIn(1000)
//                    .onResult{ pendingAdds.decrementAndGet(); }
//                    .onError{pendingAdds.decrementAndGet();}
//                    .onTimeout{x ->pendingAdds.decrementAndGet();}


        };

        while(pendingAdds.get() > 0){
            Actors.yield();
        };

        val ts1 = System.currentTimeMillis()
        println("=== inserted all to 1, t=" +(ts1-ts0));
        shouldFinished.set(true);


        // wait till finish
        while(!actRem.isEmpty){
            Actors.yield();
        }

        val query = false;

        if(query) {
            var from = 0L;
            for (ixx in 1..10) {
                println("queryIds -------------")
                val ids = ArrayList(actRem.queryNewIds(from, 100_000).await().ids);
                ids.forEach { (i, x) -> println("" + i + ":" + x.toHexString()) }

                println("get -----------")
                val id2 = ArrayList(ids.map { x -> Pair(x, actRem.get(x.second).await()) });

                id2
                        .takeLast(100)
                        .forEachIndexed { i, (_, x) ->
                            println("" + i + ":" + x.toHexString().substring(0, 30));

                        }

//            from += ids.size;
            }
        }

//        println("form 2 ===============")
//        for(ixx in 1..10) {
//            println("queryIds -------------")
//            val ids = ArrayList(actRem2.queryNewIds(from, 100_000).await().ids);
//            ids.forEach { (i, x) -> println("" + i + ":" + x.toHexString().substring(0,20)) }
//
//            println("get -----------")
//            val id2 = ArrayList(ids.map { x -> Pair(x, actRem2.get(x.second)) });
//
//            id2
//                    .takeLast(100)
//                    .forEachIndexed { i, (_, x) ->
//                        x.then { r, err -> println("" + i + ":" + r.toHexString());
//
//                        }.await() }
//
//            from += ids.size;
//        }


        println("waiting -----------")

        while(! (finished.all{ x -> x.get()}))
            Thread.sleep(1_000)
        println("stop -----------")

        acts.forEach { (_,x) -> x.close() }
        acts.forEach { (x) -> x.stop() }

//        actRem.close();
//        act.stop();
//        actRem2.close();
//        act2.stop();
//        pp.close().await()
    }
}
