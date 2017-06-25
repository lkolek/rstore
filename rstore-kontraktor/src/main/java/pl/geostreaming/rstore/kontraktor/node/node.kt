package pl.geostreaming.rstore.kontraktor.node

import org.nustaq.kontraktor.*
import org.nustaq.kontraktor.annotations.Local
import org.nustaq.serialization.annotations.Flat
import pl.geostreaming.kt.Open
import pl.geostreaming.rstore.core.model.IdList
import pl.geostreaming.rstore.core.model.RsClusterDef
import java.io.Serializable

/**
 * Created by lkolek on 21.06.2017.
 */



data class HeartbitData(val time:Long, val replId:Int, val lastSeq:Long, val totalBelow:Long): java.io.Serializable {
    override fun toString(): String {
        return "HeartbitData(time=$time, replId=$replId, lastSeq=$lastSeq, totalBelow=$totalBelow)"
    }
}

/**
 *
 * NOTES:
 *  - currently cluster cfg can't change, but in the future we don't have to kill all nodes to apply SOME changes
 */
@pl.geostreaming.kt.Open
class RsNodeActor : org.nustaq.kontraktor.Actor<RsNodeActor>() {

    fun introduce(id:Int, replicaActor: pl.geostreaming.rstore.kontraktor.node.RsNodeActor, own:Long ): org.nustaq.kontraktor.IPromise<Long>
            = reject(RuntimeException("UNIMPLEMENTED"));

    fun cfg(): org.nustaq.kontraktor.IPromise<RsClusterDef>
            =reject(RuntimeException("UNIMPLEMENTED"));
    fun put(obj:ByteArray, onlyThisNode:Boolean): org.nustaq.kontraktor.IPromise<ByteArray>
            =reject(RuntimeException("UNIMPLEMENTED"));

    fun queryNewIds(afert:Long, cnt:Int): org.nustaq.kontraktor.IPromise<IdList>
            =reject(RuntimeException("UNIMPLEMENTED"));
//    fun queryNewIdsFor(replId:Int, after: Long, cnt: Int): IPromise<IdList>
//            = reject(RuntimeException("UNIMPLEMENTED"));
    fun get(oid:ByteArray): org.nustaq.kontraktor.IPromise<ByteArray>
            = reject(RuntimeException("UNIMPLEMENTED"));


    fun listenIds(cb: org.nustaq.kontraktor.Callback<Pair<Long, ByteArray>>){}
    fun listenHeartbit(cb: org.nustaq.kontraktor.Callback<HeartbitData>){}
}

