package pl.geostreaming.rstore.core.node

import org.nustaq.kontraktor.*
import org.nustaq.kontraktor.annotations.Local
import org.nustaq.serialization.annotations.Flat
import pl.geostreaming.kt.Open
import pl.geostreaming.rstore.core.model.RsClusterDef
import java.io.Serializable

/**
 * Created by lkolek on 21.06.2017.
 */

class NotThisNode(msg:String):Exception(msg),Serializable;

data class IdList(
        val ids:ArrayList<ByteArray>,
        val afterSeqId:Long,
        val lastSeqId:Long
):Serializable;

data class SyncState(val forReplId:Int, val seqOwn:Int, val seqRemote:Int):Serializable;
data class ReplState(val replId:Int, val sync:ArrayList<SyncState>):Serializable;

/**
 *
 * NOTES:
 *  - currently cluster cfg can't change, but in the future we don't have to kill all nodes to apply SOME changes
 */
@Open
class RsNodeActor : Actor<RsNodeActor>() {

    fun introduce(id:Int, replicaActor:RsNodeActor, own:SyncState ):IPromise<SyncState>
            = reject(RuntimeException("UNIMPLEMENTED"));

    fun test1(test:String):IPromise<String>
            =reject(RuntimeException("UNIMPLEMENTED"));
    fun cfg():IPromise<RsClusterDef>
            =reject(RuntimeException("UNIMPLEMENTED"));
    fun put(obj:ByteArray, onlyThisNode:Boolean):IPromise<ByteArray>
            =reject(RuntimeException("UNIMPLEMENTED"));

    fun queryNewIds(afert:Long, cnt:Int):IPromise<IdList>
            =reject(RuntimeException("UNIMPLEMENTED"));
    fun queryNewIdsFor(replId:Int, after: Long, cnt: Int): IPromise<IdList>
            = reject(RuntimeException("UNIMPLEMENTED"));
    fun get(oid:ByteArray):IPromise<ByteArray>
            = reject(RuntimeException("UNIMPLEMENTED"));


    fun listenIds(cb:Callback<Pair<Long,ByteArray>>){}
}

