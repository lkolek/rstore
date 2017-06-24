package pl.geostreaming.rstore.core.node

import org.nustaq.kontraktor.*
import org.nustaq.kontraktor.annotations.Local
import org.nustaq.serialization.annotations.Flat
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
open class RsNodeActor : Actor<RsNodeActor>() {

    open fun introduce(id:Int, replicaActor:RsNodeActor, own:SyncState ):IPromise<SyncState>
            = reject(RuntimeException("UNIMPLEMENTED"));

    open fun test1(test:String):IPromise<String>
            =reject(RuntimeException("UNIMPLEMENTED"));
    open fun cfg():IPromise<RsClusterDef>
            =reject(RuntimeException("UNIMPLEMENTED"));
    open fun put(obj:ByteArray, onlyThisNode:Boolean):IPromise<ByteArray>
            =reject(RuntimeException("UNIMPLEMENTED"));

    open fun queryNewIds(afert:Long, cnt:Int):IPromise<IdList>
            =reject(RuntimeException("UNIMPLEMENTED"));
    open fun queryNewIdsFor(replId:Int, after: Long, cnt: Int): IPromise<IdList>
            = reject(RuntimeException("UNIMPLEMENTED"));
    open fun get(oid:ByteArray):IPromise<ByteArray>
            = reject(RuntimeException("UNIMPLEMENTED"));


    open fun listenIds(cb:Callback<Pair<Long,ByteArray>>){}
}

