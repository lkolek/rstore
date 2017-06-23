package pl.geostreaming.rstore.core.node

import org.nustaq.kontraktor.Actor
import org.nustaq.kontraktor.Actors
import org.nustaq.kontraktor.IPromise
import org.nustaq.kontraktor.Promise
import org.nustaq.kontraktor.annotations.Local
import org.nustaq.serialization.annotations.Flat
import pl.geostreaming.rstore.core.model.RsClusterDef
import java.io.Serializable

/**
 * Created by lkolek on 21.06.2017.
 */

class NotThisNode(msg:String):Exception(msg),Serializable;

data class IdList(val ids:ArrayList<ByteArray>):Serializable;

data class SyncState(val forReplId:Int, val seqOwn:Int, val seqRemote:Int):Serializable;
data class ReplState(val replId:Int, val sync:ArrayList<SyncState>):Serializable;

/**
 *
 * NOTES:
 *  - currently cluster cfg can't change, but in the future we don't have to kill all nodes to apply SOME changes
 */
open class RsNodeActor : Actor<RsNodeActor>() {

    // clustering
    open fun introduce(id:Int, replicaActor:RsNodeActor, own:SyncState ):IPromise<SyncState> = reject(RuntimeException("UNIMPLEMENTED"));

    open fun test1(test:String):IPromise<String> {  return Actors.reject(RuntimeException("UNIMPLEMENTED")); }
    open fun cfg():IPromise<RsClusterDef> {  return Actors.reject(RuntimeException("UNIMPLEMENTED")); }
    open fun put(obj:ByteArray, onlyThisNode:Boolean):IPromise<ByteArray> { return Actors.reject(RuntimeException("UNIMPLEMENTED")); }

    open fun queryNewIds(afert:Long, cnt:Int):IPromise<IdList>{ return Actors.reject(RuntimeException("UNIMPLEMENTED")); }
    open fun get(oid:ByteArray):IPromise<ByteArray> { return Actors.reject(RuntimeException("UNIMPLEMENTED")); }
}

