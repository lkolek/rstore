package pl.geostreaming.rstore.kontraktor.node

import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import org.mapdb.Atomic
import org.mapdb.BTreeMap
import org.mapdb.DB
import org.mapdb.HTreeMap
import pl.geostreaming.rstore.core.model.*
import java.util.*
import kotlin.coroutines.experimental.CoroutineContext
import kotlinx.coroutines.experimental.run

/**
 * Created by lkolek on 25.06.2017.
 */


/**
 * Replica worker iterface, implementing non-blocking.
 * (?) Should be called in the same thread - how to make it happen and sure?.
 *
 * Performs basic replica operations / storage.
 *
 * Does not (directly) do any replication / communication stuff.
 */
interface RelicaWorker{
    suspend fun put(obj:ByteArray):ObjId;
    suspend fun queryIds(afertSeqId:Long, cnt:Int):IdList;
    suspend fun get(oid:ObjId):ByteArray?;

}

/**
 * Replica worker implementation using Mapdb.
 * @see ReplicaWorker
 */
class ReplicaWorkerMapdbImpl (
        val replId:Int,
        val cl:RsCluster,
        val db:DB,

        val context:CoroutineContext
) :RelicaWorker {
    val objs: HTreeMap<ObjId, ByteArray>
    val seq:Atomic.Long
    val seq2id: BTreeMap<Long, ByteArray>

    val COMMIT_DELAY:Long = 1500;
    var toCommit = false;

    init{

        objs = db.hashMap("objs")
                .keySerializer(org.mapdb.Serializer.BYTE_ARRAY)
                .valueSerializer(org.mapdb.serializer.SerializerCompressionWrapper(org.mapdb.Serializer.BYTE_ARRAY))
                .valueInline()
                .counterEnable()
                .createOrOpen();

        seq = db.atomicLong("seq").createOrOpen();

        seq2id = db.treeMap("seq2id")
                .keySerializer(org.mapdb.Serializer.LONG_DELTA)
                .valueSerializer(org.mapdb.Serializer.BYTE_ARRAY)
                .counterEnable()
                .createOrOpen();
    }

    suspend fun delayedCommit()=run(context){
        if(!toCommit){
            launch( context ) {
                toCommit = true;
                delay(COMMIT_DELAY);
                db.commit();
            }
        }
    }

    /**
     * Gets given object (if avaible).
     */
    suspend override fun get(oid: ObjId)= run(context) {objs.get(oid);}

    /**
     * Internal put. Object belongs to that replica (checked).
     * Should be called in replica thread
     */
    suspend override fun put(obj: ByteArray): ObjId = run(context) {
        val oid = cl.objectId(obj);
        if(!cl.isReplicaForObjectId(oid, replId)){
            throw NotThisNode("Put not for this node")
        }
        val seqId = seq.incrementAndGet();
        objs.put(oid,obj);
        seq2id.put(seqId,oid);

        // TODO: should we wait for ending ?
        delayedCommit();

        oid;
    }

    /**
     * Query for ids after given seqId, acording to local ordering.
     */
    suspend override fun queryIds(afertSeqId: Long, cnt: Int): IdList = run(context) {
        val r1 = ArrayList(
                seq2id.tailMap(afertSeqId,false).entries.take(cnt).map { x -> Pair(x.key,x.value) }
        );
        IdList(r1, afertSeqId, seq.get());
    }

}

/**
 * Gets objects from remote replica.
 * All methods are invoked in provided context
 */
class RetriverWorker(
        val myReplica:RelicaWorker,
        val context:CoroutineContext
) {
    protected var pending:Int = 0;
    protected val pendingOids = HashSet<OID>();

    suspend fun pending() = run(context) {pending}
    suspend fun has(oid: OID) = run(context) {pendingOids.contains(oid);}

    suspend fun replicate(oid: OID, fromRepl: RelicaWorker)= run(context) {
        if (pendingOids.add(oid)) {
            pending++;
            try {
                val obj = fromRepl.get(oid.hash);
                if (obj != null)
                    myReplica.put(obj)
                else {
                    // TODO: proper Exception
                    throw RuntimeException("Object id=${oid.hash} not present in remote");
                }
            } finally {
                pending--;
                pendingOids.remove(oid);
            }
        } else {
            // already replicating: how to wait for finish?

        }
    }
}

class Replicator(
        val myReplica:RelicaWorker,
        val replId:Int,
        val remote:RelicaWorker,
        val remoteId:Int,
        protected val db: DB,
        val context:CoroutineContext
) {
    protected val lastSeqIdRemote = db.atomicLong("lastSeqIdRemote.${remoteId}").createOrOpen();
    protected val fullyReplicatedTo = db.atomicLong("fulltyReplicatedTo.${remoteId}").createOrOpen();
    protected val toRepl = TreeMap<Long,ReplOp>();

    protected abstract class ReplOp(val seqId:Long){ abstract fun completed():Boolean; };
    protected class NoOp( seqId:Long): ReplOp(seqId){ override fun completed() = true; }
    protected class ToReplicate( seqId:Long,val objId: OID): ReplOp(seqId){
        override fun completed() = replicated;
        var attemts: Int = 0
        var replicated: Boolean = false
        var processing:Boolean = false
    }


    /**
     * How far replication is behind of src replica
     */
    suspend fun behind() = run(context){this.lastSeqIdRemote.get() - fullyReplicatedTo.get()}
}
