package pl.geostreaming.rstore.kontraktor.node

import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import org.mapdb.Atomic
import org.mapdb.BTreeMap
import org.mapdb.DB
import org.mapdb.HTreeMap
import pl.geostreaming.rstore.core.model.*
import kotlin.coroutines.experimental.CoroutineContext

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

    suspend fun delayedCommit(){
        if(!toCommit){
            launch( context ) {
                toCommit = true;
                delay(COMMIT_DELAY);
                db.commit();
            }
        }
    }

    /**
     * Gets given object (if avaible)
     */
    suspend override fun get(oid: ObjId)=objs.get(oid);

    /**
     * Internal put. Object belongs to that replica (checked).
     * Should be called in replica thread
     */
    suspend override fun put(obj: ByteArray): ObjId {
        val oid = cl.objectId(obj);
        if(!cl.isReplicaForObjectId(oid, replId)){
            throw NotThisNode("Put not for this node")
        }
        val seqId = seq.incrementAndGet();
        objs.put(oid,obj);
        seq2id.put(seqId,oid);

        // TODO: should we wait for ending ?
        delayedCommit();

        return oid;
    }

    /**
     * Query for ids after given seqId, acording to local ordering.
     */
    suspend override fun queryIds(afertSeqId: Long, cnt: Int): IdList {
        val r1 = ArrayList(
                seq2id.tailMap(afertSeqId,false).entries.take(cnt).map { x -> Pair(x.key,x.value) }
        );
        return IdList(r1, afertSeqId, seq.get());
    }



}

