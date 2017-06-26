package pl.geostreaming.rstore.kontraktor.node

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import org.mapdb.Atomic
import org.mapdb.BTreeMap
import org.mapdb.DB
import org.mapdb.HTreeMap
import pl.geostreaming.rstore.core.model.*
import java.util.*
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.collections.ArrayList

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
    suspend fun listenNewIds():Channel<Pair<Long, ObjId>>;

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
    private val myjob = Job()
    val objs: HTreeMap<ObjId, ByteArray>
    val seq:Atomic.Long
    val seq2id: BTreeMap<Long, ByteArray>

    val COMMIT_DELAY:Long = 1500;
    var toCommit = false;

    private val newIds = Channel<Pair<Long, ObjId>>();
    private val newIdsListeners = ArrayList<Channel<Pair<Long, ObjId>>>();

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


        runBlocking(context){

            launch(context + myjob){
                newIds.consumeEach { id ->
                    newIdsListeners.forEach { r ->
                        if(!r.isClosedForSend) {
                            async(context) {
                                r.send(id)
                            }
                        }
                    }
                }
            }
        }
    }

    suspend override fun listenNewIds():Channel<Pair<Long, ObjId>> = run(context){
        val ret = Channel<Pair<Long, ObjId>>();
        newIdsListeners.add(ret);
        ret;
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

    suspend fun cancel() = run(context) {
        newIdsListeners.forEach { x -> x.close() }
        newIds.close();
        myjob.cancel()
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
    private val myjob = Job()

    protected var pending:Int = 0;
    protected val pendingOids = HashSet<OID>();



    suspend fun pending() = run(context) {pending}
    suspend fun has(oid: OID) = run(context) {pendingOids.contains(oid);}

    suspend fun replicate(oid: OID, fromRepl: RelicaWorker)= run(context + myjob) {
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

    suspend fun cancel() = run(context) {
        myjob.cancel()
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
    private val myjob = Job()
    protected val lastSeqIdRemote = db.atomicLong("lastSeqIdRemote.${remoteId}").createOrOpen();
    protected val fullyReplicatedTo = db.atomicLong("fulltyReplicatedTo.${remoteId}").createOrOpen();
    protected val toReplicate = TreeMap<Long,ReplOp>();

    protected abstract class ReplOp(val seqId:Long){ abstract fun completed():Boolean; };
    protected class NoOp( seqId:Long): ReplOp(seqId){ override fun completed() = true; }
    protected class ToReplicate( seqId:Long,val objId: OID): ReplOp(seqId){
        override fun completed() = replicated;
        var attemts: Int = 0
        var replicated: Boolean = false
        var processing:Boolean = false
    }

    init {
        launch(context) { remote.listenNewIds().consumeEach {
            (seqId, oid) -> append(seqId,OID(oid))
            if(lastSeqIdRemote.get()< seqId){
                lastSeqIdRemote.set(seqId)
            }
            // TODO: perform queryIds if needed?
        } }
    }

    suspend fun calcLastNotMissing() = run(context){
        var seq = fullyReplicatedTo.get();
        val last = lastSeqIdRemote.get();
        while(seq <= last && toReplicate.containsKey(seq+1)){
            seq++;
        }
        seq;
    }

    protected suspend fun performCleaning() = run(context + myjob){
        var frt = fullyReplicatedTo.get();
        while(toReplicate.size > 0 && toReplicate.firstEntry().key <= frt){
            toReplicate.remove(toReplicate.firstEntry().key)
        }
        while( toReplicate.size >0 &&  toReplicate.firstEntry().key <= frt+1 && toReplicate.firstEntry().value.completed()){
            frt = toReplicate.firstEntry().key;
            toReplicate.remove(toReplicate.firstEntry().key)
        }
        fullyReplicatedTo.set(frt);
    }



    protected suspend fun doReplicate() = run(context + myjob){
        var qids = remote.queryIds(fullyReplicatedTo.get(),1000);
        val frt = fullyReplicatedTo.get();
        qids.ids
                .filter { (seq,oid) -> seq > frt && !toReplicate.containsKey(seq) }
                .forEach { (seq,oid) -> append(seq,OID(oid))};

        if(lastSeqIdRemote.get() < qids.lastSeqId){
            lastSeqIdRemote.set(qids.lastSeqId)
        }

        // TODO: check if next part is needed
    }

    protected suspend fun append(seq:Long,oid:OID){

    }

    /**
     * How far replication is behind of src replica
     */
    suspend fun behind() = run(context){this.lastSeqIdRemote.get() - fullyReplicatedTo.get()}
}
