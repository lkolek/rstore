package pl.geostreaming.rstore.core.node

/**
 * Created by lkolek on 26.06.2017.
 */

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import org.mapdb.Atomic
import org.mapdb.BTreeMap
import org.mapdb.DB
import org.mapdb.HTreeMap
import pl.geostreaming.rstore.core.model.*
import java.util.*
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.collections.ArrayList


/**
 * Gets objects from remote replica.
 *
 * Prevents concurrent retrival of the same objects.
 *
 * All methods are invoked in provided context.
 */
class RetriverWorker(
        val myReplica:RelicaWorker,
        val context:CoroutineContext
) {
    private val myjob = Job()

    protected var pending:Int = 0;
    protected val pendingOids = HashMap<OID,Mutex>();




    suspend fun pending() = run(context) {pending}
    suspend fun has(oid: OID) = run(context) {pendingOids.contains(oid);}

    /**
     * Calls for replication.
     * Returns after finishing operation (both positive and negative).
     * In case there is one like this pending, it waits for replication
     */
    suspend fun replicate(oid: OID, fromRepl: RelicaWorker)  = run(context + myjob) {
        if (!pendingOids.containsKey(oid)) {
            val mutex = Mutex()
            pendingOids.put(oid, mutex)

            pending++;
            async(context) {
                mutex.withLock {
                    try {
                        val obj = fromRepl.get(oid.hash);
                        if (obj != null) {
                            myReplica.put(obj);
                        } else {
                            // TODO: proper Exception
                            throw RuntimeException("Object id=${oid.hash} not present in remote");
                        }
                    } finally {
                        pending--;
                        pendingOids.remove(oid);
                    }
                }
            }

            // wait for replcation to finish!
            mutex.lock();
            mutex.unlock();
            // TODO: how to get result? In case of fail, exception will be thrown
        } else {
            // already replicating: how to wait for finish?
            val mutex = pendingOids.get(oid)!!
            mutex.lock();
            mutex.unlock();
            // TODO: how to get result? Do we need it?

        }
    }

    suspend fun cancel() = run(context) {
        myjob.cancel()
    }
}




/**
 * Replicator: replicates from remote to out own [RelicaWorker]
 *
 * Theere are two source of ids to replicate if needed:
 * - channel of (seqId, objId) - after object was added to remote replica,
 * - result of query (on start, after seqId is missing)
 *
 * Object are retrived when:
 * - there is record in queue [toReplicate] for that obj,
 * - myReplica has not that obj,
 * - there is not an on-going replication in RetriverWorker
 *
 * Objects are marked as replicated when:
 * - replica signals it has obj,
 * - (?)there is object in replica (in case signal is lost)
 *
 * Replicator has:
 * - [fullyReplicatedTo] seqId of last fully replicated obj
 * - [lastSeqIdRemote] seqId of last avaible obj
 * - [toReplicate] ordered list (queue) of obj to replicate
 *
 * Queue cleaning:
 * - advance last fully replicated seqId,
 * - remove entries from head
 * - until not replicated or hole exist
 *
 */
class Replicator(
        val myReplica:RelicaWorker,
        val replId:Int,
        val remote:RelicaWorker,
        val remoteId:Int,
        protected val db: DB,
        val context:CoroutineContext
) {


    private val myjob = Job()
    /** last known seqId from remote replica */
    protected val lastSeqIdRemote = db.atomicLong("lastSeqIdRemote.${remoteId}").createOrOpen();
    /** last remote seqId that has been fully / continuously replicated */
    protected val fullyReplicatedTo = db.atomicLong("fulltyReplicatedTo.${remoteId}").createOrOpen();
    /** queue of records to replcate, may contain holes.
     *
     * Also contains NOOP records for objects already in own replica - to fill holes
     */
    protected val toReplicate = TreeMap<Long,ReplOp>();

    protected var queryingIds = false;

    protected abstract class ReplOp(val seqId:Long){ abstract fun completed():Boolean; };
    protected class NoOp( seqId:Long): ReplOp(seqId){ override fun completed() = true; }
    protected class ToReplicate( seqId:Long,val objId: OID): ReplOp(seqId){
        override fun completed() = replicated;
        var attemts: Int = 0
        var replicated: Boolean = false
        var processing:Boolean = false
    }

    init {
        launch(context) {  processHeartbit() }
        /* new ids */
        launch(context) {  processNewOids() }
        launch(context) {  while(true) { queryLackingIds(); delay(100); }}

        /* work on ids */
    }


    private suspend fun processNewOids() =  remote.listenNewIds().consumeEach{ (seqId, oid) ->
        if(lastSeqIdRemote.get()< seqId){
            lastSeqIdRemote.set(seqId)
        }
        append(seqId,OID(oid))
    }
    private suspend fun processHeartbit() = run(context){
        remote.heartbit().consumeEach{ hbd ->
            if(lastSeqIdRemote.get()< hbd.lastSeq ){
                lastSeqIdRemote.set(hbd.lastSeq )
            }
        }
    }

    private suspend fun append(seq:Long,oid:OID){
        if(seq > fullyReplicatedTo.get() && !toReplicate.containsKey(seq)){
            toReplicate.put(seq, if(myReplica.has(oid.hash)) NoOp(seq)
                else ToReplicate(seq, oid)
            )
            // TODO: signal processing
        }
    }

    private suspend fun queryLackingIds()= run(context){
        if(!queryingIds) {
            try {
                queryingIds = true;

                var from = calcLastNotMissing();
                while (from < lastSeqIdRemote.get() && from - fullyReplicatedTo.get() < 1000) {
                    val ids = remote.queryIds(from, 500);
                    if (lastSeqIdRemote.get() < ids.lastSeqId) {
                        lastSeqIdRemote.set(ids.lastSeqId)
                    }
                    ids.ids.forEach { (seq, oid) -> append(seq, OID(oid)) }
                    from = calcLastNotMissing();
                }
            } finally {
                queryingIds = false;
            }
        }
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





    /**
     * How far replication is behind of src replica
     */
    suspend fun behind() = run(context){this.lastSeqIdRemote.get() - fullyReplicatedTo.get()}

}
