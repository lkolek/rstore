package pl.geostreaming.rstore.core.node

/**
 * Created by lkolek on 26.06.2017.
 */

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import mu.KLogging
import org.mapdb.DB
import pl.geostreaming.rstore.core.model.*
import pl.geostreaming.rstore.core.util.toHexString
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.collections.ArrayList
import kotlin.coroutines.experimental.CoroutineContext


/**
 * Provides the same value to every receiver waiting for them.
 *
 * Receivers calling [get], [getOrNull] waits for value
 * When calling [getWhenAvaibleOrNull] gets value or null immediatly
 */
class MultiReceiver<T>{
    private val mux=Mutex(true)
    private var t:T? = null;

    fun set(v:T){
        if(t != null){
            throw RuntimeException("value set called second time, not accepted!")
        }
        t = v;
        mux.unlock();
    }

    suspend fun get():T = mux.withLock{ t ?: throw RuntimeException("value not avaible, recevie cancelled") }
    suspend fun getOrNull():T? = mux.withLock{ t }


    // this implementation COULD BE WRONG, returning false because other call gets mutex for getting value!
    fun getIfAvaibleOrNull():T?  = if(mux.tryLock()){ mux.unlock();  t; } else null;
}


/**
 * Gets objects from remote replica.
 *
 * Prevents concurrent retrival of the same objects.
 *
 * All methods are invoked in provided context.
 */
class RetriverWorker(
        val myReplica: RelicaOpLog,
        val context:CoroutineContext
) {
    private val myjob = Job()
    protected val pendingOids = HashMap<OID,MultiReceiver<Boolean>>();


    suspend fun pending() = run(context) {pendingOids.size}
    suspend fun has(oid: OID) = run(context) {pendingOids.contains(oid);}

    /**
     * Calls for replication.
     * Returns after finishing operation (both positive and negative).
     * In case there is one like this pending, it waits for replication
     */
    suspend fun replicate(oid: OID, fromRepl: RelicaOpLog):Boolean  = run(context + myjob) {
        if (!pendingOids.containsKey(oid)) {
            val murec = MultiReceiver<Boolean>()
            pendingOids.put(oid, murec)

//            async(context) {
                try {
                    val obj = fromRepl.get(oid.hash);
                    if (obj != null) {
                        myReplica.put(obj);
                        murec.set(true);
                    } else {
                        // TODO: proper Exception
//                        throw RuntimeException("Object id=${oid.hash} not present in remote");
                        murec.set(false);
                    }
                } finally {
                    // TODO handling exception, set(false)
                    pendingOids.remove(oid);
                }
//            }
            murec.get()
        } else {
            // already replicating: how to wait for finish?
            pendingOids.get(oid)!!.get()
        }
    }

    suspend fun cancel() = run(context) {
        myjob.cancel()
    }
}




/**
 * Replicator: replicates from remote to out own [RelicaOpLog]
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
        val myReplica: RelicaOpLog,
        val remote: RelicaOpLog,
        val retriver: RetriverWorker,
        val concurrentGets:Int,
        protected val db: DB,
        val context:CoroutineContext
) {
    val remoteId = remote.replId;
    private companion object: KLogging()

    /* storage */
    private val lastSeqIdRemoteStorage = db.atomicLong("lastSeqIdRemote.${remoteId}").createOrOpen();
    private val fullyReplicatedToStorage = db.atomicLong("fulltyReplicatedTo.${remoteId}").createOrOpen();


    private val myjob = Job()
    /** last known seqId from remote replica */


    var lastSeqIdRemote = lastSeqIdRemoteStorage.get()
        protected set(value) {if(field < value){ field = value; lastSeqIdRemoteStorage.set(field);}}


    /** last remote seqId that has been fully / continuously replicated */
    var fullyReplicatedTo = fullyReplicatedToStorage.get()
        protected set(value) { if(value > field && value <= lastSeqIdRemote) {field = value; fullyReplicatedToStorage.set(field)}}

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
//        launch(context) {  processHeartbit() }
        /* new ids */
        launch(context) {  processNewOids() }
        launch(context) {  while(true) { queryLackingIds(); delay(100); }}

        /* work on ids */
        launch(context) {  processReplication() }
    }

    private suspend fun processReplication(){

        val toReplChannnel = Channel<Pair<Long,ToReplicate>>();
        val replFinished   = Channel<Pair<Long,ToReplicate>>();

        logger.debug { "init replication from r${remote.replId}" }

        // concurrent replicators
        (1..concurrentGets).forEach {
            launch(context){
                while(!toReplChannnel.isClosedForReceive){
                    val (seq, rd) = toReplChannnel.receive();
                    try {
                        logger.debug { "getting seq ${seq} from r${remote.replId}" }
                        if (retriver.replicate(rd.objId, remote)) {
                            rd.processing = false;
                            rd.replicated = true;
                        } else {
                            rd.processing = false;
                            rd.attemts++;
                        }

                    }catch (ex:Exception){
                        rd.processing = false;
                        rd.attemts++;
                        logger.warn("exception during getting obj ${rd.objId.hash.toHexString()}:  ${ex.message}")
                    }
                    replFinished.send(Pair(seq,rd))
                }
            }
        }

        launch(context){
            while(!replFinished.isClosedForReceive){
                val (seq, rd) = replFinished.receive();
                logger.debug { "stored seq ${seq} from r${remote.replId}" }
                performCleaning();
            }
        }

        // adding to replicators
        while(true){
            performCleaning();
            val part1 = ArrayList( toReplicate
                    .entries
                    .filter { (seg, rd) -> rd is ToReplicate && !rd.replicated && !rd.processing && !retriver.has(rd.objId) }
                    .take(1)
            );
            if(!part1.isEmpty()){
                part1.forEach { (seq,rd) ->
                    logger.debug { "performing replication: ${seq} from r${remote.replId}" }
                    rd as ToReplicate;
                    rd.processing = true;
                    toReplChannnel.send(Pair(seq,rd));
                }
            } else {
                delay(100)
            }

        }

    }



    private suspend fun processNewOids() =  remote.listenNewIds().consumeEach{ (seqId, oid) ->
        logger.debug { "R:${myReplica.replId} new oids from ${remoteId}: ${seqId}" }
        lastSeqIdRemote = seqId // max tested inside
        append(seqId,OID(oid))
    }

    // this should be changed somehow to ReplicaManager
//    private suspend fun processHeartbit() = run(context){
//        remote.heartbit().consumeEach{ hbd ->
//            lastSeqIdRemote = hbd.lastSeq // max tested inside
//        }
//    }

    private suspend fun append(seq:Long,oid:OID){
        if(seq > fullyReplicatedTo && !toReplicate.containsKey(seq)){
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
                while (from < lastSeqIdRemote && from - fullyReplicatedTo < 1000) {
                    val ids = remote.queryIds(from, 500);
                    lastSeqIdRemote= ids.lastSeqId  // max inside
                    ids.ids.forEach { (seq, oid) -> append(seq, OID(oid)) }
                    from = calcLastNotMissing();
                }
            } finally {
                queryingIds = false;
            }
        }
    }


    suspend fun calcLastNotMissing() = run(context){
        var seq = fullyReplicatedTo
        while(seq <= lastSeqIdRemote && toReplicate.containsKey(seq+1)){
            seq++;
        }
        seq;
    }

    protected suspend fun performCleaning() = run(context + myjob){
        var frt = fullyReplicatedTo

        while(toReplicate.size > 0 && toReplicate.firstEntry().key <= frt){
            toReplicate.remove(toReplicate.firstEntry().key)
        }

        while( toReplicate.size >0 &&  toReplicate.firstEntry().key <= frt+1 && toReplicate.firstEntry().value.completed()){
            frt = toReplicate.firstEntry().key;
            toReplicate.remove(toReplicate.firstEntry().key)
        }
        fullyReplicatedTo =frt;
    }


    /**
     * How far replication is behind of src replica
     */
    suspend fun behind() = run(context){this.lastSeqIdRemote - fullyReplicatedTo}

}
