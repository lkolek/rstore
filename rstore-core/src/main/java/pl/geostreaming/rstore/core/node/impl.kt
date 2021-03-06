package pl.geostreaming.rstore.core.node

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.actor
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import mu.KLogging
import org.mapdb.DB
import pl.geostreaming.rstore.core.model.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.collections.ArrayList

/**
 * Created by lkolek on 26.06.2017.
 */


/**
 * Replica worker implementation using Mapdb.
 *
 * Uses mapdb [DB] as data storage.
 *
 * It executes its suspend functions in provided [context] - it should be single thread to provide safety (for now at least).
 *
 */
class ReplicaMapdbImpl (
        override val replId:Int,
        override val cl:RsCluster,
        val db:DB,
        val context:CoroutineContext = newSingleThreadContext("Relica ${replId}")

) : ReplicaInstance {
    private val myjob = Job()

    val objs = db.hashMap("objs")
        .keySerializer(org.mapdb.Serializer.BYTE_ARRAY)
        .valueSerializer(org.mapdb.serializer.SerializerCompressionWrapper(org.mapdb.Serializer.BYTE_ARRAY))
        .valueInline()
        .counterEnable()
        .createOrOpen();
    val seq2id = db.treeMap("seq2id")
            .keySerializer(org.mapdb.Serializer.LONG_DELTA)
            .valueSerializer(org.mapdb.Serializer.BYTE_ARRAY)
            .counterEnable()
            .createOrOpen();
    val seq = db.atomicLong("seq").createOrOpen();

    val COMMIT_DELAY:Long = 1500;
    private var toCommit = false;

    private val newIds = Channel<NewId>(10);
    private val newIdsListeners = ArrayList<Channel<NewId>>();
    private val heartbitListeners = ArrayList<Channel<HeartbitData>>();

    private companion object: KLogging()


    private val jobNewIds = launch(context + myjob+ CoroutineName("newIds")){
        newIds.consumeEach { id ->
            newIdsListeners.removeIf { r -> r.isClosedForSend  }
            newIdsListeners.forEach { r ->
                try {
                    /* ? offer */
                    // how to handle slow consumers?
                    r.send(id)
                } catch( ex:Exception ){
                    logger.warn { "newId sending exception:${ex.message}" }
                }
            }
        }
    }



    suspend override fun listenNewIds():Channel<NewId> = own{
        val ret = Channel<NewId>(10);
        newIdsListeners.add(ret);
        ret;
    }

    protected val delayedCommitActor = actor<Boolean>(context + CoroutineName("delayedCommit")){
        channel.consumeEach{
            delay(COMMIT_DELAY);
            db.commit();
            logger.debug("Delayed commit, lastSeq=${seq.get()}")
        }
    }

    suspend fun delayedCommit(){
        delayedCommitActor.offer(true);
    }

    /**
     * Gets given object (if avaible).
     */
    suspend override fun get(oid: ObjId)= own {objs.get(oid);}

    /**
     * Internal put. Object belongs to that replica (checked).
     * Should be called in replica thread
     */
    suspend override fun put(obj: ByteArray): ObjId = own {
        val oid = cl.objectId(obj);
        if(!cl.isReplicaForObjectId(oid, replId)){
            throw NotThisNode("Put not for this node")
        }
        if(! has(oid)) {
            val seqId = seq.incrementAndGet();
            objs.put(oid, obj);
            seq2id.put(seqId, oid);

            delayedCommit();

            // TODO: new ids must be send! should we wait for that?
//            newIds.offer( NewId(seqId,oid));
            newIds.send( NewId(seqId,oid));
            // TODO: should we wait for ending ?
        }
        oid;
    }

    /**
     * Query for ids after given seqId, acording to local ordering.
     */
    suspend override fun queryIds(afertSeqId: Long, cnt: Int): IdList = own {
        val r1 = ArrayList(seq2id
                .tailMap(afertSeqId,false)
                .entries.take(cnt)
                .map { x -> NewId(x.key,x.value) }
        );
        IdList(r1, afertSeqId, seq.get());
    }

    suspend fun cancelAll() = run(context) {
        jobNewIds.cancel()
        jobHeartbit.cancel()

        newIdsListeners.forEach { x -> x.close() }
        newIds.close();
        myjob.cancel()
    }

    suspend override fun has(oid: ObjId) = own {objs.containsKey(oid)}



    /* ReplicaManager  implementation ==========================================================  */
    private val retriver = RetriverWorker(this, context);
    private val remoteReplications = HashMap<Int,Pair<RelicaOpLog, Replicator>>();

    private val jobHeartbit = launch(context + myjob + CoroutineName("heartbit")){

        while(true) {
            val hb = HeartbitData(System.currentTimeMillis(), replId,
                    seq.get(),remoteReplications.values.map { x-> x.second.behind() }.sum(),
                    remoteReplications.values.map { x-> "{${x.second.report()}}" }.joinToString(",")
            )
            logger.debug { "Heartbit r${replId}: ${hb}" }

            heartbitListeners.forEach { x -> async(CommonPool) { x.send(hb) } }
            delay(1000)
        }
    }

    suspend override fun heartbit():Channel<HeartbitData> = own{
        val ret = Channel<HeartbitData>();
        heartbitListeners.add(ret);
        ret;
    }

    override fun close(){
        runBlocking(context) {
            cancelAll();
            db.close();
        }
    }

    override fun introduceFrom(remote: RelicaOpLog) {
        logger.info{"r${replId} - introduce from ${remote.replId}"}
        if(!remoteReplications.containsKey(remote.replId)){
            remoteReplications.put(remote.replId, Pair(remote,
                    Replicator(this,remote,retriver,200,db,context) ))
        }
        // TODO: reintroduce
    }

    override fun disconnectFrom(remote: RelicaOpLog) {
        launch(context) {
            logger.info { "r${replId} - disconnect from ${remote.replId}" }
            remoteReplications[remote.replId]?.let { rr ->
                rr.second.close();
                remoteReplications.remove(remote.replId);
            }
        }
    }

    private val xxcnt = AtomicLong()
    protected suspend fun <T> own(block: suspend ()->T):T = run(context+myjob){
//        val no = xxcnt.incrementAndGet()
//        println("    ++> ${no}")
        val ret = block.invoke()
//        println("    --> ${no}")
        ret
    }

    suspend override fun onHeartbit(hb: HeartbitData) {
        // update seq numbers if replication
        if(replId != hb.replId && remoteReplications.containsKey(hb.replId)){
            remoteReplications[hb.replId]?.second?.updateLastSeq(hb.lastSeq)
        }
    }
}
