package pl.geostreaming.rstore.core.node

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import mu.KLogging
import org.mapdb.Atomic
import org.mapdb.BTreeMap
import org.mapdb.DB
import org.mapdb.HTreeMap
import pl.geostreaming.rstore.core.model.*
import java.util.*
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
class ReplicaWorkerMapdbImpl (
        val replId:Int,
        val cl:RsCluster,
        val db:DB,
        val context:CoroutineContext = newSingleThreadContext("Relica ${replId}")

) :RelicaWorker {
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
    var toCommit = false;

    private val newIds = Channel<Pair<Long, ObjId>>();
    private val newIdsListeners = ArrayList<Channel<Pair<Long, ObjId>>>();
    private val heartbitListeners = ArrayList<Channel<HeartbitData>>();

    companion object: KLogging()

    init{

        logger.info { "Initialized storage r${replId}, seqId=${seq.get()}" }


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
            launch(context + myjob){
                while(true) {
                    val hb = HeartbitData(System.currentTimeMillis(), replId,
                            seq.get(),
                            0 //remoteRepls.values.map { x -> x.replicator.below() }.sum()
                    )
                    logger.debug { "Heartbit r${replId}: ${hb}" }

                    heartbitListeners.forEach { x -> async(CommonPool) { x.send(hb) } }
                    delay(1000)
                }
            }
        }
    }


    suspend override fun listenNewIds():Channel<Pair<Long, ObjId>> = run(context){
        val ret = Channel<Pair<Long, ObjId>>();
        newIdsListeners.add(ret);
        ret;
    }

    suspend override fun heartbit():Channel<HeartbitData> = run(context + myjob){
        val ret = Channel<HeartbitData>();
        heartbitListeners.add(ret);
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

    suspend override fun has(oid: ObjId) = run(context) {objs.containsKey(oid)}

}