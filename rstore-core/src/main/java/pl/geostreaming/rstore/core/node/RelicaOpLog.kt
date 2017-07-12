package pl.geostreaming.rstore.core.node

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import org.mapdb.Atomic
import org.mapdb.BTreeMap
import org.mapdb.DB
import org.mapdb.HTreeMap
import pl.geostreaming.rstore.core.model.*
import java.io.Serializable
import java.util.*
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.collections.ArrayList

/**
 * Created by lkolek on 25.06.2017.
 */


/**
 * core Replica interface for inner operations
 *
 *
 * Replica worker iterface, implementing non-blocking.
 * (?) Should be called in the same thread - how to make it happen and sure?.
 *
 * Performs basic replica operations / storage.
 *
 * Does not (directly) do any replication / communication stuff.
 */
interface RelicaOpLog {

    val replId:Int

    /**
     * create new channel with new (seqId,ObjId) pair for every object added to this replica
     */
    suspend fun listenNewIds():Channel<NewId>;

    /**
     * Query ids (hashes) after given seqId
     *
     * @return ids after specified one
     */
    suspend fun queryIds(afertSeqId:Long, cnt:Int):IdList;

    /**
     * puts object to replica - internal behaviour, without propagation
     *
     * @return object id (hash) of added object
     */
    suspend fun put(obj:ByteArray):ObjId;


    /**
     * Get object by id (hash)
     *
     *
     * @return binary data or null if not present
     */
    suspend fun get(oid:ObjId):ByteArray?;

    /**
     * Checking if replica has specific object
     */
    suspend fun has(oid:ObjId):Boolean;
}


interface ReplicaManager : RelicaOpLog{
    /**
     * creates new heartbit channel with [HeartbitData] every 1s
     */
    suspend fun heartbit():Channel<HeartbitData>;

    fun close();

    fun introduceFrom(remote:RelicaOpLog )

}

interface ReplicaInstance : ReplicaManager{
    val cl:RsCluster
}