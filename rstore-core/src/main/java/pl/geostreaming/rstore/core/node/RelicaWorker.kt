package pl.geostreaming.rstore.core.node

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
interface RelicaWorker{
    /**
     * creates new heartbit channel with [HeartbitData] every 1s
     */
    suspend fun heartbit():Channel<HeartbitData>;

    /**
     * create new channel with new (seqId,ObjId) pair for every object added to this replica
     */
    suspend fun listenNewIds():Channel<Pair<Long, ObjId>>;

    /**
     * puts object to replica - internal behaviour, without propagation
     *
     * @return object id (hash) of added object
     */
    suspend fun put(obj:ByteArray):ObjId;

    /**
     * Query ids (hashes) after given seqId
     *
     * @return ids after specified one
     */
    suspend fun queryIds(afertSeqId:Long, cnt:Int):IdList;

    /**
     * Get object by id (hash)
     *
     *
     * @return binary data or null if not present
     */
    suspend fun get(oid:ObjId):ByteArray?;

    suspend fun has(oid:ObjId):Boolean;

}
