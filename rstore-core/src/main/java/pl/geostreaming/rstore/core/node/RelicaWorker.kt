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
    suspend fun heartbit():Channel<HeartbitData>;

}
