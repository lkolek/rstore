package pl.geostreaming.rstore.kontraktor.node

import pl.geostreaming.rstore.core.model.IdList

/**
 * Created by lkolek on 25.06.2017.
 */

typealias ObjId = ByteArray;



interface RelicaWorker{
    suspend fun put(obj:ByteArray):ObjId;
    suspend fun queryIds(afertSeqId:Long, cnt:Int):IdList;


}


data class Store(
        val db: org.mapdb.DB,
        val objs: org.mapdb.HTreeMap<ByteArray, ByteArray>,
        val seq: org.mapdb.Atomic.Long,
        val seq2id: org.mapdb.BTreeMap<Long, ByteArray>
);

