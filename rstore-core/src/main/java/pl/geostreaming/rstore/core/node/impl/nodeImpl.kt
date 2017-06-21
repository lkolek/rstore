package pl.geostreaming.rstore.core.node.impl

import org.mapdb.*
import org.mapdb.serializer.SerializerCompressionWrapper
import org.nustaq.kontraktor.IPromise
import org.nustaq.kontraktor.Promise
import org.nustaq.kontraktor.annotations.Local
import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.node.RsNodeActor

/**
 * Created by lkolek on 21.06.2017.
 */

open class RsNodeActorImpl:RsNodeActor(){
    data class Store(
            val db:DB,
            val objs:HTreeMap<ByteArray,ByteArray>,
            val seq:Atomic.Long,
            val seq2id:BTreeMap<Long,ByteArray>
    );
    var store:Store? = null;
    lateinit var cfg:RsClusterDef;

    @Local
    open fun init(id:Int, cfg1: RsClusterDef, dbLocation:String) {
        this.cfg = cfg1;
        println("initialized, cfg=" + cfg)
        prepare(dbLocation);
    }

    private fun prepare(dbLocation:String){
        val db =DBMaker.fileDB(dbLocation)
                .closeOnJvmShutdown()
                .fileMmapEnableIfSupported()
//                .checksumStoreEnable()
                .transactionEnable()
                .make();

         val objs = db.hashMap("objs")
                 .keySerializer(Serializer.BYTE_ARRAY)
                 .valueSerializer(SerializerCompressionWrapper(Serializer.BYTE_ARRAY))
                 .valueInline()
                 .counterEnable()
                 .createOrOpen();
        val seq2id = db.treeMap("seq2id")
                .keySerializer(Serializer.LONG_DELTA)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .counterEnable()
                .createOrOpen();

        store = Store(db,objs, db.atomicLong("seq").createOrOpen(),seq2id);

    }

    open override fun test1(test:String): IPromise<String> {
        val pr = Promise<String>();

        pr.resolve("abc")

        return pr;
    }

    open override fun cfg(): IPromise<RsClusterDef> {
        val ret= Promise<RsClusterDef>();
        ret.resolve(cfg);
        return ret;
    }
}