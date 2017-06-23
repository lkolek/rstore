package pl.geostreaming.rstore.core.node.impl

import org.mapdb.*
import org.mapdb.serializer.SerializerCompressionWrapper
import org.nustaq.kontraktor.IPromise
import org.nustaq.kontraktor.Promise
import org.nustaq.kontraktor.annotations.Local
import pl.geostreaming.rstore.core.model.RsCluster
import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.node.IdList
import pl.geostreaming.rstore.core.node.NotThisNode
import pl.geostreaming.rstore.core.node.RsNodeActor

/**
 * Created by lkolek on 21.06.2017.
 */

open class RsNodeActorImpl:RsNodeActor(){
    data class Store(
            val db:DB,
            val objs:HTreeMap<ByteArray,ByteArray>,
            val seq:Atomic.Long,
            val seq2id:BTreeMap<Long,ByteArray>,
            val lastSeqIdsLocal:BTreeMap<Int,Long>,
            val lastSeqIdsRemote:BTreeMap<Int,Long>

    );
    lateinit var id:Integer;
    lateinit var store:Store;
    lateinit var cfg:RsClusterDef;
    lateinit var cl: RsCluster;

    var delCommitMs:Long = 5000;
    var delCommit = false;
    var lastCommitPr: IPromise<Void>? = null;

    @Local
    open fun init(id:Int, cfg1: RsClusterDef, dbLocation:String, dbTrans:Boolean = false) {
        this.cfg = cfg1;
        this.id = Integer(id);
        this.cl = RsCluster(cfg1);

        println("initialized, cfg=" + cfg)
        prepare(dbLocation,dbTrans);
    }

    private fun prepare(dbLocation:String, dbTrans:Boolean){
        val db = with(DBMaker.fileDB(dbLocation)){
            DBMaker.fileDB(dbLocation)
            fileMmapEnableIfSupported()
            if(dbTrans)
                transactionEnable();
            make();
        }


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

        val lastSeqIdsLocal = db.treeMap("lastSeqIds")
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(Serializer.LONG)
                .createOrOpen();
        val lastSeqIdsRemote = db.treeMap("lastSeqIds")
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(Serializer.LONG)
                .createOrOpen();

        store = Store(db,objs, db.atomicLong("seq").createOrOpen(),seq2id,lastSeqIdsLocal,lastSeqIdsRemote);

        // init
        cfg.nodes
                .filter { n -> cl.hasCommons(n.id,id.toInt()) && n.id != id.toInt() }
                .forEach{ n ->
                    lastSeqIdsLocal.putIfAbsent(n.id, -1L)
                    lastSeqIdsRemote.putIfAbsent(n.id, -1L)
                }
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

    open override fun put(obj: ByteArray, onlyThisNode: Boolean): IPromise<ByteArray> {
        // TODO check if proper node for only this node
        val pr = Promise<ByteArray>();

        val oid = cl.objectId(obj);
        val replicas = cl.replicasForObjectId(oid);

        val thisNode = replicas.any { it.id == this.id.toInt() }
        if( thisNode ){

            if( !store.objs.containsKey(oid)) {
                store.objs.putIfAbsent(oid,obj)
                val sId = store.seq.incrementAndGet();
                store.seq2id.put(sId, oid);
                // TODO: replicate if needed
                delayedCommit();
            }
            pr.resolve(oid);
        }
        else {
            if(onlyThisNode) {
                pr.reject(NotThisNode("Not this node"));
            } else {
                // TODO: call other replicas
                val r0 = replicas.get(0);
                // ... need actor
                pr.reject(NotThisNode("Not this node"));
            }
        }
        return pr;
    }

    open override fun queryNewIds(after: Long, cnt: Int): IPromise<IdList> {
        val r1 = ArrayList(
                store.seq2id.tailMap(after,false).values.take(cnt)
        );
        return resolve( IdList(r1, after, store.seq.get() ));
    }

    /**
     *
     */
    open override fun queryNewIdsFor(replId:Int, after: Long, cnt: Int): IPromise<IdList> {
        val r1 = ArrayList(
                store.seq2id.tailMap(after,false).values
                        .filter{
                            cl.isReplicaForObjectId(it,replId)
                        }
                        .take(cnt)
        );

        // calc last seqId for repl
        // it should be retrived
        val lastSeqList = store.seq2id.descendingMap()
                ?.filter { cl.isReplicaForObjectId(it.value,replId) }
                ?.toList()
                ?.take(1)
                ?.map { it.first };
        val lastSeqId = if(lastSeqList != null &&  !lastSeqList.isEmpty()) {  lastSeqList.first()} else {0L}

        return resolve( IdList(r1, after, lastSeqId ) );
    }

    override fun stop() {
        super.stop()
        try {
            store.db.commit()
            store.db.close();
        }catch (ex:Exception){

        }
    }

    protected fun delayedCommit(){
        if(!delCommit && delCommitMs > 0L){
            delCommit = true;
            lastCommitPr = Promise();
            val pr =lastCommitPr!!;
            delayed(delCommitMs) {
                println("Delayed commit:" + store.seq2id.navigableKeySet().last())
                pr.resolve();
                store.db.commit();
                delCommit = false;
            }
        }
    }

    open override fun get(oid: ByteArray): IPromise<ByteArray> =resolve(store.objs.get(oid))
}