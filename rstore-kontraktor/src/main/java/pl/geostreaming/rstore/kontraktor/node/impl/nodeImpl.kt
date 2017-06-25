package pl.geostreaming.rstore.kontraktor.node.impl

import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.node.HeartbitData
import pl.geostreaming.rstore.core.node.IdList
import pl.geostreaming.rstore.core.node.NotThisNode
import pl.geostreaming.rstore.core.node.RsNodeActor

/**
 * Created by lkolek on 21.06.2017.
 */

class RsNodeActorImpl: pl.geostreaming.rstore.core.node.RsNodeActor(){
    data class Store(
            val db: org.mapdb.DB,
            val objs: org.mapdb.HTreeMap<ByteArray, ByteArray>,
            val seq: org.mapdb.Atomic.Long,
            val seq2id: org.mapdb.BTreeMap<Long, ByteArray>
    );



    final lateinit var id:Integer;
    final lateinit var store: pl.geostreaming.rstore.kontraktor.node.RsNodeActorImpl.Store;
    final lateinit var cfg: pl.geostreaming.rstore.core.model.RsClusterDef;
    final lateinit var cl: pl.geostreaming.rstore.core.model.RsCluster;
    final lateinit var retriver: RetriverActor;

    final var delCommitMs:Long = 1000;
    final var delCommit = false;
    final var lastCommitPr: org.nustaq.kontraktor.IPromise<Void>? = null;


    final val listenIdsCalbacks = ArrayList<org.nustaq.kontraktor.Callback<Pair<Long, ByteArray>>>();
    final val heartbitCallbacks = ArrayList<org.nustaq.kontraktor.Callback<HeartbitData>>();


    class RemoteReplicaReg(val remoteRepl: pl.geostreaming.rstore.core.node.RsNodeActor, val replicator: Replicator);
    final val remoteRepls:MutableMap<Int, pl.geostreaming.rstore.kontraktor.node.RsNodeActorImpl.RemoteReplicaReg> = HashMap();

    @org.nustaq.kontraktor.annotations.Local
    fun init(id:Int, cfg1: pl.geostreaming.rstore.core.model.RsClusterDef, dbLocation:String, dbTrans:Boolean = false) {
        this.cfg = cfg1;
        this.id = Integer(id);
        this.cl = pl.geostreaming.rstore.core.model.RsCluster(cfg1);

        this.retriver = AsActor(pl.geostreaming.rstore.core.node.impl.RetriverActor::class.java, 500)
        this.retriver.init( self() );

        println("initialized, cfg=" + cfg)
        prepare(dbLocation,dbTrans);

        // ready
        tick();
    }

    protected fun tick(){
        val hb = pl.geostreaming.rstore.core.node.HeartbitData(System.currentTimeMillis(), this.id.toInt(),
                this.store.seq.get(),
                remoteRepls.values.map { x -> x.replicator.below() }.sum()
        )
        heartbitCallbacks.forEach{x -> x.stream(hb)}

        delayed(1000){tick()}
    }

    /**
     * called from other repl for introduction
     */
    override fun introduce(id: Int, replicaActor: pl.geostreaming.rstore.core.node.RsNodeActor, own: Long): org.nustaq.kontraktor.IPromise<Long> {

        if( id != this.id.toInt() &&  cl.hasCommons(this.id.toInt(),id)){
            // for now: ignore if already exist TODO change / reitroduce / validate
            if(!remoteRepls.containsKey(id)){
                val replicator= Replicator( self(),
                        { oid -> !store.objs.containsKey(oid) && cl.isReplicaForObjectId(oid, this.id.toInt()) },
                        id,replicaActor,retriver,store.db)
                remoteRepls.put(id, pl.geostreaming.rstore.kontraktor.node.RsNodeActorImpl.RemoteReplicaReg(replicaActor, replicator))
            }
            // TODO: update lastSeq!
        }

        return resolve( store.seq.get() );
    }


    private fun prepare(dbLocation:String, dbTrans:Boolean){
        val db = with(org.mapdb.DBMaker.fileDB(dbLocation)){
            org.mapdb.DBMaker.fileDB(dbLocation)
            fileMmapEnableIfSupported()
            if(dbTrans)
                transactionEnable();
            make();
        }


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

        store = pl.geostreaming.rstore.kontraktor.node.RsNodeActorImpl.Store(db, objs, db.atomicLong("seq").createOrOpen(), seq2id);

        // init
        cfg.nodes
                .filter { n -> cl.hasCommons(n.id,id.toInt()) && n.id != id.toInt() }
                .forEach{ n ->
                }
    }



    override fun cfg(): org.nustaq.kontraktor.IPromise<RsClusterDef> {
        val ret= org.nustaq.kontraktor.Promise<RsClusterDef>();
        ret.resolve(cfg);
        return ret;
    }

    override fun put(obj: ByteArray, onlyThisNode: Boolean): org.nustaq.kontraktor.IPromise<ByteArray> {
        // TODO check if proper node for only this node
        val pr = org.nustaq.kontraktor.Promise<ByteArray>();

        val oid = cl.objectId(obj);


        if( cl.isReplicaForObjectId(oid,this.id.toInt()) ){

            if( !store.objs.containsKey(oid)) {
                store.objs.putIfAbsent(oid,obj)

                // TODO: replicate if needed
                // if replicate, seqId generation / placing should be delayed to avoid double puts

                val sId = store.seq.incrementAndGet();
                store.seq2id.put(sId, oid);

                listenIdsCalbacks.forEach { cb-> cb.stream(Pair(sId,oid)) }
                delayedCommit();
            }
            pr.resolve(oid);
        }
        else {
            val replicas = cl.replicasForObjectId(oid);

            if(onlyThisNode) {
                pr.reject(pl.geostreaming.rstore.core.node.NotThisNode("Not this node"));
            } else {
                // TODO: call other replicas
                val r0 = replicas.get(0);
                // ... need actor
                pr.reject(pl.geostreaming.rstore.core.node.NotThisNode("Not this node"));
            }
        }
        return pr;
    }

//    @Local
//    fun internalPut(oid:ByteArray, obj: ByteArray): IPromise<ByteArray>{
//        if( !store.objs.containsKey(oid))
//    }

    override fun queryNewIds(after: Long, cnt: Int): org.nustaq.kontraktor.IPromise<IdList> {
        val r1 = ArrayList(
                store.seq2id.tailMap(after,false).entries.take(cnt).map { x -> Pair(x.key,x.value) }
        );
        return resolve(pl.geostreaming.rstore.core.node.IdList(r1, after, store.seq.get()));
    }

//    /**
//     *
//     */
//    override fun queryNewIdsFor(replId:Int, after: Long, cnt: Int): IPromise<IdList> {
//        val r1 = ArrayList(
//                store.seq2id.tailMap(after,false).values
//                        .filter{
//                            cl.isReplicaForObjectId(it,replId)
//                        }
//                        .take(cnt)
//        );
//
//        // calc last seqId for repl
//        // it should be retrived
//        val lastSeqList = store.seq2id.descendingMap()
//                ?.filter { cl.isReplicaForObjectId(it.value,replId) }
//                ?.toList()
//                ?.take(1)
//                ?.map { it.first };
//        val lastSeqId = if(lastSeqList != null &&  !lastSeqList.isEmpty()) {  lastSeqList.first()} else {0L}
//
//        return resolve( IdList(r1, after, lastSeqId ) );
//    }

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
            lastCommitPr = org.nustaq.kontraktor.Promise();
            val pr =lastCommitPr!!;
            delayed(delCommitMs) {
                println("Delayed commit:" + store.seq2id.navigableKeySet().last())
                pr.resolve();
                store.db.commit();
                delCommit = false;
            }
        }
    }

    override fun get(oid: ByteArray): org.nustaq.kontraktor.IPromise<ByteArray> =resolve(store.objs.get(oid))



    override fun listenIds(cb: org.nustaq.kontraktor.Callback<Pair<Long, ByteArray>>) {
        listenIdsCalbacks.add(cb);
    }

    override fun listenHeartbit(cb: org.nustaq.kontraktor.Callback<HeartbitData>) {
        heartbitCallbacks.add(cb);
    }
}