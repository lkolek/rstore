package pl.geostreaming.rstore.core.node.impl

import org.mapdb.*
import org.mapdb.serializer.SerializerCompressionWrapper
import org.nustaq.kontraktor.Callback
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

class RsNodeActorImpl:RsNodeActor(){
    data class Store(
            val db:DB,
            val objs:HTreeMap<ByteArray,ByteArray>,
            val seq:Atomic.Long,
            val seq2id:BTreeMap<Long,ByteArray>
    );

    final lateinit var id:Integer;
    final lateinit var store:Store;
    final lateinit var cfg:RsClusterDef;
    final lateinit var cl: RsCluster;
    final lateinit var retriver:RetriverActor;

    final var delCommitMs:Long = 5000;
    final var delCommit = false;
    final var lastCommitPr: IPromise<Void>? = null;


    final val listenIdsCalbacks = ArrayList<Callback<Pair<Long, ByteArray>>>();


    class RemoteReplicaReg( val remoteRepl:RsNodeActor, val replicator: Replicator );
    final val remoteRepls:MutableMap<Int,RemoteReplicaReg> = HashMap();

    @Local
    fun init(id:Int, cfg1: RsClusterDef, dbLocation:String, dbTrans:Boolean = false) {
        this.cfg = cfg1;
        this.id = Integer(id);
        this.cl = RsCluster(cfg1);

        this.retriver = AsActor(RetriverActor::class.java)
        this.retriver.init( self() );

        println("initialized, cfg=" + cfg)
        prepare(dbLocation,dbTrans);
    }

    /**
     * called from other repl for introduction
     */
    override fun introduce(id: Int, replicaActor: RsNodeActor, own: Long): IPromise<Long> {

        if( id != this.id.toInt() &&  cl.hasCommons(this.id.toInt(),id)){
            // for now: ignore if already exist TODO change / reitroduce / validate
            if(!remoteRepls.containsKey(id)){
                val replicator= Replicator( self(),
                        { oid -> !store.objs.containsKey(oid) && cl.isReplicaForObjectId(oid, this.id.toInt()) },
                        id,replicaActor,retriver,store.db)
                remoteRepls.put(id, RemoteReplicaReg(replicaActor, replicator))
            }
            // TODO: update lastSeq!
        }

        return resolve( store.seq.get() );
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

        store = Store(db,objs, db.atomicLong("seq").createOrOpen(),seq2id);

        // init
        cfg.nodes
                .filter { n -> cl.hasCommons(n.id,id.toInt()) && n.id != id.toInt() }
                .forEach{ n ->
                }
    }



    override fun cfg(): IPromise<RsClusterDef> {
        val ret= Promise<RsClusterDef>();
        ret.resolve(cfg);
        return ret;
    }

    override fun put(obj: ByteArray, onlyThisNode: Boolean): IPromise<ByteArray> {
        // TODO check if proper node for only this node
        val pr = Promise<ByteArray>();

        val oid = cl.objectId(obj);
        val replicas = cl.replicasForObjectId(oid);

        val thisNode = replicas.any { it.id == this.id.toInt() }
        if( thisNode ){

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

//    @Local
//    fun internalPut(oid:ByteArray, obj: ByteArray): IPromise<ByteArray>{
//        if( !store.objs.containsKey(oid))
//    }

    override fun queryNewIds(after: Long, cnt: Int): IPromise<IdList> {
        val r1 = ArrayList(
                store.seq2id.tailMap(after,false).values.take(cnt)
        );
        return resolve( IdList(r1, after, store.seq.get() ));
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

    override fun get(oid: ByteArray): IPromise<ByteArray> =resolve(store.objs.get(oid))



    override fun listenIds(cb: Callback<Pair<Long, ByteArray>>) {
        listenIdsCalbacks.add(cb);
    }
}