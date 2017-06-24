package pl.geostreaming.rstore.core.node.impl

import org.mapdb.Atomic
import org.mapdb.BTreeMap
import org.mapdb.DB
import org.nustaq.kontraktor.*
import org.nustaq.kontraktor.annotations.Local
import pl.geostreaming.kt.Open
import pl.geostreaming.rstore.core.node.RsNodeActor
import pl.geostreaming.rstore.core.util.toHexString
import java.io.Serializable
import java.nio.ByteBuffer
import java.nio.LongBuffer
import java.util.*
import kotlin.collections.HashMap

/**
 * Created by lkolek on 24.06.2017.
 */

class OID(val hash:ByteArray):Serializable{
    init {
        if(hash.size !=32 )
            throw RuntimeException("Not valid oid (hash)")
    }
    override fun hashCode(): Int {
        return ByteBuffer.wrap(hash).asIntBuffer()[1]
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other?.javaClass != javaClass) return false
        other as OID
        if (!Arrays.equals(hash, other.hash)) return false
        return true
    }
}

/**
 * this class provides retriving behaviour (invoking get with proper handling)
 */
@Open
class RetriverActor  : Actor<RetriverActor>(){

    private lateinit var myRepl:RsNodeActor

    @Local
    fun init(myRepl:RsNodeActor){
        this.myRepl = myRepl;
    }

    private val currentOps = HashMap<OID, IPromise<Void> >();
    private fun has(oid:OID):Boolean{
        return currentOps.containsKey(oid);
    }

    fun replicate(oid:OID, fromRepl:RsNodeActor):IPromise<Void>{
        println("RETRIVE:" + oid.hash.toHexString() )
        val notify = Promise<Void>();
        if(has(oid)){

            val lpr= currentOps.get(oid);
            val npr = Promise<Void>();
            npr.onResult { lpr?.resolve(); notify.resolve() }
            .onError { err -> lpr?.reject(err); notify.reject(err) }

            currentOps.put(oid,npr);
            return notify;
        }
        else {
            currentOps.put(oid, notify);
            fromRepl.get(oid.hash).onResult {
                x ->
                myRepl.put(x, true).onResult {
                    currentOps.get(oid)?.complete();
                    currentOps.remove(oid)
                }.onError {
                    currentOps.get(oid)?.reject(RuntimeException("Could not get oid"))
                    currentOps.remove(oid)
                }

            }.onError {
                currentOps.get(oid)?.reject(RuntimeException("Could not get oid"))
                currentOps.remove(oid)
            }
            return notify;
        }
    }
}

/**
 * This class provides replication for r1 to r2
 */
class Replicator (
        val myRepl:RsNodeActor ,
        val checker: (ByteArray) -> Boolean,
        val fromReplId:Int,
        val fromRepl:RsNodeActor,
        val retriver: RetriverActor,
        val db:DB) {
    val lastSeqIdRemote:   Atomic.Long = db.atomicLong("lastSeqIdRemote." + fromReplId).createOrOpen()
    val fullyReplicatedTo: Atomic.Long = db.atomicLong("lastSeqIdRemote." + fromReplId).createOrOpen()
    val toReplicate:TreeMap<Long,ReplOp> = TreeMap();

    var replicating:Boolean = false;
    var queryingSeqIds:Boolean = false;

    /**
     * Replication operation (for given seqId), defaults to NO-OP
     */
    open class ReplOp(val shouldReplicate:Boolean = false){
        open fun completed():Boolean = true;
    }

    data class ToReplicate( val objId:OID ): ReplOp(true){
        var attemts: Int = 0
        var replicated: Boolean = false
        var processing:Boolean = false
        override fun completed(): Boolean {
            return replicated;
        }
    }

    init {
        fromRepl.listenIds(Callback{  (seqId,objId),err ->
            if(Actor.isResult(err)){
                println("repl onIds:" + seqId);
                append(seqId,OID(objId), checker.invoke(objId) );

                if(lastSeqIdRemote.get() +1 == seqId){
                    lastSeqIdRemote.incrementAndGet();
                } else {
                    updateLastSeqTo(seqId);
                }
            }

        });
    }

    fun updateLastSeqTo(remoteLastSeq:Long){
        if( lastSeqIdRemote.get() < remoteLastSeq){
            lastSeqIdRemote.set(remoteLastSeq);
            if(!queryingSeqIds )
                doUpdateSeq(fullyReplicatedTo.get(), remoteLastSeq);
        }
    }

    fun doUpdateSeq(after:Long, to:Long){
        fromRepl
            .queryNewIds(after, 1000)
            .onResult {
                x-> val cnt = x.ids.size;
                x.ids.forEachIndexed{
                    i,id -> append( after+i, OID(id), checker.invoke(id))
                }
            }
    }

    fun tryRepl(i:ToReplicate){
        if(! i.replicated && !i.processing ){
            // TODO ?: check if already in myRepl
                i.attemts++;
                i.processing = true;
            // TODO: this way we don't know if error is on the same fromRepl!
                retriver.replicate(i.objId,fromRepl)
                    .onResult{
                        i.replicated = true
                        i.processing = false;
                        performCleaning();
                    }.onError {
                        i.processing = false;
                        println("Some error")
                    }

        }
    }

    fun processSome(){
        println("Process some called, fullReplTo:" + fullyReplicatedTo.get())
        val CNT = 10;
        if(toReplicate.isEmpty()){
            replicating = false;

        }
        else {
            replicating = true; // to assure
            performCleaning();

            val toProc = ArrayList(toReplicate.entries.filter { (k, v) -> v is ToReplicate && !v.processing }.take(CNT));
            toProc.forEach { x -> if (!retriver.isMailboxPressured) {
                tryRepl(x.value as ToReplicate)
            }; }

            myRepl.delayed(200){processSome()}
        }
    }

    /**
     * called on new seqId from remote replica
     */
    fun append( seqId:Long, objId: OID, needsReplication:Boolean ){
        println("Append (on thread:" + Thread.currentThread())
        try {
            if (!needsReplication && seqId <= fullyReplicatedTo.get() + 1) {
                // optimisation
                fullyReplicatedTo.incrementAndGet();
            } else {
                if (seqId > fullyReplicatedTo.get() && !toReplicate.containsKey(seqId)) {
                    if (needsReplication)
                        toReplicate.put(seqId, ToReplicate(objId))
                    else
                        toReplicate.put(seqId, ReplOp())
                }

                if (!replicating) {
                    myRepl.delayed(2) { processSome() }
                }
            }
        }catch(ex:NullPointerException){
            ex.printStackTrace();
        }
    }

    /**
     * called to signal seqId was replicated
     */
    fun wasReplicated( seqId: Long){
        if(seqId > fullyReplicatedTo.get() && toReplicate.containsKey(seqId)){
            val x = toReplicate.get(seqId)
            if( x is ToReplicate){
                x.replicated = true;
            }
        }
        performCleaning();
    }

    protected fun performCleaning(){
        var frt = fullyReplicatedTo.get();
        while( toReplicate.size >0 &&  toReplicate.firstEntry().key <= frt+1 && toReplicate.firstEntry().value.completed()){
            frt = toReplicate.firstEntry().key;
            toReplicate.remove(toReplicate.firstEntry().key)
        }
        fullyReplicatedTo.set(frt);
    }

}

