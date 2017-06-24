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
import java.util.function.Consumer
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
//        println("RETRIVE try:" + oid.hash.toHexString() )
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
//                    println("RETRIVEed :" + oid.hash.toHexString() )
                }.onError {
                    currentOps.get(oid)?.reject(RuntimeException("Could not get oid"))
                    currentOps.remove(oid)
                }.onTimeout {
                    x->
                    currentOps.get(oid)?.reject(RuntimeException("Could not get oid"))
                    currentOps.remove(oid)
                }

            }.onError {
                currentOps.get(oid)?.reject(RuntimeException("Could not get oid"))
                currentOps.remove(oid)
            }.onTimeout {
                x->
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
    val fullyReplicatedTo: Atomic.Long = db.atomicLong("fullyReplicatedTo." + fromReplId).createOrOpen()
    val toReplicate:TreeMap<Long,ReplOp> = TreeMap();

    var replicating:Boolean = false;
    var queryingSeqIds:Boolean = false;


    /**
     * Replication operation (for given seqId), defaults to NO-OP
     */
    open class ReplOp(val shouldReplicate:Boolean = false){
        open fun completed():Boolean = true;
    }

    data class ToReplicate( val seqId:Long,val objId:OID ): ReplOp(true){

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
                val needIt = checker.invoke(objId);
//                println("repl onIds:" + seqId  + if(needIt){" NEED IT"} else "");

                val updSeq =  !queryingSeqIds && lastSeqIdRemote.get() +1 < seqId;


                val updSeqFrom = lastSeqIdRemote.get();


                lastSeqIdRemote.set(Math.max(lastSeqIdRemote.get(),seqId))

                append(seqId,OID(objId),  needIt);

                if(updSeq)
                    updateLastSeqTo(lastSeqIdRemote.get());

            }

        });
    }

    fun updateLastSeqTo(from:Long){
        println("UPDATE TO called:" + from);
        if( lastSeqIdRemote.get() < from) {
            lastSeqIdRemote.set(from);
        }
        if(!queryingSeqIds )
            doUpdateSeq(fullyReplicatedTo.get(), from);

    }

    fun doUpdateSeq(after:Long, to:Long){
        queryingSeqIds = true;
        fromRepl
            .queryNewIds(after, 1000)
            .onResult {
                x-> val cnt = x.ids.size;
                println("do update received:" + (after+1) + " - " + (after + cnt));
                x.ids.forEachIndexed{
                    i,id -> append( (after+i+1), OID(id), checker.invoke(id))
                }

                var after2 = after+cnt;

                // TODO: opt: we can check present in toReplicate
                after2 = toReplicate.keys.filter { k -> k < after2 }.first()
                while(toReplicate.containsKey(after2)){
                    after2++;
                }

                if(after2 < lastSeqIdRemote.get()){

                    doUpdateSeq(after2, lastSeqIdRemote.get())
                } else {
                    queryingSeqIds = false;
                }
            }
                .onError { queryingSeqIds = false; }
    }

    fun tryRepl(i:ToReplicate){
        if(!checker.invoke(i.objId.hash)){
            i.replicated = true;
            return;
        }
        if(! i.replicated && !i.processing ){
            // TODO ?: check if already in myRepl
                i.attemts++;
                i.processing = true;
            // TODO: this way we don't know if error is on the same fromRepl!
                retriver.replicate(i.objId,fromRepl)
                    .onResult{
                        i.replicated = true
                        i.processing = false;
                        wasReplicated(i.seqId)
                        performCleaning();
                    }.onError {
                        i.processing = false;
                        println("Some error")
                    }.onTimeout(Consumer<Void> {
                        i.processing = false;
                        println("Timeout")
                    })

        }
    }

    fun processSome(){
        println("Process some called, fullReplTo:" + fullyReplicatedTo.get() + ", lastSeqIdRemote=" + lastSeqIdRemote.get()
                + ", entry[0]:" + if(toReplicate.size>0 ){"" + toReplicate.entries.first().key} else {""} )
        val CNT = 50;
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

            myRepl.delayed(50){processSome()}
        }
    }

    /**
     * called on new seqId from remote replica
     */
    fun append( seqId:Long, objId: OID, needsReplication:Boolean ){
        if (!needsReplication && seqId <= fullyReplicatedTo.get() + 1) {
            // optimisation
            fullyReplicatedTo.incrementAndGet();
        } else {
            if (seqId > fullyReplicatedTo.get() && !toReplicate.containsKey(seqId)) {
                if (needsReplication)
                    toReplicate.put(seqId, ToReplicate( seqId, objId))
                else
                    toReplicate.put(seqId, ReplOp())
            }

            if (!replicating) {
                processSome()
            }
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


    /**
     * Returns distance from from lastSeq and fully
     */
    fun below():Long = this.lastSeqIdRemote.get() - fullyReplicatedTo.get();

}

