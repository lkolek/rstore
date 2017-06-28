package pl.geostreaming.rstore.kontraktor.n2

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.newSingleThreadContext
import kotlinx.coroutines.experimental.runBlocking
import org.nustaq.kontraktor.*
import org.nustaq.kontraktor.annotations.AsCallback
import org.nustaq.kontraktor.annotations.Local
import pl.geostreaming.kt.Open
import pl.geostreaming.rstore.core.model.HeartbitData
import pl.geostreaming.rstore.core.model.IdList
import pl.geostreaming.rstore.core.model.ObjId
import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.node.RelicaOpLog
import pl.geostreaming.rstore.core.node.ReplicaMapdbImpl
import java.util.function.Consumer
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.suspendCoroutine

/**
 * Created by lkolek on 28.06.2017.
 */
/**
 *
 * NOTES:
 *  - currently cluster cfg can't change, but in the future we don't have to kill all nodes to apply SOME changes
 */
@Open
class RsNodeActor : Actor<RsNodeActor>() {

    open fun introduce(id:Int, replicaActor: RsNodeActor): IPromise<Boolean>  =noimpl()


    fun cfg(): IPromise<RsClusterDef> =noimpl()

    fun put(obj:ByteArray, onlyThisNode:Boolean): IPromise<ByteArray> =noimpl()


    fun queryNewIds(afert:Long, cnt:Int): IPromise<IdList> =noimpl()
    //    fun queryNewIdsFor(replId:Int, after: Long, cnt: Int): IPromise<IdList>
//            = reject(RuntimeException("UNIMPLEMENTED"));
    fun get(oid:ByteArray): IPromise<ByteArray?> =noimpl()

    fun has(oid:ByteArray): IPromise<Boolean> =noimpl()

    fun listenIds(cb: Callback<Pair<Long, ByteArray>>){}
    fun listenHeartbit(cb: Callback<HeartbitData>){}

    private fun <T> noimpl(): IPromise<T> = reject(RuntimeException("UNIMPLEMENTED, proxy:${isProxy}, obj=${this}"))
}

@Open
class RsNodeActorImpl : RsNodeActor(){
    private final var replRef:ReplicaMapdbImpl? = null;
    private val repl get() = replRef?:throw kotlin.RuntimeException("Not initialized, replica ${this}")

    private companion object {
        private val context = newSingleThreadContext("Kontr2Kt");
    }

    @Local
    fun init( r: ReplicaMapdbImpl) {
        if(!isProxy) {
            replRef = r;
            println("repl ${this} initialized to ${r}")
        } else {
            println("!!! init called on proxy!")
        }
    }

    override fun introduce(id: Int, replicaActor: RsNodeActor)= toPromise {
        println("called RsNodeActorImpl ${this}, proxy=${isProxy}")
        val remote = RemoteRepl(id,replicaActor);
        repl.introduceFrom(remote)
        true;
    }

//    override fun introduce(id: Int, replicaActor: RsNodeActor): IPromise<Boolean>{
//        return toPromise {
//            println("called RsNodeActorImpl ${this}, proxy=${isProxy}")
//            val remote = RemoteRepl(id,replicaActor);
//            repl.introduceFrom(remote)
//            true;
//        }
//    }

    override fun cfg() = Actors.resolve(repl.cl.cfg);

    override fun put(obj: ByteArray, onlyThisNode: Boolean) = toPromise { repl.put(obj) }

    override fun get(oid: ByteArray) = toPromise{ repl.get(oid) }

    override fun queryNewIds(afert: Long, cnt: Int) = toPromise{ repl.queryIds(afert,cnt) }

    override fun has(oid: ByteArray)= toPromise{ repl.has(oid) }


    private fun <T> toPromise( x : suspend () -> T ):IPromise<T>{
        val ret = Promise<T>();
        async(context) {
            try {
                val v = x.invoke()
                ret.resolve(v)
            } catch(ex: Exception) {
                println("EXCEPTION: ${ex.message}")
                ret.reject(ex)
            }
        }
        return ret;
    }

    override fun listenIds(cb: Callback<Pair<Long, ByteArray>>) {
        async(context) {
            try {
                repl.listenNewIds().consumeEach { x -> cb.stream(x) }
            } catch( ex:Exception){
                cb.reject("Exception")
            }
        }
    }

    override fun stop() {
        repl.close();
        super.stop()
    }
}

class RemoteRepl(override val replId:Int,private val act:RsNodeActor): RelicaOpLog {
    private companion object {
        private val context = newSingleThreadContext("Kontr2Kt");
    }

    suspend override fun listenNewIds(): Channel<Pair<Long, ObjId>> {
        val ch=Channel<Pair<Long, ObjId>>(100);
        act.listenIds(Callback { result, error -> runBlocking {
            if(Actors.isResult(error)){
                ch.send(result)
            } else {
                ch.close()
            }
        } });

        return ch;
    }

    suspend override fun queryIds(afertSeqId: Long, cnt: Int) = toSuspend{  act.queryNewIds(afertSeqId,cnt)}

    suspend override fun put(obj:ByteArray) = toSuspend{  act.put(obj,false)}

    suspend override fun get(oid:ObjId) = toSuspend{  act.get(oid)}

    suspend override fun has(oid:ObjId) = toSuspend{  act.has(oid)}

    private suspend fun <T> toSuspend( l: ()->IPromise<T>) = suspendCoroutine<T> { x ->
        l.invoke()
                .onError { e ->
                    println("ERROR in toSuspend: ${e}")
                    val ex = e as Throwable ?: RuntimeException(e?.toString() ?: "no def")
                    x.resumeWithException(ex)
                }
                .onResult{ r -> x.resume(r)}
                .onTimeout (Consumer<Void> {  x.resumeWithException(RuntimeException("timeout")) });


    }
}