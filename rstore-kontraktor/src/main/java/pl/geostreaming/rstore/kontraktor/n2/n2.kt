package pl.geostreaming.rstore.kontraktor.n2

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.newSingleThreadContext
import kotlinx.coroutines.experimental.runBlocking
import org.nustaq.kontraktor.*
import org.nustaq.kontraktor.annotations.Local
import pl.geostreaming.rstore.core.model.HeartbitData
import pl.geostreaming.rstore.core.model.IdList
import pl.geostreaming.rstore.core.model.ObjId
import pl.geostreaming.rstore.core.model.RsClusterDef
import pl.geostreaming.rstore.core.node.RelicaOpLog
import pl.geostreaming.rstore.core.node.ReplicaMapdbImpl
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
@pl.geostreaming.kt.Open
class RsNodeActor : Actor<RsNodeActor>() {

    fun introduce(id:Int, replicaActor: RsNodeActor, own:Long ): IPromise<Long>  =noimpl()


    fun cfg(): IPromise<RsClusterDef> =noimpl()

    fun put(obj:ByteArray, onlyThisNode:Boolean): IPromise<ByteArray> =noimpl()


    fun queryNewIds(afert:Long, cnt:Int): IPromise<IdList> =noimpl()
    //    fun queryNewIdsFor(replId:Int, after: Long, cnt: Int): IPromise<IdList>
//            = reject(RuntimeException("UNIMPLEMENTED"));
    fun get(oid:ByteArray): IPromise<ByteArray?> =noimpl()

    fun has(oid:ByteArray): IPromise<Boolean> =noimpl()

    fun listenIds(cb: Callback<Pair<Long, ByteArray>>){}
    fun listenHeartbit(cb: Callback<HeartbitData>){}

    private fun <T> noimpl(): IPromise<T> = reject(RuntimeException("UNIMPLEMENTED"))
}

class RsNodeActorImpl : RsNodeActor(){
    private final lateinit var repl:ReplicaMapdbImpl;

    private companion object {
        private val context = newSingleThreadContext("Kontr2Kt");
    }

    @Local
    fun init( repl: ReplicaMapdbImpl) {
        this.repl = repl;
    }

    override fun cfg() = Actors.resolve(repl.cl.cfg);

    override fun put(obj: ByteArray, onlyThisNode: Boolean) = toPromise { repl.put(obj) }

    override fun get(oid: ByteArray) = toPromise{ repl.get(oid) }

    override fun queryNewIds(afert: Long, cnt: Int) = toPromise{ repl.queryIds(afert,cnt) }

    override fun has(oid: ByteArray)= toPromise{ repl.has(oid) }


    private fun <T> toPromise( x : suspend () -> T )= Promise<T>().let { ret ->
            async(context) {
                try {
                    ret.resolve(x.invoke())
                } catch(ex: Exception) {
                    ret.reject(ex)
                }
            }
            ret;
        };

    override fun listenIds(cb: Callback<Pair<Long, ByteArray>>) {
        async(context) {
            try {
                repl.listenNewIds().consumeEach { x -> cb.stream(x) }
            } catch( ex:Exception){
                cb.reject("Exception")
            }
        }
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
        l.invoke().then { result, error ->
            if(Actors.isResult(error)){
                x.resume(result)
            } else {
                val ex = error as Throwable ?: RuntimeException(error)
                x.resumeWithException(ex)
            }
        }
    }
}