package pl.geostreaming.rstore.core.model

import com.fasterxml.jackson.annotation.JsonProperty
import pl.geostreaming.rstore.core.util.toHexString
import java.io.Serializable
import java.nio.ByteBuffer
import java.nio.LongBuffer
import java.security.MessageDigest
import java.util.*

/**
 * Created by lkolek on 21.06.2017.
 */

/**
 * Pure object id / hash
 */
typealias ObjId = ByteArray;

/**
 * ObjectId with proper hash / equals implemented.
 */
class OID(val hash:ObjId): Serializable {
    init {
        if(hash.size !=32 )
            throw RuntimeException("Not valid oid (hash)")
    }
    override fun hashCode() = ByteBuffer.wrap(hash).asIntBuffer()[1]

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other?.javaClass != javaClass) return false
        other as OID
        if (!Arrays.equals(hash, other.hash)) return false
        return true
    }
}

data class RsNodeDef(val id: Int, val addr: String):Serializable

/**
 * Simple configuration / structure of cluster.
 * Currently: static, can not be rebalanced.
 * Nodes can be changed (address), restarted from scratch
 */
data class RsClusterDef private constructor(
        val nodes:List<RsNodeDef>,
        val rf:Int = 1,
        val minRepl:Int = 1,
        val replSlotSize:Int = 1013
):Serializable
{
    init {
        if(HashSet(nodes.map { it.id }).size < nodes.size
            || HashSet(nodes.map { it.addr }).size < nodes.size
                )
            throw RuntimeException("Nodes must be unique");

        for(i in 1 until nodes.size){
            if(nodes[i-1].id > nodes[i].id){
                throw RuntimeException("Nodes must be sorted by id!");
            }
        }

        if(nodes.size < rf)
            throw RuntimeException("Replication factor SHOULD be at most the size of cluster!");
    }

    private constructor(builder:Builder) : this(
            ArrayList(builder.nodes.sortedBy { it.id }),
            builder.rf,
            builder.mr,
            builder.rss
    );

    class Builder {
        var nodes = ArrayList<RsNodeDef>()
            private set
        var rf = 1
            private set
        var mr = 1
            private set

        var rss = 1013
            private set


        fun withReplicationFactor(rf:Int) = apply { this.rf = rf }
        fun withMinReplication(mr:Int) = apply {this.mr = mr}
        fun withNode( id: Int, addr: String) = apply {this.nodes.add(RsNodeDef(id,addr))}

        /**
         * CAUTION: generally probably should not be used.
         * For testing purposes (distribution etc)
         */
        fun withSlotSize(ss:Int) = apply{this.rss = ss}

        fun build() = RsClusterDef(this)
    }


    fun dump(){
        nodes.forEach{ println(it) }
    }

}


/**
 * Contains clustering / partitioning calculations for cluster of replicas
 */
class RsCluster(val cfg:RsClusterDef){
    private fun md() = MessageDigest.getInstance("SHA-256");

    fun ByteArray.toObjectId():ByteArray{
        return md().digest(this)
    }

    fun ObjId.toSlot():Int{
        val ib = ByteBuffer.wrap(this).asIntBuffer();
        val a = ib.get().xor(ib.get()).xor(ib.get()).and(0x7fffffff)
        return (a % (cfg.replSlotSize ));
    }

    /**
     * Returns first replica, that should store given key
     */
    fun firstReplIdxForObjectId(id:ObjId):Int{
        if(id.size != 32){
            throw RuntimeException("THIS IS NOT PROPER HASH");
        }
        val slot = id.toSlot();
        return (slot * cfg.nodes.size) / cfg.replSlotSize;
    }

    /**
     * Gives array of replicas that should store given id
     */
    fun replicasForObjectId(id:ObjId):Array<RsNodeDef>{
        val r1 = firstReplIdxForObjectId(id);
        return (0 until cfg.rf).map { x -> cfg.nodes[ (r1 + x) % cfg.nodes.size ] }.toTypedArray()
    }

    /**
     * Tests if ob
     */
    fun isReplicaForObjectId(id:ObjId, replId:Int):Boolean{
        val r1 = firstReplIdxForObjectId(id);
        val idx1 = cfg.nodes.indexOfFirst { it.id == replId }
        val ns = cfg.nodes.size
        return (ns + idx1 - r1 ) % ns < cfg.rf;
    }

    /**
     * tests if 2 replicas can have common objects
     */
    fun hasCommons(r1:Int,r2:Int):Boolean{
        val idx1 = cfg.nodes.indexOfFirst { it.id == r1 }
        val idx2 = cfg.nodes.indexOfFirst { it.id == r2 }
        val ns = cfg.nodes.size
        val x1= (ns + idx1 - idx2 ) % ns < cfg.rf;
        val x2= (ns + idx2 - idx1 ) % ns < cfg.rf;
        return x1 || x2;
    }

    fun objectId(obj:ByteArray) = obj.toObjectId()
    internal fun slotForId(id:ObjId) = id.toSlot()
}


/// ---- others, for communiaction

class NotThisNode(msg:String):Exception(msg), java.io.Serializable;
data class NewId(val seq:Long, val oid:ObjId):Serializable{
    override fun toString(): String {
        return "NewId(seq=$seq, oid=${oid.toHexString()}})"
    }
}

data class IdList(
        val ids:ArrayList<NewId>,
        val afterSeqId:Long,
        val lastSeqId:Long
): java.io.Serializable;


data class HeartbitData(val time:Long, val replId:Int, val lastSeq:Long, val totalBelow:Long, val replReport:String): java.io.Serializable {
    override fun toString(): String {
        return "HeartbitData(time=$time, replId=$replId, lastSeq=$lastSeq, totalBelow=$totalBelow, replReport=$replReport)"
    }
}
