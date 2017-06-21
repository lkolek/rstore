package pl.geostreaming.rstore.core.model

import java.io.Serializable
import java.nio.ByteBuffer
import java.nio.LongBuffer
import java.security.MessageDigest

/**
 * Created by lkolek on 21.06.2017.
 */

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
            throw RuntimeException("Replication factor must be at least the size of cluster!");
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
        internal fun withSlotSize(ss:Int) = apply{this.rss = ss}

        fun build() = RsClusterDef(this)
    }


    fun dump(){
        nodes.forEach{ println(it) }
    }

}
data class RsNodeDef(val id: Int, val addr: String):Serializable



class RsCluster(val cfg:RsClusterDef){
    private val md = MessageDigest.getInstance("SHA-256");

    fun ByteArray.toObjectId():ByteArray{
        return md.digest(this)
    }

    fun ByteArray.toSlot():Int{
        val ib = ByteBuffer.wrap(this).asIntBuffer();
        val a = ib.get().xor(ib.get()).xor(ib.get()).and(0x7fffffff)
        return (a % (cfg.replSlotSize ));
    }


    fun replicasForObjectId(obj:ByteArray):Array<RsNodeDef>{
        if(obj.size != 32){
            throw RuntimeException("THIS IS NOT PROPER HASH");
        }
        val slot = obj.toSlot();
        val r1= (slot * cfg.nodes.size) / cfg.replSlotSize;
        return (0 until cfg.rf).map { x -> cfg.nodes[ (r1 + x) % cfg.nodes.size ] }.toTypedArray()
    }

    fun objectId(obj:ByteArray) = obj.toObjectId()
    internal fun slotForId(id:ByteArray) = id.toSlot()
}