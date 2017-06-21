package pl.geostreaming.rstore.core.model

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
data class RsClusterDef(
        val nodes:Array<NodeDef>,
        val rf:Int = 1,
        val minRepl:Int = 1,
        val REPL_SLOT_SIZE:Int = 1013
) {

    init {
        if(HashSet(nodes.asList().map { it.id }).size < nodes.size
            || HashSet(nodes.asList().map { it.addr }).size < nodes.size
                )
            throw RuntimeException("Nodes must be unique");
        if(nodes.size < rf)
            throw RuntimeException("Replication factor must be at least the size of cluster!");
    }

    private constructor(builder:Builder) : this(
            builder.nodes.toTypedArray(),
            builder.rf,
            builder.mr,
            builder.rss
    );

    class Builder {
        var nodes = ArrayList<NodeDef>()
            private set
        var rf = 1
            private set
        var mr = 1
            private set

        var rss = 1013
            private set


        fun withReplicationFactor(rf:Int) = apply { this.rf = rf }
        fun withMinReplication(mr:Int) = apply {this.mr = mr}
        fun withNode( id: Int, addr: String) = apply {this.nodes.add(NodeDef(id,addr))}

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

    data class NodeDef(val id: Int, val addr: String)
}



class RsCluster(val cfg:RsClusterDef){
    private val md = MessageDigest.getInstance("SHA-256");

    fun ByteArray.toObjectId():ByteArray{
        return md.digest(this)
    }

    fun ByteArray.toSlot():Int{
        val ib = ByteBuffer.wrap(this).asIntBuffer();
        val a = ib.get().xor(ib.get()).xor(ib.get()).and(0x7fffffff)
        return (a % (cfg.REPL_SLOT_SIZE ));
    }



    fun objectId(obj:ByteArray) = obj.toObjectId()
    fun slotForId(id:ByteArray) = id.toSlot()
}