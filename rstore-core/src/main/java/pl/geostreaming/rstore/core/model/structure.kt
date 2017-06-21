package pl.geostreaming.rstore.core.model

/**
 * Created by lkolek on 21.06.2017.
 */

/**
 * Simple configuration / structure of cluster.
 * Currently: static, can not be rebalanced.
 * Nodes can be changed (address), restarted from scratch
 */
data class RsClusterStruct(val nodes:Array<RsNode>) {
    private constructor(builder:Builder) : this( builder.nodes.toTypedArray());
    init {
        if(HashSet(nodes.asList().map { it.id }).size < nodes.size
            || HashSet(nodes.asList().map { it.addr }).size < nodes.size
                )
            throw RuntimeException("Nodes must be unique");
    }

    class Builder {
        var nodes = ArrayList<RsNode>()
            private set;
        fun node( id: Int, addr: String) = apply {this.nodes.add(RsNode(id,addr))}

        fun build() = RsClusterStruct(this)
    }


    fun dump(){
        nodes.forEach{ println(it) }
    }
}

data class RsNode(val id: Int, val addr: String)
