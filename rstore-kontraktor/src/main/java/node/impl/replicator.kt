package pl.geostreaming.rstore.core.node.impl

import org.mapdb.Atomic
import org.mapdb.BTreeMap
import org.mapdb.DB
import pl.geostreaming.rstore.core.node.RsNodeActor

/**
 * Created by lkolek on 24.06.2017.
 */


/**
 * this class provides retriving behaviour (invoking get with proper handling)
 */
class Retriver (val myRepl:RsNodeActorImpl ){
    val currentOps = HashSet<ByteArray >();
}

/**
 * This class provides replication for r1 to r2
 */
class Replicator (
        val myRepl:RsNodeActorImpl ,
        val fromReplId:Int,
        val fromRepl:RsNodeActor,
        val db:DB) {
    val lastSeqIdRemote:   Atomic.Long = db.get("lastSeqIdRemote." + myRepl.id)
    val fullyReplicatedTo: Atomic.Long = db.get("fullyReplicatedTo." + myRepl.id)

    init {


    }
}

