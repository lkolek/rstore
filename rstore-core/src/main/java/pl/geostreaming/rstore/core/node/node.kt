package pl.geostreaming.rstore.core.node

import org.nustaq.kontraktor.Actor
import org.nustaq.kontraktor.Actors
import org.nustaq.kontraktor.IPromise
import org.nustaq.kontraktor.Promise
import org.nustaq.kontraktor.annotations.Local
import pl.geostreaming.rstore.core.model.RsClusterDef

/**
 * Created by lkolek on 21.06.2017.
 */


/**
 *
 * NOTES:
 *  - currently cluster cfg can't change, but in the future we don't have to kill all nodes to apply SOME changes
 */
open class RsNodeActor : Actor<RsNodeActor>() {

    open fun test1(test:String):IPromise<String> {  return Actors.reject(RuntimeException("UNIMPLEMENTED")); }

}

