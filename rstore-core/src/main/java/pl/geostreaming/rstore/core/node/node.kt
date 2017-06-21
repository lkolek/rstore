package pl.geostreaming.rstore.core.node

import pl.geostreaming.rstore.core.model.RsClusterDef

/**
 * Created by lkolek on 21.06.2017.
 */


/**
 *
 * NOTES:
 *  - currently cluster cfg can't change, but in the future we don't have to kill all nodes to apply SOME changes
 */
abstract class RsNode(val nodeId:Int, var cfg:RsClusterDef){


}