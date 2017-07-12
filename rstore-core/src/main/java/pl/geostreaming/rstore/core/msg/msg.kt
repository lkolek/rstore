package pl.geostreaming.rstore.core.msg

import pl.geostreaming.rstore.core.model.HeartbitData
import pl.geostreaming.rstore.core.model.IdList
import pl.geostreaming.rstore.core.model.NewId
import pl.geostreaming.rstore.core.model.ObjId
import java.io.Serializable

/**
 * Created by lkolek on 30.06.2017.
 */

interface RsOp:Serializable{
    val opid:Long
}

open abstract class RsOpReq():RsOp
open abstract class RsOpResp():RsOp {}

data class RsOpReq_queryIds(override val opid:Long,val afertSeqId:Long, val cnt:Int):RsOpReq()
data class RsOpRes_queryIds(override val opid:Long,val ret:IdList):RsOpResp()

data class RsOpReq_put(override val opid:Long,val obj:ByteArray):RsOpReq()
data class RsOpRes_put(override val opid:Long,val ret:ObjId):RsOpResp()

data class RsOpReq_get(override val opid:Long,val oid:ObjId):RsOpReq()
data class RsOpRes_get(override val opid:Long,val ret:ByteArray?):RsOpResp()

data class RsOpReq_has(override val opid:Long,val oid:ObjId):RsOpReq()
data class RsOpRes_has(override val opid:Long,val ret:Boolean):RsOpResp()

data class RsOpReq_listenNewIds(override val opid:Long):RsOpReq()
data class RsOpRes_listenNewIds_send(override val opid:Long,val ret: NewId):RsOpResp()
data class RsOpRes_listenNewIds(override val opid:Long):RsOpResp()

data class RsOpRes_bad(override val opid:Long,val ret:String):RsOpResp()


data class RsHeartbit(val hb:HeartbitData, override val opid:Long = 0L):RsOp