package pl.geostreaming.rstore.core.util

import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.launch
import kotlin.coroutines.experimental.CoroutineContext

/**
 * Created by lkolek on 10.07.2017.
 */


fun < E, F> SendChannel<E>.map( context: CoroutineContext, cap:Int = 10, mapper: (F) -> E ):SendChannel<F> {
    val ch = Channel<F>( cap );
    val me = this;
    launch(context){
        ch.consumeEach {  x->
            me.send(mapper.invoke(x))
        }
    }
    return ch;
}

fun < E, F> ReceiveChannel<E>.map( context: CoroutineContext, cap:Int = 10, mapper: (E) -> F ):ReceiveChannel<F> {
    val ch = Channel<F>( cap );
    val me = this;
    launch(context){
        me.consumeEach{ x->
            ch.send(mapper.invoke(x));
        }
    }
    return ch;
}