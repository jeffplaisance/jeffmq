// Copyright 2010 Jeff Plaisance
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

package com.jeffplaisance.jeffmq

import java.util.concurrent.LinkedBlockingQueue
import org.jboss.netty.buffer.ChannelBuffers

/**
 * @author jplaisance
 */
object JeffMQConsole {
    import JeffMQ._
    
    def main(args: Array[String]) {
        val port = args(0).toInt
        val jeffMQ = new JeffMQExchange("cluster", port)
        new Thread(new Runnable() {
            def run = {
                val queue = new LinkedBlockingQueue[JeffMQMessage]
                jeffMQ.bindQueue(args(1), queue)
                while (true) {
                    val JeffMQMessage(routingKey, buffer) = queue.take
                    val (message,_) = readUtf8FromBuffer(buffer)
                    println(routingKey+":"+message)
                }
            }
        }).start
        while (true) {
            val line = scala.Console.readLine
            val index = line.indexOf(':')
            val (routingKey, message) = line.splitAt(index)
            val buffer = ChannelBuffers.dynamicBuffer
            writeUtf8ToBuffer(buffer, message)
            jeffMQ.send(routingKey, buffer)
        }
    }
}
