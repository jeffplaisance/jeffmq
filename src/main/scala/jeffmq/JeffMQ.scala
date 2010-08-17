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

package jeffmq

import org.zeromq.ZMQ._
import collection.JavaConversions
import org.jgroups._
import com.google.common.base.Charsets
import java.io._
import org.jboss.netty.buffer.{ChannelBufferInputStream, ChannelBufferOutputStream, ChannelBuffers, ChannelBuffer}
import com.google.common.io.{CharStreams, ByteStreams}
import java.nio.ByteBuffer
import collection.mutable.{HashSet, HashMap}
import java.net.InetAddress
import java.util.concurrent.{LinkedBlockingQueue, ConcurrentHashMap, BlockingQueue}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import org.zeromq.ZMQ

/**
 * @author jplaisance
 */

object JeffMQ {
    def readUtf8FromBuffer(buffer:ChannelBuffer):(String, ChannelBuffer) = {
        val length = buffer.readInt
        val writer = new StringWriter()
        CharStreams.copy(new InputStreamReader(new ChannelBufferInputStream(buffer, length), Charsets.UTF_8), writer)
        val message = writer.toString
        (message, buffer.slice)
    }

    def writeUtf8ToBuffer(buffer:ChannelBuffer, str:String):Unit = {
        val out = new ChannelBufferOutputStream(buffer)
        val start = buffer.writerIndex
        out.writeInt(0)
        val writer = new OutputStreamWriter(out, Charsets.UTF_8)
        CharStreams.copy(new StringReader(str), writer)
        writer.close()
        buffer.setInt(start, out.writtenBytes-4)
    }
}

class JeffMQ(private val port:Int, private val protocol:String = "tcp", jGroupsConfigPath:Option[String] = None) {

    import JavaConversions.asIterator
    import JeffMQ._

    private val zmqContext = ZMQ.context(2)

    private val sendQueue = new LinkedBlockingQueue[(String, Array[Byte])]

    private val sockets = JavaConversions.asConcurrentMap(new ConcurrentHashMap[String, Socket]) //addresses to sockets

    private val bindings = new TopicTrie[BlockingQueue[JeffMQMessage]] //routing keys to queues, local
    private val bindingsLock = new ReadWriteLock(true)

    private val routingTable = new TopicTrie[AddressPair] //routing keys to addresses, global
    private val routingTableLock = new ReadWriteLock(true)

    private val viewCount = new AtomicLong

    private val localAddress = InetAddress.getLocalHost.getHostAddress+":"+port

    private val ADD = 0;
    private val REMOVE = 1;

    private val jChannel = jGroupsConfigPath.map(x => new JChannel(x)).getOrElse(new JChannel)
    jChannel.setReceiver(new JeffMQReceiverAdapter)
    jChannel.connect("cluster")
    jChannel.getState(null, 0)
    private val jGroupsAddress = jChannel.getAddress
    private val jGroupsId = jGroupsAddress.toString

    val recvRun = new AtomicBoolean(true)
    new Thread(new Runnable() {
        def run = {
            val recvSocket = zmqContext.socket(UPSTREAM)
            recvSocket.bind(protocol+"://"+localAddress)
            while (recvRun.get) {
                val JeffMQMessage(routingKey, buffer) = JeffMQMessage.parse(recvSocket.recv(0))
                val b = bindingsLock.readLock(bindings.lookup(routingKey))
                b.foreach(x => x.put(JeffMQMessage(routingKey, buffer.slice)))
            }
        }
    }).start

    val sendRun = new AtomicBoolean(true)
    new Thread(new Runnable() {
        def run: Unit = {
            while (sendRun.get) {
                val (ip, message) = sendQueue.take
                sockets.getOrElseUpdate(ip, {
                    val socket = zmqContext.socket(DOWNSTREAM)
                    socket.connect(protocol+"://"+ip)
                    socket
                }).send(message, NOBLOCK)
            }
        }
    }).start

    override def finalize = {sendRun.set(false); recvRun.set(false)}

    def bindQueue(routingKey:String, queue:BlockingQueue[JeffMQMessage]) = {
        bindingsLock.writeLock(bindings.addBinding(routingKey, queue))
        setupRoute(routingKey)
    }

    def unbindQueue(routingKey:String, queue:BlockingQueue[JeffMQMessage]) = {
        bindingsLock.writeLock.lock
        bindings.removeBinding(routingKey, queue)
        // I sincerely apologize for this locking construct from hell, but removeRoute
        // sends a reliable message over the network so it can take an arbitrarily long time
        if (bindings.get(routingKey).isEmpty) {
            bindingsLock.writeLock.unlock
            removeRoute(routingKey)
        } else bindingsLock.writeLock.unlock
    }

    def send(routingKey:String, message:ChannelBuffer):Unit = send(routingKey, new ChannelBufferInputStream(message))
    def send(routingKey:String, message:Array[Byte]):Unit = send(routingKey, new ByteArrayInputStream(message))
    def send(routingKey:String, message:ByteBuffer):Unit = send(routingKey, ChannelBuffers.wrappedBuffer(message))
    
    def send(routingKey:String, message:InputStream):Unit = {
        val buffer = ChannelBuffers.dynamicBuffer
        val out = new ChannelBufferOutputStream(buffer)
        writeUtf8ToBuffer(buffer, routingKey)
        ByteStreams.copy(message, out)
        val bytes = ByteStreams.toByteArray(new ChannelBufferInputStream(buffer))
        val r = routingTableLock.readLock(routingTable.lookup(routingKey))
        r.foreach(x => sendQueue.put((x.ip, bytes)))
    }

    private def setupRoute(routingKey: String): Unit = {
        modifyRoute(routingKey, ADD)
    }

    private def removeRoute(routingKey: String): Unit = {
        modifyRoute(routingKey, REMOVE)
    }

    private def modifyRoute(routingKey: String, mod:Int): Unit = {
        val buffer = ChannelBuffers.dynamicBuffer
        writeUtf8ToBuffer(buffer, routingKey)
        buffer.writeByte(mod)
        writeUtf8ToBuffer(buffer, localAddress)
        writeUtf8ToBuffer(buffer, jGroupsId)
        val bytes = ByteStreams.toByteArray(new ChannelBufferInputStream(buffer))
        jChannel.send(null, jGroupsAddress, bytes)
    }

    private class JeffMQReceiverAdapter extends ReceiverAdapter {
        override def viewAccepted(newView: View) = {
            val viewNum = viewCount.incrementAndGet
            val deleteOldRoutes = new Runnable() {
                def run = {
                    val members = new HashSet[String]
                    newView.getMembers.iterator.foreach(x => {
                        members+=x.toString
                    })
                    routingTableLock.writeLock({
                        if (viewNum == viewCount.get) {
                            val remove = new HashMap[String, AddressPair]
                            routingTable.foreach(set => set._2.foreach(entry => if (!members.contains(entry.jg)) remove.put(set._1, entry)))
                            remove.foreach(entry => {
                                routingTable.removeBinding(entry._1, entry._2)
                                sockets.remove(entry._2.ip)
                            })
                        }
                    })
                }
            }
            newView match {
                case mergeView:MergeView =>
                    new Thread(new Runnable() {
                        def run = {
                            val (_, primary) = mergeView.getSubgroups.iterator.foldLeft[(Int, Option[View])]((0, None))((b, a) => if (a.size() > b._1) (a.size(), Some(a)) else b)
                            if (!primary.exists(x => x.containsMember(jGroupsAddress))) {
                                jChannel.getState(null, 0)
                                val b = bindingsLock.readLock(bindings.keySet.toList)
                                b.foreach(routingKey => setupRoute(routingKey))
                            }
                            deleteOldRoutes.run
                        }
                    }).start
                case _ => new Thread(deleteOldRoutes).start
            }
        }

        override def receive(msg: Message) = {
            val JeffMQMessage(routingKey, buffer) = JeffMQMessage.parse(msg.getBuffer)
            val mod = buffer.readByte&0xFF
            val (ipPort,buffer2) = readUtf8FromBuffer(buffer)
            val (jGroupsAddr, _) = readUtf8FromBuffer(buffer2)
            routingTableLock.writeLock({
                mod match {
                    case ADD => routingTable.addBinding(routingKey, AddressPair(ipPort, jGroupsAddr))
                    case REMOVE => routingTable.removeBinding(routingKey, AddressPair(ipPort, jGroupsAddr))
                }
            })
        }

        override def setState(state: Array[Byte]) = {
            val in = new ObjectInputStream(new ByteArrayInputStream(state))
            val newRoutingTable = in.readObject.asInstanceOf[TopicTrie[AddressPair]]
            in.close
            routingTableLock.writeLock(routingTable++=(newRoutingTable))
        }

        override def getState:Array[Byte] = {
            val byteStream = new ByteArrayOutputStream
            val out = new ObjectOutputStream(byteStream)
            routingTableLock.readLock(out.writeObject(routingTable))
            out.close
            byteStream.toByteArray
        }
    }
}

object JeffMQMessage {

    def parse(bytes:Array[Byte]) = {
        val buffer = ChannelBuffers.wrappedBuffer(bytes)
        val length = buffer.readInt
        buffer.skipBytes(length)
        JeffMQMessage(new String(bytes, 4, length, Charsets.UTF_8), buffer.slice)
    }
}

final case class JeffMQMessage(routingKey:String, bytes:ChannelBuffer)

private[jeffmq] final case class AddressPair(val ip:String, val jg:String) extends Ordered[AddressPair] {
    def compare(that: AddressPair):Int = {
        val ipCmp = ip.compareTo(that.ip)
        if (ipCmp != 0) ipCmp else jg.compareTo(that.jg)
    }
}
