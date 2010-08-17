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

import java.lang.String
import collection.Iterator
import collection.immutable.Queue
import collection.mutable._

/**
 * @author jplaisance
 */
@serializable @SerialVersionUID(1L)
class TopicTrie[V] extends MultiMap[String, V] {

    val root = TopicTrieNode[V](new HashMap[String,TopicTrieNode[V]], new HashSet[V])

    def get(key: String): Option[HashSet[V]] = {
        val parts = getParts(key)
        TopicTrieNode.get(root, parts)
    }
    
    def lookup(key:String):Set[V] = {
        val parts = getParts(key)
        TopicTrieNode.lookup(root, parts)
    }

    def iterator: Iterator[(String, Set[V])] = {
        TopicTrieNode.iterator(Queue.empty[String], root)
    }

    def -=(key: String) = {
        val parts = getParts(key)
        TopicTrieNode.remove(root, parts)
        this
    }

    def +=(kv: (String, Set[V])) = {
        val parts = getParts(kv._1)
        TopicTrieNode.add(root, parts, kv._2)
        this
    }

    private def getParts(key:String):List[String] = key.split('.').toList
}

@serializable @SerialVersionUID(1L)
final case class TopicTrieNode[V](nodes:Map[String, TopicTrieNode[V]], values:HashSet[V])

object TopicTrieNode {

    def empty[V] = new TopicTrieNode(new HashMap[String, TopicTrieNode[V]], new HashSet[V])

    def lookup[V](node:TopicTrieNode[V], parts:List[String]):HashSet[V] = {
        val results = new HashSet[V]
        lookup(node, parts, results)
        results
    }

    private def lookup[V](node:TopicTrieNode[V], parts:List[String], results:HashSet[V]):Unit = {
        val TopicTrieNode(nodes, values) = node
        parts match {
            case x::xs =>
                nodes.get("#").foreach(y => {
                    lookup(node, xs, results)
                    lookup(y, xs, results)
                })
                nodes.get("*").foreach(y => lookup(y, xs, results))
                nodes.get(x).foreach(y => lookup(y, xs, results))
            case Nil =>
                results++=values
        }
    }

    def remove[V](node:TopicTrieNode[V], parts:List[String]):Unit = {
        val TopicTrieNode(nodes, values) = node
        parts match {
            case x::xs => nodes.get(x).foreach(y => remove(y, xs))
            case Nil => values.clear
        }
    }

    def add[V](node:TopicTrieNode[V], parts:List[String], vals:Set[V]):Unit = {
        val TopicTrieNode(nodes, values) = node
        parts match {
            case x::xs => add(nodes.getOrElseUpdate(x, TopicTrieNode.empty[V]), xs, vals)
            case Nil => values++=vals
        }
    }

    def get[V](node:TopicTrieNode[V], parts:List[String]):Option[HashSet[V]] = {
        val TopicTrieNode(nodes, values) = node
        parts match {
            case x::xs => nodes.get(x).flatMap(y => get(y, xs))
            case Nil => if (values.isEmpty) None else Some(values)
        }
    }

    def iterator[V](queue:Queue[String], node:TopicTrieNode[V]):Iterator[(String, HashSet[V])] = {
        val TopicTrieNode(nodes, values) = node
        val results = nodes.map(x => iterator(queue.enqueue(x._1), x._2)).iterator.flatten
        if (queue.isEmpty) results else results++Iterator.single((queue.mkString("."), values))
    }
}
