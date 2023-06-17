package edu.ucr.dblab.pflock.pbk

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ListBuffer
import org.locationtech.jts.geom.Point

/**
 * FP-Tree data structure used in FP-Growth.
 * @tparam T item type
 */
class FPTree[T] extends Serializable {

  import FPTree._

  val root: Node[T] = new Node(null)

  private val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty

  def print = println(root.printTree)

  /** Adds a transaction with count. */
  def add(t: Iterable[T], count: Long = 1L): FPTree[T] = {
    require(count > 0)
    var curr = root
    curr.count += count
    t.foreach { item =>
      val summary = summaries.getOrElseUpdate(item, new Summary)
      summary.count += count
      val child = curr.children.getOrElseUpdate(item, {
        val newNode = new Node(curr)
        newNode.item = item
        summary.nodes += newNode
        newNode
      })
      child.count += count
      curr = child
    }
    this
  }

  /** Merges another FP-Tree. */
  def merge(other: FPTree[T]): FPTree[T] = {
    other.transactions.foreach { case (t, c) =>
      add(t, c)
    }
    this
  }

  /** Gets a subtree with the suffix. */
  private def project(suffix: T): FPTree[T] = {
    val tree = new FPTree[T]
    if (summaries.contains(suffix)) {
      val summary = summaries(suffix)
      summary.nodes.foreach { node =>
        var t = List.empty[T]
        var curr = node.parent
        while (!curr.isRoot) {
          t = curr.item :: t
          curr = curr.parent
        }
        tree.add(t, node.count)
      }
    }
    tree
  }

  /** Returns all transactions in an iterator. */
  def iterator: Iterator[List[T]] = transactions.map(_._1)
  def transactions: Iterator[(List[T], Long)] = getTransactions(root)

  /** Returns all transactions under this node. */
  private def getTransactions(node: Node[T]): Iterator[(List[T], Long)] = {
    var count = node.count
    node.children.iterator.flatMap { case (item, child) =>
      getTransactions(child).map { case (t, c) =>
        count -= c
        (item :: t, c)
      }
    } ++ {
      if (count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }

  /** Extracts all patterns with valid suffix and minimum count. */
  def extract(
      minCount: Long,
      validateSuffix: T => Boolean = _ => true): Iterator[(List[T], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item) && summary.count >= minCount) {
        Iterator.single((item :: Nil, summary.count)) ++
          project(item).extract(minCount).map { case (t, c) =>
            (item :: t, c)
          }
      } else {
        Iterator.empty
      }
    }
  }
}

object FPTree {

  /** Representing a node in an FP-Tree. */
  class Node[T](val parent: Node[T]) extends Serializable {
    var item: T = _
    var count: Long = 0L
    val children: mutable.Map[T, Node[T]] = mutable.Map.empty

    def isRoot: Boolean = parent == null

    /** **/
    def printTree(): String = {
      var buffer = ListBuffer[String]()
      printT(buffer, "", "").mkString("")
    }
    private def printT(buffer: ListBuffer[String], prefix: String, childrenPrefix: String):
        ListBuffer[String] = {

      if(!this.isRoot){

        val toPrint = s"${item.asInstanceOf[Point].getUserData}"
        buffer.append(prefix)
        buffer.append(toPrint)
        buffer.append("\n")
      }
      val it = children.iterator
      while (it.hasNext) {
        val next = it.next
        
        if (it.hasNext) {
          next._2.printT(buffer, childrenPrefix + "├─ ", childrenPrefix + "│  ");
        } else {
          next._2.printT(buffer, childrenPrefix + "└─ ", childrenPrefix + "   ");
        }
      }
      buffer
    }

    def toDot(): ListBuffer[String] = {
      var nodesT = ListBuffer[String]()
      var edgesT = ListBuffer[String]()
      toDot(nodesT, edgesT, 0)
      nodesT ++ edgesT
    }
    private def toDot(nodes: ListBuffer[String], edges: ListBuffer[String], nId: Int):
        Int = {

      if(!this.isRoot){
        val iId = item.asInstanceOf[Point].getUserData.toString
        val iLabel = s"${iId} ($count)"
        nodes.append(s"""$nId [label="$iLabel"];\n""")
      } else {
        nodes.append(s"""$nId [label="*"];\n""")
      }

      val it = children.iterator
      var newId = nId
      while (it.hasNext) {
        val next = it.next
        edges.append(s"$nId -> ${newId + 1};\n")
        newId = next._2.toDot(nodes, edges, newId + 1);
      }
      newId
    }
    
  }

  /** Summary of an item in an FP-Tree. */
  private class Summary[T] extends Serializable {
    var count: Long = 0L
    val nodes: ListBuffer[Node[T]] = ListBuffer.empty
  }
}
