package edu.ucr.dblab

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
import scala.annotation.tailrec
import com.vividsolutions.jts.geom.Point

/**
 * Based on FP-Tree data structure used in Apache Spark FP-Growth.
 * @tparam item type is a Point.
 */
class PointPrefixTree extends Serializable {

  import PointPrefixTree._

  val root: Node = new Node(null)

  val summaries: mutable.Map[Point, Summary] = mutable.Map.empty

  /** Adds a transaction with count. */
  def add(t: Iterable[Point], count: Long = 1L): PointPrefixTree = {
    require(count > 0)
    var curr = root
    curr.count += count
    t.foreach { point =>
      val summary = summaries.getOrElseUpdate(point, new Summary)
      summary.count += count
      val child = curr.children.getOrElseUpdate(point, {
        val newNode = new Node(curr)
        newNode.point = point
        summary.nodes += newNode
        newNode
      })
      child.count += count
      curr = child
    }
    this
  }

  /** Returns all transactions in an iterator. */
  def transactions: Iterator[(List[Point], Long)] = getTransactions(root)

  /** Returns all transactions under this node. */
  private def getTransactions(node: Node): Iterator[(List[Point], Long)] = {
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
}

object PointPrefixTree {

  /** Representing a node in an FP-Tree. */
  class Node(val parent: Node) extends Serializable {
    var point: Point = _
    var count: Long = 0L
    val children: mutable.Map[Point, Node] = mutable.Map.empty

    def isRoot: Boolean = parent == null

    def getBranch: List[Point] = {
      @tailrec
      def getBranch(current: Node, branch: List[Point]): List[Point] = {
        if(current.isRoot)
          branch
        else
          getBranch(current.parent, current.point +: branch)
      }

      getBranch(this, List.empty[Point])
    }

    def printTree: String = {
      var buffer = ListBuffer[String]()
      printTree(buffer, "", "").mkString("")
    }
    private def printTree(buffer: ListBuffer[String], prefix: String,
      childrenPrefix: String): ListBuffer[String] = {

      if(!this.isRoot){
        val toPrint = s"${point.getUserData} ($count)"
        buffer.append(prefix)
        buffer.append(toPrint)
        buffer.append("\n")
      }
      val it = children.iterator
      while (it.hasNext) {
        val next = it.next
        
        if (it.hasNext) {
          next._2.printTree(buffer, childrenPrefix + "├─ ", childrenPrefix + "│  ");
        } else {
          next._2.printTree(buffer, childrenPrefix + "└─ ", childrenPrefix + "   ");
        }
      }
      buffer
    }

    def toDot(): ListBuffer[String] = {
      var nodes = ListBuffer[String]()
      var edges = ListBuffer[String]()
      toDot(nodes, edges, 0)
      nodes ++ edges
    }
    private def toDot(nodes: ListBuffer[String],
      edges: ListBuffer[String], nId: Int): Int = {

      if(!this.isRoot){
        val iId = point.getUserData.toString
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
  class Summary extends Serializable {
    var count: Long = 0L
    val nodes: ListBuffer[Node] = ListBuffer.empty
  }
}
