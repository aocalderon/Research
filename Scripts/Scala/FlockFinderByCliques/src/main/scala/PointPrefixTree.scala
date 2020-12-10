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
import com.vividsolutions.jts.geom.{GeometryFactory, Point}

import CliqueFinderUtils.findCenters

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

  import CliqueFinderUtils._

  /** Representing a node in an FP-Tree. */
  class Node(val parent: Node) extends Serializable {
    var point: Point = _
    var candidates: List[Disk] = List.empty
    var flocks: List[Disk] = List.empty
    var count: Long = 0L
    val children: mutable.Map[Point, Node] = mutable.Map.empty

    def isRoot: Boolean = parent == null

    def isLeaf: Boolean = children.isEmpty

    def isRootChild: Boolean = parent.isRoot

    def getNextWithBranches(): Node = {
      @tailrec
      def get(current: Node): Node = {
        if(current.children.size > 1)
          current
        else
          get(current.children.head._2)
      }
      get(this)
    }

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

    def updateDisks(epsilon: Double, r2: Double, mu: Int)
        (implicit geofactory: GeometryFactory, tolerance: Tolerance): Unit = {

      val points = getBranch
      val centers = findCenters(points, epsilon, r2)
      val join = for{
        p <- points
        c <- centers if c.distance(p) <= (epsilon / 2) + tolerance.value
      } yield {
        (c, p.getUserData.asInstanceOf[Int])
      }
      candidates = join.groupBy(_._1).mapValues(_.map(_._2).sorted)
        .map(d => Disk(d._1.getX, d._1.getY, d._2)).toList
      flocks = pruneDisks(candidates, mu)
    }

    def updateDisksFromParent(epsilon: Double, mu: Int)
        (implicit geofactory: GeometryFactory, tolerance: Tolerance): Unit = {
      def update(current: Node): Unit = {
        val p = current.point.getUserData.asInstanceOf[Int]
        println(s"P: ${p}\tC: ${current.candidates.size}\tF: ${current.flocks.size}")
        current.flocks.filter(_.pids.contains(p))
          .map(disk => s"${disk.pids.mkString(" ")} ${disk.wkt}")
          .foreach{println}
        save(s"/tmp/disks$p.txt"){
          current.candidates.map(_.wkt + "\n").sorted
        }
        val it = current.children.iterator
        while(it.hasNext){
          val next = it.next._2

          val r = epsilon / 2.0 + tolerance.value
          val (updateCands, prevCands) = current.candidates
            .partition(_.getCenter.distance(next.point) <= r)
          println(s"${next.point} intersects ${updateCands.size} candidates...")
          val updatedCands = updateCands.map{ cand =>
            val pid = next.point.getUserData.asInstanceOf[Int]
            val newPids = cand.pids :+ pid
            cand.copy(pids = newPids)
          }
          
          val (newCands, newFlocks) = updatedCands.partition(_.pids.size < mu)
          
          next.candidates = prevCands ++ updatedCands
          next.flocks = pruneDisks(current.flocks ++ newFlocks, mu)

          update(next)
        }
      }
      update(this)
    }

    def printNodes: Unit = {
      val it = children.iterator
      while(it.hasNext){
        val next = it.next._2
        val point = next.point
        val candidates = next.candidates
        val flocks = next.flocks
        val msg = s"${point.getUserData}\t" +
        s"# Candidates: ${candidates.size}\t" +
        s"#Flocks: ${flocks.size}" +
        s"\tisLeaf? ${next.isLeaf}"
        println(msg)
        flocks.foreach(println)
        next.printNodes
      }
    }

    def printLeaves: Unit = {
      val it = children.iterator
      while(it.hasNext){
        val next = it.next._2
        if(next.isLeaf){
          val point = next.point
          val candidates = next.candidates
          val flocks = next.flocks
          val msg = s"${point.getUserData}\t" +
          s"# Candidates: ${candidates.size}\t" +
          s"#Flocks: ${flocks.size}"
          println(msg)
          flocks.foreach(println)
        } else {
          next.printLeaves
        }
      }
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
    
    def toText: String = s"${point.toText}\t${point.getUserData}"
  }

  /** Summary of an item in an FP-Tree. */
  class Summary extends Serializable {
    var count: Long = 0L
    val nodes: ListBuffer[Node] = ListBuffer.empty
  }
}
