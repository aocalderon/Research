package edu.ucr.dblab

import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.Graphs
import scala.collection.mutable.{ListBuffer, HashSet}
import scala.collection.JavaConverters._

object Tomita {
  var Q = HashSet[Int]()

  def expand(subg: HashSet[Int], cand: HashSet[Int], index: Int)
    (implicit graph: SimpleGraph[Int, DefaultEdge]): Unit = {
    println(s"SUBG: $subg")
    println(s"CAND: $cand")
    if(subg.isEmpty){
      println(s"clique! [$Q]")
    } else {
      val u = pivot(subg, cand)
      println(s"u: $u N(u): ${neighbours(u)}")
      println(s"candidates: ${cand -- neighbours(u)}")
      for( q <- cand -- neighbours(u)){
        print(q + ",")
        Q.add(q)
        val subg_q = subg.intersect(neighbours(q))
        val cand_q = cand.intersect(neighbours(q))
        expand(subg_q, cand_q, index + 1)
        cand.remove{q}

        print("back,")
        Q.remove(q)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val graph = new SimpleGraph[Int, DefaultEdge](classOf[DefaultEdge])
    val points = Set(1,2,3,4,5,6,7,8,9)
    val pairs = Set(
      (1,2), (1,9),
      (2,3), (2,9),
      (3,4), (3,8), (3,9),
      (4,5), (4,6), (4,7), (4,8),
      (5,6),
      (6,7), (6,8),
      (7,8)
    )

    points.foreach(graph.addVertex)
    pairs.foreach{ case(a, b) => graph.addEdge(a, b) }

    var V = HashSet[Int]()
    graph.vertexSet.asScala.foreach{ v => V.add(v)}
    expand(V, V, 0)
    println
  }

  def neighbours(vertex: Int)
    (implicit graph: SimpleGraph[Int, DefaultEdge]): HashSet[Int] = {
    var N = HashSet[Int]()
    graph.edgesOf(vertex).asScala.map{ edge =>
      N.add(Graphs.getOppositeVertex(graph, edge, vertex))
    }
    N
  }

  def pivot(subg: HashSet[Int], cand: HashSet[Int])
    (implicit graph: SimpleGraph[Int, DefaultEdge]): Int = {

    var max = -1
    var pivot = -1

    for(u <- subg){
      var count = 0
      for(e <- graph.edgesOf(u).asScala){
        if(cand.contains(Graphs.getOppositeVertex(graph, e, u))){
          count = count + 1
        }
      }
      if(count >= max){
        max = count
        pivot = u
      }
    }

    pivot
  }
}
