package edu.ucr.dblab

/******************************************************************************************
Bron–Kerbosch Algorithm with pivot selection. 
[1] Cazals and Karande (2008) A note on the problem of reporting maximal clique.
[2] Tomita et al (2006) The worst-case time complexity for generating all maximal cliques 
    and computational experiments.                                                    
******************************************************************************************/

import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.Graphs
import scala.collection.mutable.{ListBuffer, HashSet}
import scala.collection.JavaConverters._

object PBK {

  def IK_*(R: HashSet[Int], P: HashSet[Int], X: HashSet[Int], level: Int = 0)
    (implicit graph: SimpleGraph[Int, DefaultEdge]): Unit = {
    if(P.isEmpty && X.isEmpty){
      println(s"$R")
    } else {
      val u_p = pivot(P, X)

      for( u_i <- P -- neighbours(u_p)){
        P.remove{u_i}
        val P_new = P.intersect(neighbours(u_i))
        val X_new = X.intersect(neighbours(u_i))
        val R_new = R.union(Set{u_i})
        IK_*(R_new, P_new, X_new, level + 1)
        X.add{u_i}
      }
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val graph = new SimpleGraph[Int, DefaultEdge](classOf[DefaultEdge])

    val (vertices, edges) = readTGF(args(0))
    vertices.foreach(graph.addVertex)
    edges.foreach{ case(a, b) => graph.addEdge(a, b) }

    var R = HashSet[Int]()
    var P = HashSet[Int]()
    var X = HashSet[Int]()

    graph.vertexSet.asScala.foreach{ v => P.add(v)}

    IK_*(R, P, X)
  }

  def neighbours(vertex: Int)
    (implicit graph: SimpleGraph[Int, DefaultEdge]): HashSet[Int] = {
    var N = HashSet[Int]()
    graph.edgesOf(vertex).asScala.map{ edge =>
      N.add(Graphs.getOppositeVertex(graph, edge, vertex))
    }
    N
  }

  def pivot(P: HashSet[Int], X: HashSet[Int])
    (implicit graph: SimpleGraph[Int, DefaultEdge]): Int = {

    var max = -1
    var pivot = -1

    for(u <- P.union(X)){
      var count = 0
      for(e <- graph.edgesOf(u).asScala){
        if(P.contains(Graphs.getOppositeVertex(graph, e, u))){
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

  import scala.io.Source
  def readTGF(filename: String): (Set[Int], Set[(Int, Int)]) = {
    val buffer = Source.fromFile(filename)
    val lines = buffer.getLines.span(_ != "#")
    val vertices = lines._1.map(_.trim.toInt).toSet
    val edges = lines._2.filter(_ != "#").map{ line =>
      val arr = line.split("\t")
      (arr(0).trim.toInt, arr(1).trim.toInt)
    }.toSet
    buffer.close

    (vertices, edges)
  }
}
