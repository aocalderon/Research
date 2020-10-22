package edu.ucr.dblab;

import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import scala.collection.JavaConverters._

object TestPBK {
  def main(args: Array[String]): Unit = {
    val graph = new SimpleGraph[Int, DefaultEdge](classOf[DefaultEdge])
    val vertices = List(1,2,3,4,5,6,7,8,9)
    val edges = List(
      (1,2),(1,9),
      (2,3),(2,9),
      (3,4),(3,8),(3,9),
      (4,5),(4,6),(4,7),(4,8),
      (5,6),
      (6,7),(6,8),
      (7,8)
    )
    vertices.foreach{ vertex =>  graph.addVertex(vertex) }
    edges.foreach{ case(a, b) => graph.addEdge(a, b) }

    val finder = new PBKCliqueFinder(graph)

    //println
    finder.lazyRun//.asScala.toList.map{
      //_.asScala.toList.mkString(",")
    //}//.foreach{println}
    
  }
}
