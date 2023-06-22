package edu.ucr.dblab

import scala.collection.JavaConverters._
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.alg.clique.BronKerboschCliqueFinder
import java.io.PrintWriter
import scala.io.Source

object GraphViewer {
  def main(args: Array[String]): Unit = {
    val graph = new SimpleGraph[String, DefaultEdge](classOf[DefaultEdge])

    val input = "/opt/mace22/test100.grh"
    val delimiter = ","

    val buffer = Source.fromFile(input)
    val iterator = for {
      line <- buffer.getLines.zipWithIndex
    } yield {
      val neighbours = line._1.split(delimiter)
      val index = line._2.toString
      (index, neighbours)
    }
    val adjacencies = iterator.toList
    buffer.close

    val vertices = 0 until adjacencies.size
    vertices.foreach{ vertex =>  graph.addVertex(vertex.toString) }

    vertices.foreach{ println }

    val edges = adjacencies.flatMap{ case(index, neighbours) =>
      neighbours.map{ neighbour => (index, neighbour)}      
    }.filter(_._2 != "")

    edges.foreach{println}

    edges.foreach{ edge => graph.addEdge(edge._1, edge._2) }

    val cliques= {
      val finder = new BronKerboschCliqueFinder(graph)
      finder.iterator.asScala.toList
    }

    val f = new PrintWriter("/tmp/cliques.txt")
    val triangles = cliques.map{ clique =>
      val triangle = clique.asScala.toList.map(_.toInt).sorted.reverse.mkString(" ")
      s"$triangle\n"
    }.mkString("")
    f.write(triangles)
    f.close
  }
}
