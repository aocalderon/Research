package edu.ucr.dblab

/******************************************************************************************
Bron–Kerbosch Algorithm with pivot selection. 
[1] Cazals and Karande (2008) A note on the problem of reporting maximal clique.
[2] Tomita et al (2006) The worst-case time complexity for generating all maximal cliques 
    and computational experiments.                                                    
******************************************************************************************/

import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.Graphs
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Point, Coordinate}
import scala.collection.mutable.{ListBuffer, HashSet}
import scala.collection.JavaConverters._

object PPBK {

  def IK_*(R: HashSet[Point], P: HashSet[Point], X: HashSet[Point], level: Int = 0)
    (implicit graph: SimpleGraph[Point, DefaultEdge],
      cliques: FPTree[Point], sortMode: SortMode): Unit = {
    if(P.isEmpty && X.isEmpty){
      val r = sortMode.mode match {
        case 1 => R.toList
        case 2 => sortByDegree(R)
        case _ => sortById(R)
      }
      cliques.add(r)
      //println(r.mkString(" "))
    } else {
      val u_p = pivot(P, X)

      for( u_i <- P -- N(u_p)){
        val P_new = P.intersect(N(u_i))
        val X_new = X.intersect(N(u_i))
        val R_new = R.union(Set{u_i})

        P.remove{u_i}
        X.add{u_i}

        IK_*(R_new, P_new, X_new, level + 1)
      }
    }
  }

  case class SortMode(mode: Int)
  def main(args: Array[String]): Unit = {
    implicit val geofactory = new GeometryFactory(new PrecisionModel(1000))
    implicit val graph = new SimpleGraph[Point, DefaultEdge](classOf[DefaultEdge])
    implicit val sortMode = if(args.size == 3) SortMode(args(2).toInt) else SortMode(1)

    val (vertices, edges) = readTrajs(args(0), args(1).toDouble)

    vertices.foreach(graph.addVertex)
    edges.foreach{ case(a, b) => graph.addEdge(a, b) }

    implicit val cliques = new FPTree[Point]
    var R = HashSet[Point]()
    var P = HashSet[Point]()
    var X = HashSet[Point]()

    graph.vertexSet.asScala.foreach{ v => P.add(v)}

    IK_*(R, P, X)

    cliques.transactions.map(_._1.map(_.getUserData)).foreach{println}
  }

  def N(vertex: Point)
    (implicit graph: SimpleGraph[Point, DefaultEdge]): Set[Point] = {
    
    graph.edgesOf(vertex).asScala.map{ edge =>
      Graphs.getOppositeVertex(graph, edge, vertex)
    }.toSet
  }

  def pivot(P: HashSet[Point], X: HashSet[Point])
    (implicit graph: SimpleGraph[Point, DefaultEdge]): Point = {

    val px = P union X 

    val ve = for{
      v <- px
      e <- graph.edgesOf(v).asScala
      if P contains Graphs.getOppositeVertex(graph, e, v)
    } yield {
      (v,e)
    }

    if(ve.isEmpty){
      px.last
    } else {
      ve.groupBy(_._1).map{ c => (c._1, c._2.size) }.maxBy(_._2)._1
    }
  }

  def sortByDegree(R: HashSet[Point])
    (implicit graph: SimpleGraph[Point, DefaultEdge]): List[Point] = {

    case class Count(vertex: Point, count: Int)
    R.toList.map{ vertex =>
      Count(vertex, graph.edgesOf(vertex).size())
    }.sortBy(- _.count).map(_.vertex)
  }

  def sortByAngle(R: HashSet[Point]): List[Point] = {

    case class Count(p: Point, count: Int)
    R.toList.map{ vertex =>
      val spread = 1

      Count(vertex, spread)
    }.sortBy(- _.count).map(_.p)
  }

  def sortById(R: HashSet[Point]): List[Point] = {
    R.toList.map{ p =>
      val id = p.getUserData.asInstanceOf[Int]
      (id, p)
    }.sortBy(_._1).map(_._2)
  }

  import scala.io.Source
  def readTrajs(filename: String, epsilon: Double)
    (implicit geofactory: GeometryFactory): (Set[Point], Set[(Point, Point)]) = {

    val buffer = Source.fromFile(filename)
    val points = buffer.getLines.map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val x  = arr(1).toDouble
      val y  = arr(2).toDouble
      val point = geofactory.createPoint(new Coordinate(x, y))
      point.setUserData(id)
      point
    }.toSet

    val pairs = for{
      p1 <- points
      p2 <- points
      if p1.distance(p2) <= epsilon &&
      p1.getUserData.asInstanceOf[Int] < p2.getUserData.asInstanceOf[Int]
    } yield {
      (p1, p2)
    }
    buffer.close

    (points, pairs)
  }

  import java.io.PrintWriter
  def save(filename: String)(content: Seq[String]): Unit = {
    val f = new PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    println(s"Saved $filename [${content.size} records].")
  }
}
