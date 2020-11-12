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

object PBK {

  def IK_*(R: HashSet[Int], P: HashSet[Int], X: HashSet[Int], level: Int = 0)
    (implicit graph: SimpleGraph[Int, DefaultEdge], sortMode: SortMode): Unit = {
    if(P.isEmpty && X.isEmpty){
      val r = sortMode.mode match {
        case 1 => R.toList
        case 2 => sortByDegree(R)
        case _ => sortById(R)
      }
      println(r.mkString(" "))
    } else {
      val u_p = pivot2(P, X)

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
    implicit val graph = new SimpleGraph[Int, DefaultEdge](classOf[DefaultEdge])
    implicit val sortMode = if(args.size == 2) SortMode(args(1).toInt) else SortMode(1)

    val filename = args(0).split("\\.")
    val (vertices, edges) = filename(1) match {
      case "tgf" => readTGF(filename(0) + ".tgf")
      case "tsv" => readTrajs(filename(0) + ".tsv", 10)
    }

    vertices.foreach(graph.addVertex)
    edges.foreach{ case(a, b) => graph.addEdge(a, b) }

    var R = HashSet[Int]()
    var P = HashSet[Int]()
    var X = HashSet[Int]()

    graph.vertexSet.asScala.foreach{ v => P.add(v)}

    IK_*(R, P, X)
  }

  def N(vertex: Int)
    (implicit graph: SimpleGraph[Int, DefaultEdge]): Set[Int] = {
    
    graph.edgesOf(vertex).asScala.map{ edge =>
      Graphs.getOppositeVertex(graph, edge, vertex)
    }.toSet
  }

  def pivot2(P: HashSet[Int], X: HashSet[Int])
    (implicit graph: SimpleGraph[Int, DefaultEdge]): Int = {

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

  def sortByDegree(R: HashSet[Int])
    (implicit graph: SimpleGraph[Int, DefaultEdge]): List[Int] = {

    case class Count(vertex: Int, count: Int)
    R.toList.map{ vertex =>
      Count(vertex, graph.edgesOf(vertex).size())
    }.sortBy(- _.count).map(_.vertex)
  }

  def sortById(R: HashSet[Int]): List[Int] = R.toList.sorted

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

  def readTrajs(filename: String, epsilon: Double)
    (implicit geofactory: GeometryFactory): (Set[Int], Set[(Int, Int)]) = {
    case class Vertex(point: Point, id: Int)
    val buffer = Source.fromFile(filename)
    val points = buffer.getLines.map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val x  = arr(1).toDouble
      val y  = arr(2).toDouble
      val point = geofactory.createPoint(new Coordinate(x, y))
      Vertex(point, id)
    }.toList.distinct

    //println(s"Number of points: ${points.size}")

    
    save(s"/tmp/edgesPoints.wkt"){
      points.map{ p =>
        s"${p.point.toText}\t${p.id}\n"
      }
    }

    val pairs = for{
      p1 <- points
      p2 <- points
      if p1.point.distance(p2.point) <= epsilon && p1.id < p2.id 
    } yield {
      (p1, p2)
    }

    save(s"/tmp/edgesPairs.wkt"){
      pairs.map{ case(a, b) =>
        val wkt = geofactory.createLineString(
          Array(a.point.getCoordinate, b.point.getCoordinate)
        )
        s"$wkt\n"
      }
    }


    buffer.close
    val vertices = points.map(_.id).toSet
    val edges = pairs.map{ case(a, b) => (a.id, b.id) }.toSet

    (vertices, edges)
  }

  import java.io.PrintWriter
  def save(filename: String)(content: Seq[String]): Unit = {
    val f = new PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    //println(s"Saved $filename [${content.size} records].")
  }
}
