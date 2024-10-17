package edu.ucr.dblab.pflock.pbk

/******************************************************************************************
Bron–Kerbosch Algorithm with pivot selection. 
[1] Cazals and Karande (2008) A note on the problem of reporting maximal clique.
[2] Tomita et al (2006) The worst-case time complexity for generating all maximal cliques 
    and computational experiments.                                                    
******************************************************************************************/

import org.jgrapht.graph.{DefaultEdge, SimpleGraph}
import org.jgrapht.Graphs
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}

import scala.collection.mutable.{HashSet, ListBuffer}
import scala.collection.JavaConverters._
import edu.ucr.dblab.pflock.pbk.PBK_Utils._
import edu.ucr.dblab.pflock.Utils._

import scala.annotation.tailrec

object PBK {

  def bk(vertices: List[Point], edges: List[(Point, Point)]): FPTree[Point] = {
    implicit val graph = new SimpleGraph[Point, DefaultEdge](classOf[DefaultEdge])
    implicit val cliques = new FPTree[Point]
    implicit val sortMode = SortMode(2)

    vertices.foreach(graph.addVertex)
    edges.foreach{ case(a, b) => graph.addEdge(a, b) }

    var R = HashSet[Point]()
    var P = HashSet[Point]()
    var X = HashSet[Point]()

    graph.vertexSet.asScala.foreach{ v => P.add(v)}
    IK_*(R, P, X)

    cliques
  }

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
}
