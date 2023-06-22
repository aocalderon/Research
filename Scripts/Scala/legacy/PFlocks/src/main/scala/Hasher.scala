package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Coordinate, Point}

import scala.collection.mutable.{Map, ListBuffer}
import scala.collection.JavaConverters._

import scala.annotation.tailrec

import Utils.Data

object Hasher {
  case class P(key: Set[Long], points: List[Point])

  def main(args: Array[String]): Unit = {

    val geofactory = new GeometryFactory()
    val p1 = geofactory.createPoint(new Coordinate(1, 1))
    p1.setUserData(Data(1,1))
    val p2 = geofactory.createPoint(new Coordinate(1, 1))
    p2.setUserData(Data(2,1))
    val p3 = geofactory.createPoint(new Coordinate(1, 1))
    p3.setUserData(Data(3,1))
    val p4 = geofactory.createPoint(new Coordinate(1, 1))
    p4.setUserData(Data(4,1))
    val p5 = geofactory.createPoint(new Coordinate(1, 1))
    p5.setUserData(Data(5,1))
    val p6 = geofactory.createPoint(new Coordinate(1, 1))
    p6.setUserData(Data(6,1))
    val p7 = geofactory.createPoint(new Coordinate(1, 1))
    p7.setUserData(Data(7,1))
    val p8 = geofactory.createPoint(new Coordinate(1, 1))
    p8.setUserData(Data(8,1))
    val p9 = geofactory.createPoint(new Coordinate(1, 1))
    p9.setUserData(Data(9,1))
    val p0 = geofactory.createPoint(new Coordinate(1, 1))
    p0.setUserData(Data(0,1))

    val cliques = List(
      List(p1,p2,p3),
      List(p2,p3),
      List(p4,p5,p6),
      List(p2,p3,p4),
      List(p5,p6,p7),
      List(p1,p8),
      List(p2,p3,p4,p5,p6),
      List(p2,p9)
    )
    val C = cliques.map{ points =>
      val key = points.map(_.getUserData.asInstanceOf[Data].id).toSet
      P(key, points)
    }
    val threshold = 2
    val L = List[(Set[Long], Int)]()
    val S = List[P]()
    val G = List[(Int, P)]()

    val r = run(C, threshold, L, S, G)

    r.map{ case(index, p) =>
      val pids = p.points.map(_.getUserData.asInstanceOf[Data].id)

      s"$index\t${pids.mkString(" ")}"
    }.foreach(println)

    val clusteredOuts = r.groupBy(_._1).map{ case(key, values) =>
      values.map{ case(k, v) => v.points }.flatten.distinct
    }

    clusteredOuts.foreach(println)
  }

  def run(cliques: List[P], threshold: Int,
    L: List[(Set[Long], Int)],
    S: List[P],
    G: List[(Int, P)]
  ): List[(Int, P)] = {

    cliques match {
      case Nil => {
        G ++ S.map( s => (-1, s))
      }
      case head :: tail => {
        val c = head
        val index = searchL(L, c)
        if(index >= 0){
          val newG = G :+ ((index,  c))
          run(tail, threshold, L, S, newG)
        } else {
          val s = searchS(S, c, threshold)
          if(!s.key.isEmpty){
            val key = s.key & c.key
            val index = L.size
            val newL = L :+ ((key, index))
            val newG = G ++ List( (index, s), (index, c) )
            val newS = S.filterNot(_ == s)

            run(tail, threshold, newL, newS, newG)
          } else {
            val newS = S :+ c

            run(tail, threshold, L, newS, G)
          }
        }
      }
    }
  }

  @tailrec
  def searchL(L: List[(Set[Long], Int)], c: P): Int = {
    L match {
      case Nil => -1
      case l :: tail => {
        if( (l._1 & c.key) == l._1 ){
          l._2
        } else {
          searchL(tail, c)
        }
      }
    }
  }

  @tailrec
  def searchS(S: List[P], c: P, threshold: Int): P = {
    S match {
      case Nil => P(Set.empty[Long], List.empty[Point])
      case s :: tail => {
        if( (s.key & c.key).size >= threshold){
          s
        } else {
          searchS(tail, c, threshold)
        }
      }
    }
  }
}
