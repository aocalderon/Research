package edu.ucr.dblab

import com.vividsolutions.jts.algorithm.MinimumBoundingCircle
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Coordinate, Point}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.index.quadtree.Quadtree
import scala.collection.JavaConverters._
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.alg.clique.BronKerboschCliqueFinder
import java.io.PrintWriter
import scala.io.Source
import scala.annotation.tailrec
import edu.ucr.dblab.djoin.SPMF._

object CliqueFinderUtils {
  case class Clique(id: Int, points: List[Point])
  case class Disk(x: Double, y: Double, pids: List[Int], clique_id: Int = -1)
  case class MBC(circle: MinimumBoundingCircle, inner: List[Int], outer: List[Int])

  def findCenters(points: List[Point], epsilon: Double, r2: Double)
      (implicit geofactory: GeometryFactory): List[Point] = {
    val centers = for {
      a <- points
      b <- points if {
        val id1 = a.getUserData.asInstanceOf[Int]
        val id2 = b.getUserData.asInstanceOf[Int]

        id1 < id2 & a.distance(b) <= epsilon
      }
    } yield {
      calculateCenterCoordinates(a, b, r2)
    }

    centers.flatten    
  }

  def clocktime: Long = System.nanoTime()

  def timer[R](msg: String)(block: => R): R = {
    val t0 = clocktime
    val result = block    // call-by-name
    val t1 = clocktime
    println("%-30s|%6.2f".format(msg, (t1 - t0) / 1e9))
    result
  }

  def debug[R](block: => R)(implicit d: Boolean): Unit = { if(d) block }

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1e9)
    println(s"Saved ${filename} in ${time}s [${content.size} records].")
  }

  def calculateCenterCoordinates(p1: Point, p2: Point, r2: Double,
    delta: Double = 0.001)(implicit geofactory: GeometryFactory): List[Point] = {

    val X: Double = p1.getX - p2.getX
    val Y: Double = p1.getY - p2.getY
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + p2.getX
      val k1: Double = ((Y - X * root) / 2) + p2.getY
      val h2: Double = ((X - Y * root) / 2) + p2.getX
      val k2: Double = ((Y + X * root) / 2) + p2.getY
      val h = geofactory.createPoint(new Coordinate(h1,k1))
      val k = geofactory.createPoint(new Coordinate(h2,k2))
      List(h, k)
    } else {
      val p2_prime = geofactory.createPoint(new Coordinate(p2.getX + delta, p2.getY))
      calculateCenterCoordinates(p1, p2_prime, r2)
    }
  }

  def pruneDisks(disks: List[Disk], mu: Int): List[Disk] = {
    val transactions = disks.map{ disk =>
      ((disk.x, disk.y), disk.pids.mkString(" "))
    }.groupBy(_._2).map{ disk =>
      val pids = disk._1
      val (x, y) = disk._2.head._1

      new Transaction(x, y, pids)
    }.toList

    val data = new Transactions(transactions.asJava, 0)
    val lcm = new AlgoLCM2()
    lcm.run(data)

    lcm.getPointsAndPids.asScala
      .filter(_.getItems.size >= mu)
      .map{ m =>
        val pids = m.getItems.toList.map(_.toInt).sorted
        val x = m.getX
        val y = m.getY
        Disk(x, y, pids)
      }.toList
  }

  def farthestPoints(pts: Array[Coordinate]): Array[Coordinate] = {
    val dist01 = pts(0).distance(pts(1))
    val dist12 = pts(1).distance(pts(2))
    val dist20 = pts(2).distance(pts(0))

    if (dist01 >= dist12 && dist01 >= dist20){
      Array(pts(0), pts(1))
    } else if (dist12 >= dist01 && dist12 >= dist20){
      Array(pts(1), pts(2))
    } else {
      Array(pts(2), pts(0))
    }
  }

  def closestPoints(pts: Array[Coordinate]): Array[Coordinate] = {
    val dist01 = pts(0).distance(pts(1))
    val dist12 = pts(1).distance(pts(2))
    val dist20 = pts(2).distance(pts(0))

    if (dist01 <= dist12 && dist01 <= dist20){
      Array(pts(0), pts(1))
    } else if (dist12 <= dist01 && dist12 <= dist20){
      Array(pts(1), pts(2))
    } else {
      Array(pts(2), pts(0))
    }
  }
}
