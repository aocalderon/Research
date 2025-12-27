package puj.bfe

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory, Point, LineString}
import org.locationtech.jts.index.strtree.STRtree

import scala.collection.JavaConverters._
import sys.process._

import archery._

import puj.Params
import puj.Utils._

object MF_Utils extends Logging {

  def encode(x: Int, y: Int): Long = {
    return if(x >= y){ x * x + x + y } else { y * y + x }
  }

  def decode(z: Long): (Int, Int) = {
    val b = math.floor(math.sqrt(z.toDouble))
    val a = z - b * b

    return if(a < b){ (a.toInt, b.toInt) } else { (b.toInt, (a - b).toInt) }
  }

  def insertMaximal(maximals: List[Disk], candidate: Disk): List[Disk] = {
    if(maximals.isEmpty){
      maximals :+ candidate
    } else {
      if( maximals.exists( maximal => candidate.isSubsetOf(maximal) ) ){
        maximals
      } else {
        if( maximals.exists( maximal => maximal.isSubsetOf(candidate) ) ){
          maximals.filterNot( maximal => maximal.isSubsetOf(candidate) ) :+ candidate
        } else {
          maximals :+ candidate
        }
      }
    }
  }

  def insertMaximal(maximals: archery.RTree[Disk], candidate: Disk)
    (implicit S: Settings): archery.RTree[Disk] = {

    if(maximals.entries.size == 0){
      val center = archery.Point(candidate.X, candidate.Y)
      val toInsert = Entry(center, candidate)
      maximals.insert(toInsert)
    } else {
      val maximals_prime = maximals.search(candidate.bbox(S.epsilon.toFloat)).map(_.value)

      if( maximals_prime.exists( maximal => candidate.isSubsetOf(maximal) ) ){
        maximals
      } else {
        if( maximals_prime.exists( maximal => maximal.isSubsetOf(candidate) ) ){
          val toRemove = maximals_prime.filter( maximal => maximal.isSubsetOf(candidate) )
            .map{ maximal =>
              val center = archery.Point(maximal.X, maximal.Y)
              Entry(center, maximal)
            }
          val center = archery.Point(candidate.X, candidate.Y)
          val toInsert = Entry(center, candidate)
          maximals.removeAll(toRemove).insert(toInsert)
        } else {
          val center = archery.Point(candidate.X, candidate.Y)
          val toInsert = Entry(center, candidate)
          maximals.insert(toInsert)
        }
      }
    }
  }

  def getPointsAroundCenter(center: Point, points: List[STPoint])
      (implicit S: Settings, G: GeometryFactory): Disk = {
    val pids = for{
      point <- points if { point.distanceToPoint(center) <= S.r }
    } yield {
      point
    }
    val c = G.createMultiPoint(pids.map(_.point).toArray).getCentroid
    Disk(c, pids.map(_.oid))
  }

  def computeCounts(points_prime: List[STPoint])(implicit S: Settings): List[STPoint] = {
    val tree = new STRtree(200)
    points_prime.foreach{ point => tree.insert(point.envelope, point) }

    val join = points_prime.flatMap{ p1 =>
      val envelope = p1.envelope
      envelope.expandBy(S.epsilon)
      val hood = tree.query(envelope).asScala.map{_.asInstanceOf[STPoint]}
      for{
        p2 <- hood if{ p1.distance(p2) <= S.epsilon }
      } yield {
        (p1, 1)
      }
    }
    val points = join.groupBy(_._1).map{ case(point, counts) =>
      point.count = counts.size
      point
    }.toList
    points
  }

  case class Grid(points: List[STPoint], envelope: Envelope = new Envelope()){
    private var minx: Double = _
    private var miny: Double = _
    private var maxx: Double = _
    private var maxy: Double = _
    var index: Map[Long, List[STPoint]] = Map.empty
    var expansion: Boolean = false

    def buildGrid(implicit S: Settings): Unit = {
      val epsilon = if(expansion) S.expansion else S.eprime
      minx = if(envelope.isNull()) points.minBy(_.X).X else envelope.getMinX
      miny = if(envelope.isNull()) points.minBy(_.Y).Y else envelope.getMinY
      val grid = points.filter(_.count >= S.mu).map{ point =>
        val i = math.floor( (point.X - minx) / epsilon ).toInt
        val j = math.floor( (point.Y - miny) / epsilon ).toInt
        (encode(i, j), point)
      }.groupBy(_._1)

      index = grid.mapValues(_.map(_._2))
    }

    def buildGrid1_5(minX: Double, minY: Double)(implicit S: Settings): Map[Long, List[STPoint]] = {
      val epsilon = (S.eprime * 1.5) + S.tolerance
      val grid = points.map{ point =>
        val i = math.floor( (point.X - minX) / epsilon ).toInt
        val j = math.floor( (point.Y - minY) / epsilon ).toInt
        (encode(i, j), point)
      }.groupBy(_._1)

      grid.mapValues(_.map(_._2))
    }

    def pointsToText: List[String] = {
      index.values.flatten.map{_.wkt + "\n"}.toList
    }

    def toText: List[String] = {
      index.flatMap{ case(key, points) =>
        val (i, j) = decode(key)
        points.map{ point =>
          val wkt = point.toText
          val oid = point.oid

          s"$wkt\t$key\t($i $j)\t$oid\n"
        }
      }.toList
    }

    def getRows(implicit S: Settings): Int = {
      if(!index.isEmpty){
        maxx = if(envelope.isNull) index.values.flatten.maxBy(_.X).X else envelope.getMaxX
        val epsilon = if(expansion) S.expansion else S.eprime
        math.ceil( (maxx - minx) / epsilon ).toInt
      } else {
        0
      }
    }

    def getColumns(implicit S: Settings): Int = {
      if(!index.isEmpty){
        maxy = if(envelope.isNull) index.values.flatten.maxBy(_.Y).Y else envelope.getMaxY
        val epsilon = if(expansion) S.expansion else S.eprime
        math.ceil( (maxy - miny) / epsilon ).toInt
      } else {
        0
      }
    }

    def getEnvelope(implicit S: Settings): Envelope = {
      if(!index.isEmpty){
        if(envelope.isNull){
          getRows
          getColumns
          new Envelope(minx, maxx, miny, maxy)
        } else {
          envelope
        }
      } else {
        new Envelope()
      }
    }

    def wkt(limit: Int = 2000)(implicit S: Settings, G: GeometryFactory): Seq[String] = {
      if(!index.isEmpty){
        val epsilon = if(expansion) S.expansion else S.eprime
        val (mbr, n, m) = if(envelope.isNull){
          buildGrid
          ( new Envelope(minx, maxx, miny, maxy), getRows, getColumns )
        } else{
          val n = math.ceil(envelope.getWidth  / epsilon).toInt
          val m = math.ceil(envelope.getHeight / epsilon).toInt
          ( envelope, n, m )
        }

        if(n * m < limit){
          val X_prime = (mbr.getMinX until mbr.getMaxX by epsilon).toList :+ mbr.getMaxX
          val X = if(X_prime.size == 1) mbr.getMinX +: X_prime else X_prime
          val Y_prime = (mbr.getMinY until mbr.getMaxY by epsilon).toList :+ mbr.getMaxY
          val Y = if(Y_prime.size == 1) mbr.getMinY +: Y_prime else Y_prime

          debug{
            println(s"X size: ${X.size}")
            println(s"Y size: ${Y.size}")
          }

          for{
            i <- 0 until X.size - 1
            j <- 0 until Y.size - 1
          } yield {
            val grid_cell = new Envelope( X(i), X(i + 1), Y(j), Y(j + 1) )
            val gridId = encode(i, j).toInt
            val polygon = G.toGeometry(grid_cell)
            val wkt = polygon.toText

            s"$wkt\t($i $j)\t$gridId\n"
          }
        } else {
          Seq.empty
        }
      } else {
        Seq.empty
      }
    }
  }
}

