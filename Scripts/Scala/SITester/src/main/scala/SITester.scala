package edu.ucr.dblab.sitester

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.strtree._
import org.locationtech.jts.index.hprtree._
import org.locationtech.jts.index.quadtree._
import org.locationtech.jts.index.kdtree._

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source

object SITester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new Params(args)
    implicit val geofactory = new GeometryFactory(new PrecisionModel(
      1.0 / params.tolerance()
    ))

    val points = readPoints(params.input())
    val epsilon = params.epsilon()
    val size = params.size()
    
    for{ i <- (1 to 10) }{
      val n = i * size
      val sample = points.take(n)
      log(s"Inserting ${n} points")
      val (_, iRtreeTime) = timer{
        val rtree = new STRtree()
        sample.foreach{ point =>
          rtree.insert(point.getEnvelopeInternal, point)
        }
      }

      val (_, iHprtreeTime) = timer{
        val hprtree = new HPRtree()
        sample.foreach{ point =>
          hprtree.insert(point.getEnvelopeInternal, point)
        }
      }
      
      val (_, iQuadtreeTime) = timer{
        val quadtree = new Quadtree()
        sample.foreach{ point =>
          quadtree.insert(point.getEnvelopeInternal, point)
        }
      }

      val (_, iKdtreeTime) = timer{
        val kdtree = new KdTree()
        sample.foreach{ point =>
          kdtree.insert(point.getCoordinate, point)
        }
      }

      logt(s"Insertion|$n|STRtree |$iRtreeTime")
      logt(s"Insertion|$n|HPRtree |$iHprtreeTime")
      logt(s"Insertion|$n|Quadtree|$iQuadtreeTime")
      logt(s"Insertion|$n|KDtree  |$iKdtreeTime")
    }

    val point = new Coordinate(1982027.884,561914.125)
    val sample = points.take(10 * size)
    val rtree = new STRtree()
    sample.foreach{ point =>
      rtree.insert(point.getEnvelopeInternal, point)
    }
    val hprtree = new HPRtree()
    sample.foreach{ point =>
      hprtree.insert(point.getEnvelopeInternal, point)
    }
    val quadtree = new Quadtree()
    sample.foreach{ point =>
      quadtree.insert(point.getEnvelopeInternal, point)
    }
    val kdtree = new KdTree()
    sample.foreach{ point =>
      kdtree.insert(point.getCoordinate, point)
    }

    for{ e <- (1 to 10) }{
      val n = e * epsilon
      val envelope = new Envelope(point.x - n, point.x + n, point.y - n, point.y + n)
      val (q1, qRtreeTime) = timer{ rtree.query(envelope).size }
      val (q2, qHprtreeTime) = timer{ hprtree.query(envelope).size }
      val (q3, qQuadtreeTime) = timer{ quadtree.query(envelope).size }
      val (q4, qKdtreeTime) = timer{ kdtree.query(envelope).size }
      log(s"Range query ${n}m [$q1 $q2 $q3 $q4]")
      logt(s"Query|$n|STRtree |$qRtreeTime")
      logt(s"Query|$n|HPRtree |$qHprtreeTime")
      logt(s"Query|$n|Quadtree|$qQuadtreeTime")
      logt(s"Query|$n|KDtree  |$qKdtreeTime")
    }
  }

  def readPoints(input: String)
    (implicit geofactory: GeometryFactory): List[Point] = {

    val buffer = Source.fromFile(input)
    val points = buffer.getLines.toList
      .map{ line =>
        val arr = line.split("\t")
        val i = arr(0).toInt
        val x = arr(1).toDouble
        val y = arr(2).toDouble
        val t = arr(3).toInt
        val point = geofactory.createPoint(new Coordinate(x, y))
        point
      }
    buffer.close
    points
  }

  def clocktime: Long = System.nanoTime()

  def log(msg: String)(implicit logger: Logger): Unit = {
    logger.info(s"INFO|$msg")
  }

  def logt(msg: String)(implicit logger: Logger): Unit = {
    logger.info(s"TIME|$msg")
  }

  def timer[R](block: => R): (R, Double) = {
    val t0 = clocktime
    val result = block    // call-by-name
    val t1 = clocktime
    val time = (t1 - t0) / 1e9
    (result, time)
  }
}

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val input:     ScallopOption[String]  = opt[String]  (default = Some(""))
  val epsilon:   ScallopOption[Double]  = opt[Double]  (default = Some(10.0))
  val size    :  ScallopOption[Int]     = opt[Int]     (default = Some(5000))
  val capacity:  ScallopOption[Int]     = opt[Int]     (default = Some(100))
  val tolerance: ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))

  verify()
}
