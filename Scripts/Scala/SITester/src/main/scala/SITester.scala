package edu.ucr.dblab.sitester

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.strtree._
import org.locationtech.jts.index.hprtree._
import org.locationtech.jts.index.quadtree._
import org.locationtech.jts.index.kdtree._

import edu.ucr.dblab.pflock.sedona.{StandardQuadTree, QuadRectangle}
import org.apache.sedona.core.spatialPartitioning.KDB

import edu.ucr.dblab.sitester.spmf.{KDNode, KDTree => SPMFKDTree}
import edu.ucr.dblab.sitester.spmf.DoubleArray

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source

object SITester {
  case class SedonaEnvelope(envelope: Envelope, point: Point) extends Envelope(envelope)

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

      val minX = sample.map(_.getX).min
      val minY = sample.map(_.getY).min
      val maxX = sample.map(_.getX).max
      val maxY = sample.map(_.getY).max
      val envelope = new Envelope(minX, maxX, minY, maxY)
      val boundary = new QuadRectangle(envelope)
      val (_, iSedonaQuadtreeTime) = timer{
        val sedona = new StandardQuadTree[Point](boundary, 0, 200, 16)
        sample.foreach{ point =>
          sedona.insert(new QuadRectangle(point.getEnvelopeInternal), point)
        }
      }

      val (_, iSedonaKdtreeTime) = timer{
        val sedona = new KDB(200, 16, envelope)
        sample.foreach{ point =>
          val envelope = SedonaEnvelope(point.getEnvelopeInternal, point)
          sedona.insert(envelope)
        }
      }

      val (_, iSpmfKdtreeTime) = timer{
        val spmf_kdtree = new SPMFKDTree()
        val vectors = sample.map{ point =>
          new DoubleArray(point)
        }.toList.asJava
        spmf_kdtree.buildtree(vectors)
      }

      logt(s"Insertion|$n|JTS_STRtree    |$iRtreeTime")
      logt(s"Insertion|$n|JTS_HPRtree    |$iHprtreeTime")
      logt(s"Insertion|$n|JTS_Quadtree   |$iQuadtreeTime")
      logt(s"Insertion|$n|JTS_KDtree     |$iKdtreeTime")
      logt(s"Insertion|$n|Sedona_Quadtree|$iSedonaQuadtreeTime")
      logt(s"Insertion|$n|Sedona_KDtree  |$iSedonaKdtreeTime")
      logt(s"Insertion|$n|Spmf_KDtree    |$iSpmfKdtreeTime")
    }

    val rtree = new STRtree()
    points.foreach{ point =>
      rtree.insert(point.getEnvelopeInternal, point)
    }
    val hprtree = new HPRtree()
    points.foreach{ point =>
      hprtree.insert(point.getEnvelopeInternal, point)
    }
    val quadtree = new Quadtree()
    points.foreach{ point =>
      quadtree.insert(point.getEnvelopeInternal, point)
    }
    val kdtree = new KdTree()
    points.foreach{ point =>
      kdtree.insert(point.getCoordinate, point)
    }

    val minX = points.map(_.getX).min
    val minY = points.map(_.getY).min
    val maxX = points.map(_.getX).max
    val maxY = points.map(_.getY).max
    val envelope = new Envelope(minX, maxX, minY, maxY)
    val boundary = new QuadRectangle(envelope)
    val sedona_quadtree = new StandardQuadTree[Point](boundary, 0, 200, 16)
    points.foreach{ point =>
      sedona_quadtree.insert(new QuadRectangle(point.getEnvelopeInternal), point)
    }

    val sedona_kdtree = new KDB(200, 16, envelope)
    points.foreach{ point =>
      val envelope = SedonaEnvelope(point.getEnvelopeInternal, point)
      sedona_kdtree.insert(envelope)
    }

    val spmf_kdtree = new SPMFKDTree()
    val vectors = points.map{ point =>
      new DoubleArray(point)
    }.asJava
    spmf_kdtree.buildtree(vectors)

    for{ e <- (1 to 10) }{
      val n = e * epsilon
      val (q1, qJTSRtreeTime) = timer{
        points.map{ p1 =>
          val envelope = new Envelope(p1.getX - n, p1.getX + n, p1.getY - n, p1.getY + n)
          val hood = rtree.query(envelope).asScala.map(_.asInstanceOf[Point])
          val pairs = getPairs(p1, hood, n)
          (p1, pairs)
        }
      }
      /*
      val (q2, qHprtreeTime) = timer{
        points.map{ p1 =>
          val envelope = new Envelope(p1.getX - n, p1.getX + n, p1.getY - n, p1.getY + n)
          val hood = hprtree.query(envelope).asScala.map(_.asInstanceOf[Point])
          val count = getCounts(p1, hood, n)
          (p1, count)
        }
      }*/
      val (q3, qJTSQuadtreeTime) = timer{
        points.map{ p1 =>
          val envelope = new Envelope(p1.getX - n, p1.getX + n, p1.getY - n, p1.getY + n)
          val hood = quadtree.query(envelope).asScala.map(_.asInstanceOf[Point])
          val pairs = getPairs(p1, hood, n)
          (p1, pairs)
        }
      }
      val (q4, qJTSKdtreeTime) = timer{
        points.map{ p1 =>
          val envelope = new Envelope(p1.getX - n, p1.getX + n, p1.getY - n, p1.getY + n)
          val hood = kdtree.query(envelope).asScala.map{ c =>
            geofactory.createPoint(c.asInstanceOf[KdNode].getCoordinate)
          }
          val pairs = getPairs(p1, hood, n)
          (p1, pairs)
        }
      }
      val (q5, qSedonaQuadtreeTime) = timer{
        points.map{ p1 =>
          val envelope = new Envelope(p1.getX - n, p1.getX + n, p1.getY - n, p1.getY + n)
          val rectangle = new QuadRectangle(envelope)
          val hood = sedona_quadtree.getElements(rectangle).asScala.toList
          val pairs = getPairs(p1, hood, n)
          (p1, pairs)
        }
      }
      val (q6, qSedonaKdtreeTime) = timer{
        points.map{ p1 =>
          val envelope = new Envelope(p1.getX - n, p1.getX + n, p1.getY - n, p1.getY + n)
          val hood = sedona_kdtree.findLeafNodes(envelope).asScala.toList.flatMap{ node =>
            node.getItems.asScala.map{_.asInstanceOf[SedonaEnvelope].point}
          }
          val pairs = getPairs(p1, hood, n)
          (p1, pairs)
        }
      }
      val (q7, qSpmfKdtreeTime) = timer{
        points.map{ p1 =>
          val querypoint = new DoubleArray(p1)
          val hood = spmf_kdtree.pointsWithinRadiusOf(querypoint, n).asScala.map{_.point}
          val pairs = getPairs(p1, hood, n)
          (p1, pairs)
        }
      }

      logt(s"Query|$n|JTS_Rtree      |$qJTSRtreeTime")
      //logt(s"Query|$n|HPRtree |$qHprtreeTime")
      logt(s"Query|$n|JTS_Quadtree   |$qJTSQuadtreeTime")
      logt(s"Query|$n|JTS_KDtree     |$qJTSKdtreeTime")
      logt(s"Query|$n|Sedona_Quadtree|$qSedonaQuadtreeTime")
      logt(s"Query|$n|Sedona_Kdtree  |$qSedonaKdtreeTime")
      logt(s"Query|$n|Spmf_Kdtree    |$qSpmfKdtreeTime")

      if(params.debug()){
        val pairs = s"${getNPairs(q1)} ${getNPairs(q3)} ${getNPairs(q4)} ${getNPairs(q5)}"
        log(s"Range query ${n}m [${pairs}]")
        val a = q1.map{ case(p, pts) => (p, pts.sortBy(_.getCoordinate).mkString(" ")) }
          .sortBy(_._1.getCoordinate).mkString("\n")
        val b = q3.map{ case(p, pts) => (p, pts.sortBy(_.getCoordinate).mkString(" ")) }
          .sortBy(_._1.getCoordinate).mkString("\n")
        val c = q4.map{ case(p, pts) => (p, pts.sortBy(_.getCoordinate).mkString(" ")) }
          .sortBy(_._1.getCoordinate).mkString("\n")
        val d = q5.map{ case(p, pts) => (p, pts.sortBy(_.getCoordinate).mkString(" ")) }
          .sortBy(_._1.getCoordinate).mkString("\n")
        val e = q6.map{ case(p, pts) => (p, pts.sortBy(_.getCoordinate).mkString(" ")) }
          .sortBy(_._1.getCoordinate).mkString("\n")
        val f = q6.map{ case(p, pts) => (p, pts.sortBy(_.getCoordinate).mkString(" ")) }
          .sortBy(_._1.getCoordinate).mkString("\n")
        log(s"Comparing... ${a == b}")
        log(s"Comparing... ${a == c}")
        log(s"Comparing... ${a == d}")
        log(s"Comparing... ${a == e}")
        log(s"Comparing... ${a == f}")
      }
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

  def getNPairs(points: List[(Point, List[Point])]): Int = points.map(_._2.size).sum

  def getPairs(p1: Point, hood: Iterable[Point], epsilon: Double): List[Point] = {
    val pairs = for{
      p2 <- hood
      if{
        p1.getCoordinate.compareTo(p2.getCoordinate) < 0 &&
        p1.distance(p2) <= epsilon
      }
    } yield {
      p2
    }
    pairs.toList
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
  val input:     ScallopOption[String]  = opt[String]  (default = Some("/home/and/Research/Datasets/LA_50K_T320.tsv"))
  val epsilon:   ScallopOption[Double]  = opt[Double]  (default = Some(10.0))
  val size:      ScallopOption[Int]     = opt[Int]     (default = Some(5000))
  val capacity:  ScallopOption[Int]     = opt[Int]     (default = Some(100))
  val tolerance: ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))
  val debug:     ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
