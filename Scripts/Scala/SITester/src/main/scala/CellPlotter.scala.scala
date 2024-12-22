package edu.ucr.dblab.sitester

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}

import edu.ucr.dblab.pflock.sedona.{StandardQuadTree, QuadRectangle}
import org.apache.sedona.core.spatialPartitioning.KDB

import scala.collection.JavaConverters._
import scala.io.Source

object CellPlotter {
  case class SedonaEnvelope(envelope: Envelope, point: Point) extends Envelope(envelope)

  def main(args: Array[String]): Unit = {
    implicit val params = new CP_Params(args)
    implicit val geofactory = new GeometryFactory( new PrecisionModel(1.0 / params.tolerance()) )

    val points = readPoints(params.input())
    val epsilon = params.epsilon()
    val size = params.size()

    val n = 10000
    val sample = points.take(n)
    println(s"Inserting ${n} points")

    val minX = sample.map(_.getX).min
    val minY = sample.map(_.getY).min
    val maxX = sample.map(_.getX).max
    val maxY = sample.map(_.getY).max
    val envelope = new Envelope(minX, maxX, minY, maxY)
    val boundary = new QuadRectangle(envelope)
    val (quadtree, iSedonaQuadtreeTime) = timer{
        val sedona = new StandardQuadTree[Point](boundary, 0, 200, 16)
        sample.foreach{ point =>
            sedona.insert(new QuadRectangle(point.getEnvelopeInternal), point)
        }
        sedona
    }

    val (kdtree, iSedonaKdtreeTime) = timer{
        val sedona = new KDB(200, 16, envelope)
        sample.foreach{ point =>
            val envelope = SedonaEnvelope(point.getEnvelopeInternal, point)
            sedona.insert(envelope)
        }
        sedona
    }

    println(s"Insertion|$n|Sedona_Quadtree|$iSedonaQuadtreeTime")
    println(s"Insertion|$n|Sedona_KDtree  |$iSedonaKdtreeTime")

    save("/tmp/edgesQ.wkt"){
      quadtree.getLeafZones.asScala.map{ zone => 
        val wkt = geofactory.toGeometry(zone.getEnvelope).toText 
        s"$wkt\n"
      }
    }

    save("/tmp/edgesK.wkt"){
      kdtree.fetchLeafZones.asScala.map{ zone => 
        val wkt = geofactory.toGeometry(zone).toText 
        s"$wkt\n"
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

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1e9)
    println(s"Saved ${filename}\tin\t${time}s\t[${content.size} records].")
  }

  def clocktime: Long = System.nanoTime()

  def timer[R](block: => R): (R, Double) = {
    val t0 = clocktime
    val result = block    // call-by-name
    val t1 = clocktime
    val time = (t1 - t0) / 1e9
    (result, time)
  }
}

import org.rogach.scallop._

class CP_Params(args: Seq[String]) extends ScallopConf(args) {
  val input:     ScallopOption[String]  = opt[String]  (default = Some("/home/and/Research/Datasets/LA_50K_T320.tsv"))
  val epsilon:   ScallopOption[Double]  = opt[Double]  (default = Some(10.0))
  val size:      ScallopOption[Int]     = opt[Int]     (default = Some(5000))
  val capacity:  ScallopOption[Int]     = opt[Int]     (default = Some(100))
  val tolerance: ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))
  val debug:     ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}

